"""Definition of :class:`DataBlock` and related classes."""

import re
import logging

from . import utils
from .utils import BaseClass
from . import syntax
from . import section_names
from .lib import block
from .mpi import CurrentMPIComm


class BlockMapping(block.BlockMapping,BaseClass):
    """
    This class handles a mapping between different (section, name) entries in :class:`DataBlock`.
    It is useful if one wants to locally (i.e. for a specific module) change the entry corresponding
    to an item saved in the :class:`DataBlock` instance.

    Attributes
    ----------
    data : dict
        Single level dictionary containing (section, name) mapping.

    Note
    ----
    Only (section, name) :class:`DataBlock` getters and setters will be impacted by the mapping.
    For example, :meth:`DataBlock.items` will list (section, name), values whatever the mapping is.
    """
    def __init__(self, data=None, sep=None):
        """
        Initialise :class:`BlockMapping`.

        Parameters
        ----------
        data : dict, default=None
            Single level dictionary, where the :class:`DataBlock` ``section2``, ``name2`` internal entry
            accessed when calling :class:`DataBlock` getters and setters with ``section1``, ``name1`` is::

                data[section1,name1]

            If ``sep`` is not ``None``, the corresponding ``data`` key may be ``section1[sep]name1`` and the corresponding
            value ``section2[sep]name2`` (see below).
            One can also specify the mapping for a whole section ``section1`` to an internal section ``section2`` with::

                data[section1]

        sep : string, default=None
            If not ``None``, ``data`` string keys and values will be split using the separator ``sep``.
            For example, if ``sep == '.'``, the key ``section1.name1`` will be split into ``section1``, ``name1``.
        """
        if data is None or isinstance(data,block.BlockMapping):
            super(BlockMapping,self).__init__(data=data)
            return

        data_ = {}
        for key,value in data.items():
            if sep is not None:
                key,value = (syntax.split_sections(section_name,sep=sep) for section_name in [key,value])
            data_[key] = value
        super(BlockMapping,self).__init__(data=data_)

    def __getstate__(self):
        """Return this class state dictionary."""
        return {'data':self.data}

    def __setstate__(self, state):
        """Set the class state dictionary."""
        return self.__init__(data=state['data'])

    def __iter__(self):
        """Iter. TODO: implement in C."""
        return iter(self.keys())

    def setdefault(self, key, value):
        """Set default value. TODO: implement in C."""
        if key not in self:
            self[key] = value

    def __copy__(self):
        """Return a shallow copy of ``self``, i.e. only the dictionary mapping to the stored items is copied, not the items themselves."""
        return self.__class__(super(BlockMapping,self).copy())

    def to_dict(self, sep=None):
        """Return as dictionary of tuples, joined by ``sep`` if not ``None``."""
        if sep is not None:
            toret = {}
            for key,value in self.data.items():
                toret[syntax.join_sections(key,sep=sep)] = syntax.join_sections(value,sep=sep)
        else:
            toret = self.data.copy()
        return toret


class DataBlock(block.DataBlock,BaseClass):
    """
    The data structure fed to all modules.

    It is essentially a dictionary, with items to be accessed through the key (section, name).
    Most useful methods are those to get (get, get_type, get_[type]...) and set objects.
    The class mostly inherits from the DataBlock type coded using the Python C API.
    Only a few convenience methods are written in Python below.

    >>> data_block = DataBlock({'section1':{'name1':1}})
    >>> data_block.get('section1','name1')
    >>> data_block.get_int('section1','name1')
    >>> data_block.get_string('section1','name1')
    Traceback (most recent call last): pypescript.block.TypeError: Wrong type for "name1" in section [section1].

    Attributes
    ----------
    data : dict
        Double level (section, name) dictionary.

    mapping : BlockMapping
        See documentation of :class:`BlockMapping`.

    """
    logger = logging.getLogger('DataBlock')

    def __init__(self, data=None, mapping=None, add_sections=None):
        """
        Initialise :class:`DataBlock`.

        Parameters
        ----------
        data : dict, default=None
            Double level dictionary, where the item corresponding to ``section1``, ``name1`` can be accessed through::

                data[section1][name1]

        mapping : BlockMapping, dict, default=None
            See documentation of :class:`BlockMapping`.

        add_sections: list, default=None
            List of sections to be added to ``self``.
            If ``None``, defaults to :attr:`syntax.common_sections`.
        """
        if isinstance(data,str) and data.endswith(syntax.block_save_extension):
            new = self.load(data)
            super(DataBlock,self).__init__(data=new.data,mapping=new.mapping)
            return

        if not isinstance(mapping,BlockMapping):
            mapping = BlockMapping(mapping)

        super(DataBlock,self).__init__(data=data,mapping=mapping)
        if add_sections is None:
            add_sections = syntax.common_sections
        for section in add_sections:
            if section not in self: self[section] = {}
        self.setdefault(section_names.mpi,'comm',CurrentMPIComm.get())

    def get_type(self, section, name, types, *args, **kwargs):
        """
        Wrapper around :meth:`DataBlock.get` which further checks the output type
        and returns a ``TypeError`` if the result is not an instance of any of ``types``.

        Parameters
        ----------
        section : string
            Section name.

        name : string
            Element name. ``section``, ``name`` is the complete ``DataBlock`` entry.

        types : list, tuple, string, type or class
            Types to check the return value of :meth:`DataBlock.get` against.
            If list or tuple, check whether any of the proposed types matches.
            If a type is string, will search for the corresponding builtin type.

        args : list
            Other arguments to :meth:`DataBlock.get`.

        kwargs : dict
            Other arguments to :meth:`DataBlock.get`.

        Returns
        -------
        value : object
            Return value in ``self`` if it exists and is of the correct type (else raises a ``TypeError``),
            else default value if provided (else raises a ``KeyError``).

        Raises
        ------
        TypeError if the result of :meth:`DataBlock.get` is not the default value (if provided) and not a ``type_`` instance.

        Note
        ----
        If a default value is provided, and returned (in case the required ``section``, ``name`` is not in :class:`DataBlock`)
        then no type-checking is performed (hence no exception is raised).
        """
        if not self.has(section,name):
            return self.get(section,name,*args,**kwargs)

        value = self.get(section,name)

        if not utils.is_of_type(value, types):
            raise TypeError('Wrong type for "{}" in section [{}] (accepted: {}).'.format(name,section,types))

        return value

    def set_mapping(self, mapping=None):
        """
        Set mapping.

        Parameters
        ----------
        mapping :  BlockMapping, dict, default=None
            See documentation of :class:`BlockMapping`.
        """
        return super(DataBlock,self).set_mapping(mapping if isinstance(mapping,BlockMapping) else BlockMapping(mapping))

    def copy(self, nocopy=None):
        """
        Return a shallow copy of ``self``, i.e. only the dictionary mapping to the stored items is copied.
        The items themselves are not copied, except if they have an attribute ``_copy_if_datablock_copy`` set to ``True``.

        Parameters
        ----------
        nocopy : list, default=None
            List of sections to **not** copy (such that any change affecting in these sections of ``self`` will affect the
            returned copy as well).
            If ``None``, defaults to :attr:`syntax.common_sections`.

        Note
        ----
        :attr:`mapping` instance is simply added to the returned :class:`DataBlock` instance, no copy is performed.
        """
        if nocopy is None:
            nocopy = [section for section in syntax.common_sections if section in self]
        new = self.__class__(mapping=self.mapping,add_sections=[])
        for section in self.sections():
            if section in nocopy:
                new[section] = self[section]
                continue
            new_section = {}
            for name,value in self[section].items():
                if getattr(value,syntax.copy_if_datablock_copy,False):
                    value = value.copy()
                new_section[name] = value
            new[section] = new_section
        return new

    def update(self, other, nocopy=None):
        """
        Update ``self``, i.e. only the dictionary mapping to the stored items is updated with ``other``, not the items themselves.

        Parameters
        ----------
        nocopy : list, default=None
            List of sections to **not** update (such that any change affecting in these sections of ``other`` will affect ``self`` as well.
            If ``None``, defaults to :attr:`syntax.common_sections`.

        Note
        ----
        :attr:`mapping` instance is **not** updated.
        """
        if nocopy is None:
            nocopy = [section for section in syntax.common_sections if section in self]
        return super(DataBlock,self).update(other,nocopy=nocopy)

    def __getstate__(self):
        """Return this class state dictionary."""
        data = {}
        for section in self.sections():
            data[section] = {}
            for name, value in self[section].items():
                if (section,name) == (section_names.mpi,'comm'):
                    continue
                if hasattr(value,'__getstate__'):
                    data[section][name] = {'__class__':value.__class__,'__dict__':value.__getstate__()}
                else:
                    data[section][name] = value
        return {'data':data,'mapping':self.mapping.__getstate__()}

    def __setstate__(self, state):
        """Set the class state dictionary."""
        data = {}
        for section in state['data']:
            data[section] = {}
            for name,value in state['data'][section].items():
                if isinstance(value,dict) and set(value.keys()) == {'__class__','__dict__'}:
                    data[section][name] = value['__class__'].from_state(value['__dict__'])
                else:
                    data[section][name] = value
        super(DataBlock,self).__init__(data=data,mapping=BlockMapping.from_state(state['mapping']))

    def __iter__(self, **kwargs):
        """Iter. TODO: implement in C."""
        return iter(self.keys(**kwargs))

    def setdefault(self, section, name, value):
        """Set default value. TODO: implement in C."""
        if (section,name) not in self:
            self.set(section,name,value)

    def mpi_distribute(self, dests, mpicomm=None):
        for key,value in self.items():
            if hasattr(value,'mpi_distribute'):
                self[key] = value.mpi_distribute(dests=dests,mpicomm=mpicomm)
        self['mpi','comm'] = mpicomm
        return self


def _make_getter(type_):

    def getter(self, section, name, *args, **kwargs):
        """:meth:`DataBlock.get_type` for type {}.""".format(type_)
        return self.get_type(section,name,type_,*args,**kwargs)

    return getter


for type_ in ['bool','int','float','string','list','dict','bool_array','int_array','float_array','string_array']:
    setattr(DataBlock,'get_{}'.format(type_),_make_getter(type_))


class SectionBlock(object):
    """
    Convenient wrapper to restrict a :class:`DataBlock` instance to a given section, such that items can be accessed
    as in a single-level dictionary.

    >>> section_block = SectionBlock(data_block,'section1')
    >>> section_block.get('name1')
    """

    def __init__(self, block, section):
        """
        Initialise :class:`SectionBlock`.

        Parameters
        ----------
        block : DataBlock
            :class:`DataBlock` instance.

        section : string
            Section to restrict :attr:``block`` to.
        """
        self.block = block
        self.section = section

    def __str__(self):
        """Class string as a dictionary."""
        return str(self.block[self.section])

    def items(self):
        """Yield (name, value) tuples."""
        yield from self.block[self.section].items()

    def has(self, name):
        """Has this ``name``?"""
        return self.block.has(self.section,name)

    def __getitem__(self, key):
        """Get item."""
        return self.block[self.section,key]

    def __setitem__(self, key, value):
        """Set item."""
        self.block[self.section,key] = value

    def __contains__(self, name):
        """Contains this ``name``?"""
        return self.has(name)

    def keys(self):
        """Return names in the :attr:``block`` section."""
        return (name for section,name in self.block.keys(section=self.section))

    def setdefault(self, name, value):
        """Set default value. TODO: implement in C."""
        self.block.setdefault(self.section,name,value)


def _make_getter(name):

    def getter(self, key, *args, **kwargs):
        """:meth:`DataBlock.{}` for the :attr:``block`` section.""".format(name)
        return getattr(self.block,name)(self.section,key,*args,**kwargs)

    return getter


for name in dir(DataBlock):

    if name.startswith('get'):
        setattr(SectionBlock,name,_make_getter(name))
