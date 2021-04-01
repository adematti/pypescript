"""Definition of :class:`DataBlock` and related classes."""

import re
import logging

import numpy as np

from . import utils
from .utils import BaseClass
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
        if data is None or isinstance(data, block.BlockMapping):
            super(BlockMapping,self).__init__(data=data)
            return
        data_ = {}
        for key,value in data.items():
            if not ((isinstance(key,str) and isinstance(value,str)) or (isinstance(key,tuple) and isinstance(value,tuple))):
                raise TypeError('In mapping {} = {}, both terms should be either string or tuple'.format(key,value))
            if sep is not None:
                key,value = (utils.split_section_name(section_name) for section_name in [key,value])
            if isinstance(key,tuple):
                if not (len(key) == len(value) <= 2):
                    raise TypeError('In mapping {} = {}, both terms should be same size (1 or 2)'.format(key,value))
                if len(key) == 1:
                    key,value = key[0],value[0]
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
        """Iter. TODO: implement in C."""
        if key not in self:
            self[key] = value


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
            If ``None``, defaults to :attr:`section_names.nocopy`.
        """
        super(DataBlock,self).__init__(data=data, mapping=mapping if isinstance(mapping,BlockMapping) else BlockMapping(mapping))
        if add_sections is None:
            add_sections = section_names.nocopy
        for section in add_sections:
            if section not in self: self[section] = {}
        self.setdefault(section_names.mpi,'comm',CurrentMPIComm.get())

    def get_type(self, section, name, type_, *args, **kwargs):
        """
        Wrapper around :meth:`DataBlock.get` which further checks the output type
        and returns a ``TypeError`` if the result is not a ``type_`` instance.

        Parameters
        ----------
        section : string
            Section name.

        name : string
            Element name. ``section``, ``name`` is the complete ``DataBlock`` entry.

        type_ : string, type or class
            Type to check the return value of :meth:`DataBlock.get` against.
            If string, will search for the corresponding builtin type.

        args : list
            Other arguments to :meth:`DataBlock.get`.

        kwargs : dict
            Other arguments to :meth:`DataBlock.get`.

        Returns
        -------
        value : object
            ``type_`` instance or default value if provided.

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
        convert = {'string':'str'}

        def get_type_from_str(type_):
            return __builtins__.get(type_,None)

        def get_nptype_from_str(type_):
            return {'bool':np.bool_,'int':np.integer,'float':np.floating,'str':np.string_}.get(type_,None)

        error = TypeError('Wrong type for "{}" in section [{}].'.format(name,section))
        if isinstance(type_,str):
            type_ = convert.get(type_,type_)
            type_py = get_type_from_str(type_)
            if type_py is not None:
                if not isinstance(value,type_py):
                    raise error
            else:
                match = re.match('(.*)_array',type_)
                if match is None:
                    raise error
                type_ = convert.get(match.group(1),match.group(1))
                if isinstance(value,np.ndarray):
                    type_np = get_nptype_from_str(type_)
                    if type_np is None or not np.issubdtype(value.dtype,type_np):
                        raise error
                else:
                    raise error
        elif not isinstance(value,type_):
            raise error
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
        Return a shallow copy of ``self``, i.e. only the dictionary mapping to the stored items is copied, not the items themselves.

        Parameters
        ----------
        nocopy : list, default=None
            List of sections to **not** copy (such that any change affecting in these sections of ``self`` will affect the
            returned copy as well).
            If ``None``, defaults to :attr:`section_names.nocopy`.

        Note
        ----
        :attr:`mapping` instance is simply added to the returned :class:`DataBlock` instance, no copy is performed.
        """
        if nocopy is None:
            nocopy = [section for section in section_names.nocopy if section in self]
        return super(DataBlock,self).copy(nocopy=nocopy)

    def update(self, other, nocopy=None):
        """
        Update ``self``, i.e. only the dictionary mapping to the stored items is updated with ``other``, not the items themselves.

        Parameters
        ----------
        nocopy : list, default=None
            List of sections to **not** update (such that any change affecting in these sections of ``other`` will affect ``self`` as well.
            If ``None``, defaults to :attr:`section_names.nocopy`.

        Note
        ----
        :attr:`mapping` instance is **not** updated.
        """
        if nocopy is None:
            nocopy = [section for section in section_names.nocopy if section in self]
        return super(DataBlock,self).update(other,nocopy=nocopy)

    def __getstate__(self):
        """Return this class state dictionary."""
        return {'data':self.data,'mapping':self.mapping.__getstate__()}

    def __setstate__(self, state):
        """Set the class state dictionary."""
        super(DataBlock,self).__init__(data=state['data'],mapping=BlockMapping.from_state(state['mapping']))

    def __iter__(self, **kwargs):
        """Iter. TODO: implement in C."""
        return iter(self.keys(**kwargs))

    def setdefault(self, section, name, value):
        """Iter. TODO: implement in C."""
        if (section,name) not in self:
            self.set(section,name,value)

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


def _make_getter(name):

    def getter(self, key, *args, **kwargs):
        """:meth:`DataBlock.{}` for the :attr:``block`` section.""".format(name)
        return getattr(self.block,name)(self.section,key,*args,**kwargs)

    return getter


for name in dir(DataBlock):

    if name.startswith('get'):
        setattr(SectionBlock,name,_make_getter(name))
