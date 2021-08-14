import os
import re
import copy
from collections import UserDict

import yaml

from .libutils.syntax_description import yaml_parser, YamlLoader, split_sections, join_sections, search_in_dict, ParserError
from . import section_names


keyword_re_pattern = re.compile('\$(.*?)$')
replace_re_pattern = re.compile('\${(.*?)}')
mapping_re_pattern = re.compile('\$\&{(.*?)}')
repeat_re_pattern = re.compile('.*\$\((.*?)\)')

datablock_duplicate_re_pattern = re.compile('\$\[(.*?)\]')
datablock_mapping_re_pattern = re.compile('\$\&\[(.*?)\]')

eval_re_pattern = re.compile("e'(.*?)'$")
format_re_pattern = re.compile("f'(.*?)'$")

section_sep = '.'
module_function_sep = ':'
module_reference = '&'
block_save_extension = '.npy'
main = 'main'
setup_function = 'setup'
execute_function = 'execute'
cleanup_function = 'cleanup'
common_sections = [section_names.common]
copy_if_datablock_copy = '_copy_if_datablock_copy'

_keyword_names = ['module_base_dir','module_name','module_file','module_class',\
'datablock_set','datablock_mapping','datablock_duplicate',
'modules','setup','execute','cleanup',\
'iter','nprocs_per_task','configblock_iter','datablock_iter','datablock_key_iter',\
'mpiexec','hpc_job_dir','hpc_job_submit','hpc_job_template','hpc_job_options']
_keywords = {}

# add keywords to local dictionary
for keyword in _keyword_names:
    locals()[keyword] = _keywords[keyword] = '${}'.format(keyword)


class KeywordError(Exception):

    """Exception raised when issue with **pypescript** keyword."""

    def __init__(self, word):
        self.word = word

    def __str__(self):
        return 'Unknown keyword {}'.format(self.word)


def remove_keywords(di, others=None):
    """Remove **pypescript** keyword entries (and other keys ``others``) from input dictionary ``di``."""
    toret = {}
    if others is None: others = []
    for key,value in di.items():
        if key not in _keywords and key not in others:
            toret[key] = value
    return toret


def expand_sections(di, sep=section_sep):
    """
    Recursively replace ``section_sep`` separated ``di`` keys by nested dictionary.

    >>> expand_sections({'section1.section2':'value'},sep='.')
    {'section1': {'section2': 'value'}}
    """
    toret = {}
    for key,value in di.items():
        if sep is not None:
            keys = split_sections(key,sep=sep)
        else:
            keys = key
            if not isinstance(keys,(list,tuple)):
                keys = (key,)
        if len(keys) > 1:
            key,value = keys[0],{sep.join(keys[1:]) if sep is not None else keys[1:]:value}
        else:
            key = keys[0]
        if isinstance(value,dict):
            tmp = toret.get(key,{})
            tmp.update(expand_sections(value,sep=sep))
            toret[key] = tmp
        else:
            toret[key] = value
    return toret


def collapse_sections(di, maxdepth=None, sep=section_sep):
    """
    Collapse nested dictionaries up to ``maxdepth``.

    >>> collapse_sections({'section1': {'section2': {'section3': 'value'}}},maxdepth=2,sep='.')
    {'section1.section2': {'section3': 'value'}}
    """
    def callback(di, maxdepth):
        toret = {}
        for key,value in di.items():
            if not isinstance(key,tuple):
                key = (key,)
            if maxdepth != 1 and isinstance(value,dict):
                tmp = callback(value,maxdepth - 1 if maxdepth is not None else None)
                for key2,value2 in tmp.items():
                    toret[key + key2] = value2
            else:
                toret[key] = value
        return toret

    if maxdepth is not None and maxdepth < 1:
        raise ValueError('maxepth = {} should be > 1'.format(maxdepth))
    toret = callback(di,maxdepth)
    if sep is not None:
        toret = {sep.join(key):value for key,value in toret.items()}

    return toret


class Decoder(UserDict):
    """
    Class that decodes configuration dictionary, taking care of template forms.

    Attributes
    ----------
    data : dict
        Decoded configuration dictionary.

    raw : dict
        Raw (without decoding of template forms) configuration dictionary.

    filename : string
        Path to corresponding configuration file.

    parser : callable
        *yaml* parser.
    """
    def __init__(self, data=None, string=None, parser=None, decode=True, **kwargs):
        """
        Initialize :class:`Decoder`.

        Parameters
        ----------
        data : dict, string, default=None
            Dictionary or path to a configuration *yaml* file to decode.

        string : string
            If not ``None``, *yaml* format string to decode.
            Added on top of ``data``.

        parser : callable, default=yaml_parser
            Function that parses *yaml* string into a dictionary.
            Used when ``data`` is string, or ``string`` is not ``None``.

        decode : bool, default=True
            Whether to decode configuration dictionary, i.e. solve template forms.

        kwargs : dict
            Arguments for :func:`parser`.
        """
        self.parser = parser
        if parser is None:
            self.parser = yaml_parser

        data_ = {}

        self.base_dir = '.'
        if isinstance(data,str):
            if string is None: string = ''
            #if base_dir is None: self.base_dir = os.path.dirname(data)
            string += self.read_file(data)
        elif data is not None:
            data_ = dict(data)

        if string is not None:
            data_.update(self.parser(string,**kwargs))

        self.data = self.raw = data_
        self._cache = {}
        if decode: self.decode()

    def read_file(self, filename):
        """Read file at path ``filename``."""
        with open(filename,'r') as file:
            toret = file.read()
        return toret

    def search(self, *keys):
        """Search value corresponding to the input sequence of keys."""
        return search_in_dict(self.data,*keys)

    def decode(self):
        """
        Decode description dictionary :attr:`data`:

        - expand ``section.name: value`` entries into ``{'section': {'name': 'value'}}`` dictionary
        - generate repeats $(%)
        - replace ``${filename:section.name}`` by corresponding value in configuration file at path ``filename``,
          at ``section`` , ``name`` keys.
        - replace ``f'here is the value: ${filename:section.name}'`` templates by ``'here is the value: value'``
        - replace ``e'42 + ${filename:section.name}' forms by ``42 + value``
        - decode :class:`DataBlock` duplicate: $[section1.name1] = $[section2.name2]
        - decode :class:`DataBlock` mapping: $[section1.name1] = &$[section2.name2]
        - decode :class:`DataBlock` set: $[section1.name1] = value
        - decode **pypescript** keywords (starting with '$')
        - decode :class:`ConfigBlock` mapping: &${section.name}
        """
        # first expand the dictionary
        def callback(di):

            toret = {}
            for key,value in di.items():
                if (not isinstance(key,str)) or re.match(replace_re_pattern,key) or re.match(datablock_duplicate_re_pattern,key):
                    toret[key] = value
                    continue
                keys = split_sections(key)
                if len(keys) > 1:
                    key,value = keys[0],{join_sections(keys[1:]):value}
                else:
                    key = keys[0]
                if isinstance(value,dict):
                    tmp = toret.get(key,{})
                    tmp.update(callback(value))
                    toret[key] = tmp
                else:
                    toret[key] = value
            return toret

        self.data = callback(self.data)

        # then expand repeats
        def callback(fulldi, keys=None, placeholder=None):
            keys = keys or []
            oldkeys = set()
            di = search_in_dict(fulldi,*keys)
            isscalar = not isinstance(di,(dict,list))
            if isscalar: # di not necessarily dict, if e.g. global: value
                di = [di]
            items = list(di.items()) if isinstance(di,dict) else list(enumerate(di))
            for key,value in items:
                repeat = self.decode_repeat(value,placeholder=placeholder)
                if repeat is None:
                    di[key] = value
                elif isinstance(repeat,str):
                    di[key] = repeat
                else:
                    newvalue,newkey,rootkey,placeholder = repeat
                    di[key] = newvalue
                    fulldi[newkey] = copy.deepcopy(fulldi[rootkey])
                    oldkeys |= set([rootkey])
                    oldkeys |= callback(fulldi,[newkey],placeholder=placeholder)
                if isinstance(di[key],(dict,list)):
                    oldkeys |= callback(fulldi,keys + [key],placeholder=placeholder)
            if isscalar:
                tmp = fulldi
                for key in keys[:-1]:
                    tmp = fulldi[key]
                tmp[keys[-1]] = di[0]
            return oldkeys

        oldkeys = callback(self.data)
        # remove keys containing $(%)
        for key in oldkeys:
            del self.data[key]

        # replace ${}
        def callback(di):
            toret = {}
            for key,value in di.items():
                replace = self.decode_replace(key)
                if replace is not None:
                    if value is not None:
                        raise ParserError('Key is to be replaced, but value is not None in {}: {}'.format(key,value))
                    toret = callback({**replace,**toret})
                    continue
                replace = self.decode_replace(value)
                if replace is not None:
                    toret[key] = replace
                else:
                    toret[key] = value
                if isinstance(toret[key],dict):
                    toret[key] = callback(toret[key])
            return toret

        self.data = callback(self.data)

        # interpret f'', e''
        def callback(di):
            toret = di.copy()
            items = list(di.items()) if isinstance(di,dict) else list(enumerate(di))
            for key,value in items:
                if isinstance(value,(dict,list)):
                    toret[key] = callback(value)
                    continue
                decode = self.decode_format(value)
                if decode is not None:
                    toret[key] = decode
                    continue
                decode = self.decode_eval(value)
                if decode is not None:
                    toret[key] = decode
                    continue
                toret[key] = value
            return toret

        self.data = callback(self.data)

        # decode :class:`DataBlock` duplicate: $[section1.name1] = $[section2.name2]
        # decode :class:`DataBlock` mapping: $[section1.name1] = &$[section2.name2]
        # decode :class:`DataBlock` set: $[section1.name1] = value
        def callback(di):
            toret = {}
            _duplicate, _mapping, _set = {}, {}, {}
            for key,value in list(di.items()):
                if isinstance(key,str):
                    m = re.match(datablock_duplicate_re_pattern,key)
                    if m:
                        key_datablock = m.group(1)
                        if isinstance(value,str):
                            mv = re.match(datablock_duplicate_re_pattern,value)
                            if mv:
                                _duplicate[key_datablock] = mv.group(1)
                                break
                            mv = re.match(datablock_mapping_re_pattern,value)
                            if mv:
                                _mapping[key_datablock] = mv.group(1)
                                break
                        _set[key_datablock] = value
                    elif isinstance(value,dict):
                        toret[key] = callback(value)
                    else:
                        toret[key] = value
            for di,kw in zip([_duplicate,_mapping,_set],[datablock_duplicate,datablock_mapping,datablock_set]):
                if di:
                    if kw not in toret:
                        toret[kw] = {}
                    toret[kw].update(di)
            return toret

        self.data = callback(self.data)

        # decode keyword in the end, such that previous $ matches are removed
        def callback(di):
            toret = {}
            for key,value in di.items():
                decode = self.decode_keyword(key)
                if decode is not None:
                    toret[decode] = value
                else:
                    toret[key] = value
                if isinstance(value,dict):
                    toret[key] = callback(value)
            return toret

        self.data = callback(self.data)

        # decode :class:`ConfigBlock` mapping: &${section.name}
        def callback(di):
            toret = {}
            for key,value in list(di.items()):
                if isinstance(value,dict):
                    tmp = callback(value)
                    for key2,value2 in tmp.items():
                        toret[(key,) + key2] = value2
                else:
                    key_mapping = self.decode_mapping(value)
                    if key_mapping is not None:
                        toret[(key,)] = key_mapping
                        del di[key]
            return toret

        self.mapping = callback(self.data)

    def decode_keyword(self, word):
        """
        If ``word`` matches template ``$keyword``, with ``keyword`` **pypescript** keyword, return ``keyword``.
        Else return ``None``.
        """
        if isinstance(word,str):
            m = re.match(keyword_re_pattern,word)
            if m:
                word = m.group(1)
                try:
                    return _keywords[word]
                except KeyError:
                    raise KeywordError(word)

    def decode_repeat(self, word, placeholder=None):
        """
        If ``word`` matches template ``start$(value)end``:

        - if ``value`` is ``%``, ``placeholder`` is not ``None``: replace by ``$(placeholder)``
        - else, try to find ``start$(%)end`` in data, return new word, new key in data (``startvalueend``), old key in data (``start$(%)end``) and ``value``

        Else, if ``placeholder`` is not ``None``, replace ``$%`` by ``placeholder``.
        """
        if isinstance(word,str):
            m = re.match(repeat_re_pattern,word)
            if m:
                value = m.group(1)
                if value == '%':
                    if placeholder is None: # nothing to do
                        return
                    return self.decode_repeat(word.replace('$(%)','$({})'.format(placeholder)),placeholder=None)
                newword = word.replace('$({})'.format(value),value)
                newkey = newword
                if re.match(replace_re_pattern,newword): # $(value) is in replace pattern ${}: replace by value
                    word = re.match(replace_re_pattern,word).group(1)
                    newkey = word.replace('$({})'.format(value),value)
                # startvalueend already in data: nothing to do
                if newkey in self.data:
                    return newword
                # else try to find matching start$(%)end in data
                key_pattern = word.replace('$({})'.format(value),'$(%)')
                for key in self.data:
                    if key == key_pattern:
                        return newword, newkey, key, value
            if placeholder is not None:
                return word.replace('$%',placeholder)

    def decode_replace(self, word):
        """
        If ``word`` matches template ``${filename:section.name}``, return corresponding value in configuration file at path ``filename``,
        at ``section`` , ``name`` keys.
        Else return ``None``.
        """
        if isinstance(word,str):
            m = re.match(replace_re_pattern,word)
            if m:
                fn_sections = m.group(1).split(':')
                sections = split_sections(fn_sections[-1])
                if len(fn_sections) == 1:
                    toret = self.search(*sections)
                elif len(fn_sections) == 2:
                    fn = os.path.join(self.base_dir,fn_sections[0])
                    if fn in self._cache:
                        new = self._cache[fn]
                    else:
                        new = self._cache[fn] = self.__class__(fn,decode=False)
                    if not fn_sections[1]: #path: => we retrieve the whole dict
                        toret = new.data
                    else:
                        toret = new.search(*sections)
                else:
                    raise ParserError('Cannot parse {} as it contains multiple colons'.format(word))
                replace = self.decode_replace(toret)
                if replace is not None:
                    return copy.deepcopy(replace)
                return toret

    def decode_mapping(self, word):
        """
        If ``word`` matches template ``&${section.name}``, return tuple ``(section, name)``.
        Else return ``None``.
        """
        if isinstance(word,str):
            m = re.match(mapping_re_pattern,word)
            if m:
                return split_sections(m.group(1))

    def decode_eval(self, word):
        """
        If ``word`` matches template ``e'42 + ${filename:section.name}', return ``42 + value``
        Else return ``None``.
        """
        if isinstance(word,str):
            m = re.search(eval_re_pattern,word)
            import numpy as np
            if m:
                words = m.group(1)
                replaces = re.finditer('(\${.*?})', words)
                dglobals = {'np':np}
                for ireplace,replace in enumerate(replaces):
                    value = self.decode_replace(replace.group(1))
                    x = '__replace_{:d}__'.format(ireplace)
                    if x in words: # in case it's already in there
                        raise ParserError('Please do not use {} in your expression'.format(x))
                    words = words.replace(replace.group(1),x)
                    dglobals[x] = value
                return eval(words,dglobals,{})

    def decode_format(self, word):
        """
        If ``word`` matches template ``f'here is the value: ${filename:section.name}'``, return ``'here is the value: value'``
        Else return ``None``.
        """
        if isinstance(word,str):
            m = re.search(format_re_pattern,word)
            import numpy as np
            if m:
                word = m.group(1)
                replaces = re.finditer('(\${.*?})', word)
                for ireplace,replace in enumerate(replaces):
                    word = word.replace(replace.group(1),self.decode_replace(replace.group(1)))
                return word
