import re
import copy
from collections import UserDict

import yaml

from .libutils.syntax_description import yaml_parser, YamlLoader
from . import section_names


keyword_re_pattern = re.compile('\$(.*?)$')
replace_re_pattern = re.compile('\${(.*?)}')
mapping_re_pattern = re.compile('\$\&{(.*?)}')

datablock_duplicate_re_pattern = re.compile('\$\[(.*?)\]')
datablock_mapping_re_pattern = re.compile('\$\&\[(.*?)\]')

eval_re_pattern = re.compile("e'(.*?)'$")
format_re_pattern = re.compile("f'(.*?)'$")


section_sep = '.'
block_save_extension = '.npy'
main = 'main'
setup_function = 'setup'
execute_function = 'execute'
cleanup_function = 'cleanup'
common_sections = [section_names.common]

_keyword_names = ['module_base_dir','module_name','module_file','module_class',\
'datablock_set','datablock_mapping','datablock_duplicate',
'modules','setup','execute','cleanup',\
'iter','nprocs_per_task','configblock_iter','datablock_iter','datablock_key_iter',\
'mpiexec','hpc_job_dir','hpc_job_submit','hpc_job_template','hpc_job_options']
_keyword_cls = []
keywords = {}


for keyword in _keyword_names:

    #def __str__(self):
    #    return self.__class__.__name__

    #locals()[keyword] = keywords[keyword] = type(keyword,(object,),{'__str__': __str__,'keyword':'${}'.format(keyword)})()
    locals()[keyword] = keywords[keyword] = '${}'.format(keyword)
    _keyword_cls.append(keywords[keyword])


def remove_keywords(di, other=None):
    toret = {}
    if other is None: other = []
    for key,value in di.items():
        if key not in _keyword_cls and key not in other:
            toret[key] = value
    return toret


class KeywordError(Exception):

    def __init__(self, word):
        self.word = word

    def __str__(self):
        return 'Unknown keyword {}'.format(self.word)


class ParserError(Exception):

    pass


def split_sections(word, sep=section_sep, default_section=None):
    if isinstance(word,str):
        toret = tuple(word.strip().split(sep))
    elif isinstance(word,(list,tuple)):
        toret = tuple(word)
    else:
        raise TypeError('Wrong type {} for sections {}'.format(type(word),word))
    if len(toret) == 1 and default_section is not None:
        toret = (default_section,) + toret
    return toret


def join_sections(words, sep=section_sep):
    return section_sep.join(words)


def expand_sections(di, sep=section_sep):

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

    def __init__(self, data=None, string=None, parser=None):

        self.parser = parser
        if parser is None:
            self.parser = yaml_parser

        data_ = {}

        if isinstance(data,str):
            if string is None: string = ''
            string += self.read_file(data)
        elif data is not None:
            data_ = dict(data)

        if string is not None:
            data_.update(self.parser(string))

        self.data = self.raw = data_
        self._cache = {}
        self.decode()


    def read_file(self, filename):
        with open(filename,'r') as file:
            toret = file.read()
        return toret


    def search(self, *keys):

        if len(keys) == 0:
            return self.data

        def callback(data, key, *keys):
            if len(keys) == 0:
                return data[key]
            return callback(data[key],*keys)

        try:
            toret = callback(self.data,*keys)
        except KeyError as exc:
            raise ParserError('Required key "{}" does not exit'.format(section_sep.join(keys))) from exc
        return toret


    def decode(self):

        # first expand the dictionary .
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

        def callback(di):
            if isinstance(di,list):
                toret = []
                for value in di:
                    if isinstance(value,(dict,list)):
                        toret.append(callback(value))
                        continue
                    decode = self.decode_format(value)
                    if decode is not None:
                        toret.append(decode)
                        continue
                    decode = self.decode_eval(value)
                    if decode is not None:
                        toret.append(decode)
                        continue
                    toret.append(value)
                return toret
            toret = {}
            for key,value in di.items():
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
        if isinstance(word,str):
            m = re.match(keyword_re_pattern,word)
            if m:
                word = m.group(1)
                try:
                    return keywords[word]
                except KeyError:
                    raise KeywordError(word)


    def decode_replace(self, word):
        if isinstance(word,str):
            m = re.match(replace_re_pattern,word)
            if m:
                fn_sections = m.group(1).split(':')
                sections = split_sections(fn_sections[-1])
                if len(fn_sections) == 1:
                    toret = self.search(*sections)
                elif len(fn_sections) == 2:
                    fn = fn_sections[0]
                    if fn in self._cache:
                        new = self._cache[fn]
                    else:
                        new = self._cache[fn] = self.__class__(fn)
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
        if isinstance(word,str):
            m = re.match(mapping_re_pattern,word)
            if m:
                return split_sections(m.group(1))

    def decode_eval(self, word):

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

        if isinstance(word,str):
            m = re.search(format_re_pattern,word)
            import numpy as np
            if m:
                word = m.group(1)
                replaces = re.finditer('(\${.*?})', word)
                for ireplace,replace in enumerate(replaces):
                    word = word.replace(replace.group(1),self.decode_replace(replace.group(1)))
                return word
