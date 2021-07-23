import os
import re
import copy
from collections import UserDict

import yaml


keyword_re_pattern = re.compile('\$(.*?)$')
replace_re_pattern = re.compile('\${(.*?)}')

eval_re_pattern = re.compile("e'(.*?)'$")
format_re_pattern = re.compile("f'(.*?)'$")

section_sep = '.'

others = '$others'

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


class YamlLoader(yaml.SafeLoader):

    pass

YamlLoader.add_implicit_resolver(u'tag:yaml.org,2002:float',
                            re.compile(u'''^(?:
                             [-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+]?[0-9]+)?
                            |[-+]?(?:[0-9][0-9_]*)(?:[eE][-+]?[0-9]+)
                            |\\.[0-9_]+(?:[eE][-+][0-9]+)?
                            |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*
                            |[-+]?\\.(?:inf|Inf|INF)
                            |\\.(?:nan|NaN|NAN))$''', re.X),
                            list(u'-+0123456789.'))

YamlLoader.add_implicit_resolver('!none',re.compile('None$'),first='None')

def none_constructor(loader, node):
  return None

YamlLoader.add_constructor('!none',none_constructor)


def yaml_parser(string, index=None):
    """Parse string in the *yaml* format."""
    # https://stackoverflow.com/questions/30458977/yaml-loads-5e-6-as-string-and-not-a-number
    alls = list(yaml.load_all(string,Loader=YamlLoader))
    if index is not None:
        if isinstance(index,dict):
            for config in alls:
                if all([config.get(name) == value for name,value in index.items()]):
                    break
        else:
            config = alls[index]
    else:
        config = yaml.load(string,Loader=YamlLoader)
    data = dict(config)
    return data


class ParserError(Exception):

    pass


class Decoder(UserDict):

    def __init__(self, data=None, string=None, parser=None, filename=None, decode=True, decode_eval=True, **kwargs):

        self.parser = parser
        if parser is None:
            self.parser = yaml_parser

        data_ = {}

        self.filename = filename
        if isinstance(data,str):
            if string is None: string = ''
            self.filename = data
            string += self.read_file(data)
        elif data is not None:
            data_ = dict(data)

        if string is not None:
            data_.update(self.parser(string,**kwargs))

        self.data = self.raw = data_
        self._cache = {}
        if decode: self.decode(decode_eval=decode_eval)


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
            raise ParserError('Required key "{}" does not exist'.format(section_sep.join(keys))) from exc
        return toret


    def decode(self, decode_eval=True):

        # first expand the dictionary .
        def callback(di):

            toret = {}
            for key,value in di.items():
                if (not isinstance(key,str)) or re.match(replace_re_pattern,key):
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
                    if decode_eval:
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
                if decode_eval:
                    decode = self.decode_eval(value)
                    if decode is not None:
                        toret[key] = decode
                        continue
                toret[key] = value
            return toret

        self.data = callback(self.data)

    def decode_replace(self, word):
        if isinstance(word,str):
            m = re.match(replace_re_pattern,word)
            if m:
                fn_sections = m.group(1).split(':')
                sections = split_sections(fn_sections[-1])
                if len(fn_sections) == 1:
                    toret = self.search(*sections)
                elif len(fn_sections) == 2:
                    fn_index = fn_sections[0]
                    if fn_index in self._cache:
                        new = self._cache[fn_index]
                    else:
                        match = re.match('(.*)#(.*?)$',fn_index)
                        if match:
                            fn = match.group(1)
                            try:
                                index = int(match.group(2))
                            except ValueError:
                                index = {'name':match.group(2)}
                        else:
                            fn = fn_index
                            index = None
                        if fn:
                            fn = os.path.join(os.path.dirname(self.filename),fn)
                        else:
                            fn = self.filename
                        new = self._cache[fn_index] = self.__class__(fn,index=index,decode=False)
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
