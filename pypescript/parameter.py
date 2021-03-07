import logging
from collections import UserList
import math
import re

import numpy as np

from .utils import BaseClass
from .config import parse_yaml


class ParamError(Exception):

    pass


class ParamBlock(UserList):

    logger = logging.getLogger('ParamBlock')

    def __init__(self, filename=None, string=None, parser=parse_yaml):

        data = {}
        self.data = []

        if isinstance(filename,self.__class__):
            self.__dict__.update(filename.__dict__)
        elif isinstance(filename,str):
            self.filename = filename
            with open(filename,'r') as file:
                if string is None: string = ''
                string += file.read()
        elif isinstance(filename,list):
            self.data = filename
        elif filename is not None:
            data = dict(filename)

        if string is not None and parser is not None:
            data.update(parser(string))

        for name,conf in data.items():
            self.data.append(Parameter(name=name,**conf))

    def __getitem__(self, name):
        if isinstance(name,str):
            name = self._get_index(name)
        return self.data[name]

    def __setitem__(self, name, item):
        if isinstance(name,str):
            if item.name != name:
                raise KeyError('Parameter {} should be indexed by name (incorrect {})'.format(item.name,name))
            try:
                name = self._get_index(name)
            except ValueError:
                self.append(item)
                return
        self.data[name] = item

    def keys(self):
        return (item.name for item in self.data)

    def _get_index(self, name):
        return list(self.keys()).index(name)

    def __contains__(self, name):
        if isinstance(name,str):
            return name in self.keys()
        return name in self.data

    def update(self, other):
        for name in other.keys():
            self[name] = other[name]


class Parameter(BaseClass):

    logger = logging.getLogger('Parameter')

    def __init__(self, name=None, value=None, fixed=None, prior=None, ref=None, proposal=None, latex=None):
        self.name = name
        self.prior = Prior(**(prior or {}))
        if value is None:
            if self.prior.proper():
                self.value = np.mean(self.prior.limits)
            else:
                raise ParamError('An initial value must be provided for parameter {}'.format(self.name))
        else:
            self.value = float(value)
        if ref is not None:
            self.ref = Prior(**ref)
        else:
            self.ref = self.prior.copy()
        self.latex = latex or self.name
        if fixed is None:
            if prior is not None or ref is not None:
                fixed = False
            else:
                fixed = True
        self.fixed = bool(fixed)
        if proposal is None:
            self.proposal = None
            if (ref is not None or prior is not None):
                if hasattr(self.ref,'scale'):
                    self.proposal = self.ref.scale
                else:
                    self.proposal = (self.ref.limits[1] - self.ref.limits[0])/2.
        else:
            self.proposal = float(proposal)

    def add_suffix(self, suffix):
        self.name = '{}_{}'.format(self.name,suffix)
        match1 = re.match('(.*)_(.)$',self.latex)
        match2 = re.match('(.*)_{(.*)}$',self.latex)
        if match1 is not None:
            self.latex = '%s_{%s,\\mathrm{%s}}' % (match1.group(1),match1.group(2),self.name)
        elif match2 is not None:
            self.latex = '%s_{%s,\\mathrm{%s}}' % (match2.group(1),match2.group(2),self.name)
        else:
            self.latex = '%s_{\\mathrm{%s}}' % (self.latex,self.name)

    @property
    def limits(self):
        return self.prior.limits

    def __getstate__(self):
        state = {}
        for key in ['name','value','latex','fixed']:
            state[key] = getattr(self,key)
        for key in ['prior','ref']:
            state[key] = getattr(self,key).__getstate__()

    def __repr__(self):
        return 'parameter {} ({})'.format(self.name,'fixed' if self.fixed else 'varied')


def Prior(dist='uniform',limits=None,**kwargs):

    if isinstance(dist,BasePrior):
        dist = dist.copy()
        if limits is not None:
            dist.set_limits(limits)
        return dist

    if dist.lower() in BasePrior.registry:
        cls = BasePrior.registry[dist.lower()]
    else:
        raise ParamError('Unable to understand prior {}; it should be one of {}'.format(dist,list(prior_registry.keys())))

    return cls(**kwargs,limits=limits)


class PriorError(Exception):

    pass


class BasePrior(BaseClass):

    logger = logging.getLogger('BasePrior')
    _keys = []

    def set_limits(self, limits=None):
        if not limits:
            limits = (-np.inf,np.inf)
        self.limits = tuple(limits)
        if self.limits[1] <= self.limits[0]:
            raise PriorError('Prior range {} has min greater than max'.format(self.limits))
        if np.isinf(self.limits).any():
            return 1
        return 0

    def isin(self, x):
        return  self.limits[0] < x < self.limits[1]

    def __call__(self, x):
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def __setstate__(self,state):
        super(BasePrior,self).__setstate__(state)
        self.set_limits(self.limits)

    def __getstate__(self):
        state = {}
        for key in ['limits'] + self._keys:
            state[key] = getattr(self,key)
        return state

    def proper(self):
        return True


class UniformPrior(BasePrior):

    logger = logging.getLogger('UniformPrior')

    def __init__(self, limits=None):
        self.set_limits(limits)

    def set_limits(self, limits=None):
        if super(UniformPrior,self).set_limits(limits) == 1:
            self.norm = 0.  # we tolerate improper priors
        else:
            self.norm = -np.log(limits[1] - limits[0])

    def __call__(self, x):
        if not self.isin(x):
            return -np.inf
        return self.norm

    def __repr__(self):
        return 'Uniform({},{})'.format(*self.limits)

    def sample(self, size=None, seed=None, rng=None):
        if not self.proper():
            raise PriorError('Cannot sample from improper prior')
        self.rng = rng or np.random.RandomState(seed=seed)
        return self.rng.uniform(*self.limits,size=size)

    def proper(self):
        return not np.isinf(self.limits).any()


class NormPrior(BasePrior):

    logger = logging.getLogger('NormPrior')
    _keys = ['loc','scale']

    def __init__(self, loc=0., scale=1., limits=None):
        self.loc = loc
        self.scale = scale
        self.set_limits(limits)

    @property
    def scale2(self):
        return self.scale**2

    def set_limits(self, limits):
        super(NormPrior,self).set_limits(limits)

        def cdf(x):
            return 0.5*(math.erf(x/math.sqrt(2.)) + 1)

        a,b = [(x-self.loc)/self.scale for x in limits]
        self.norm = np.log(cdf(b) - cdf(a)) + 0.5*np.log(2*np.pi*self.scale**2)

    def __call__(self, x):
        if not self.isin(x):
            return -np.inf
        return -0.5 * ((x-self.loc) / self.scale)**2 - self.norm

    def __repr__(self):
        return 'Normal({},{})'.format(self.loc,self.scale)

    def sample(self, size=None, seed=None, rng=None):
        self.rng = rng or np.random.RandomState(seed=seed)
        if self.limits == (-np.inf,np.inf):
            return self.rng.normal(loc=self.loc,scale=self.scale,size=size)
        samples = []
        isscalar = size is None
        if isscalar: size = 1
        while len(samples) < size:
            x = self.rng.normal(loc=self.loc,scale=self.scale)
            if self.isin(x):
                samples.append(x)
        if isscalar:
            return samples[0]
        return np.array(samples)

BasePrior.registry = {}
for cls in BasePrior.__subclasses__():
    name = cls.__name__[:-len('Prior')].lower()
    BasePrior.registry[name] = cls
