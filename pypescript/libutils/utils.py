"""A few utilities to walk through the **pypescript** library modules."""

import os
import re
import yaml


def mkdir(filename):
    """Try to create directory of ``filename`` and catch :class:`OSError`."""
    try:
        os.makedirs(os.path.dirname(filename)) # MPI...
    except OSError:
        return


def read_path_list(filename):
    """
    Parse list of module names into modules to be included/excluded.

    Parameters
    ----------
    filename : string
        File name of module list. This should respect the bash-syntax,
        except separators are dots.

    Returns
    -------
    include : list
        List of module name ``re`` patterns to include.

    exclude:
        List of module name ``re`` patterns to exclude.

    Example
    -------
    To include modules in directory ``dir``, write ``dir.*``.
    To exclude modules in directory ``dir`` starting with ``mod``, write ``dir.mod*``.
    """
    import fnmatch
    include, exclude = [], []

    def get_re_pattern(rule):
        #return fnmatch.translate(rule.replace(os.sep,'.'))
        return fnmatch.translate(rule)

    with open(filename,'r') as file:
        for line in file:
            rule = line.strip()
            if rule.startswith('!'):
                exclude.append(get_re_pattern(rule[1:]))
            else:
                include.append(get_re_pattern(rule))

    return include, exclude


def module_full_name(module_file, base_dir='.'):
    """
    Return module full name, starting from ``base_dir``.

    Parameters
    ----------
    module_file : string
        Module file name.

    base_dir : string, default='.'
        Base package directory.

    >>> module_full_name('/path/to/module/file.py', base_dir='/path/to')
    module.file
    """
    return os.path.relpath(os.path.splitext(module_file)[0],start=base_dir).replace(os.sep,'.').lstrip('.')


def module_file_name(full_name, base_dir='.'):
    """
    Return module file name (without extension).

    Parameters
    ----------
    full_name : string
        Module full name, starting from ``base_dir``.
        See :func:`module_full_name`.

    base_dir : string, default='.'
        Base package directory.

    >>> module_file_name('/path/to/module/file.py', base_dir='/path/to')
    module/file
    """
    return os.path.join(base_dir,full_name.replace('.',os.sep))


def walk_pype_modules(base_dir='.', include_pype_module_names=None, exclude_pype_module_names=None):
    """
    Walk through **pypescript** modules and yield (module directory, module full name (w.r.t. ``base_dir``), description file name, desciption dictionary).

    Parameters
    ----------
    base_dir : string, default='.'
        Root of the directory tree to explore.

    include_pype_module_names : list, default=None
        List of module names (w.r.t. ``base_dir``) or regex to include.
        If ``None``, all modules in ``base_dir`` are considered.

    exclude_pype_module_names : list, default=None
        List of module names (w.r.t. ``base_dir``) or regex to exclude.
        If ``None``, no module is excluded.
    """
    from .module_description import ModuleDescription

    exclude_pype_module_names = exclude_pype_module_names or []
    extensions,modules,requirements = [],[],set()

    for module_dir,dirs,files in os.walk(base_dir,followlinks=True):
        description_files = [os.path.join(module_dir,file) for file in files if file.endswith(ModuleDescription._file_extension)]
        for description_file in description_files:
            if not ModuleDescription.isinstance(description_file):
                continue
            descriptions = ModuleDescription.load(description_file,decode_eval=False)
            if not isinstance(descriptions,list):
                descriptions = [descriptions]
            for description in descriptions:
                full_name = module_full_name(os.path.join(module_dir,description['name']),base_dir=base_dir)
                if include_pype_module_names is not None:
                    toinclude = False
                    for include in include_pype_module_names:
                        if re.match(include,full_name):
                            toinclude = True
                            break
                    if not toinclude: continue
                toexclude = False
                for exclude in exclude_pype_module_names:
                    if re.match(exclude,full_name):
                        toexclude = True
                        break
                if toexclude: continue
                yield module_dir, full_name, description_file, description
