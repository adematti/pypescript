.. _user-framework:

Framework
=========

Modules
-------

**pypescript** articulates **modules** (e.g. a theory model). As in CosmoSIS, they should include the following routines:

  - ``setup``: setup module (called at the beginning)
  - ``execute``: execute, i.e. do calculation (called at each iteration)
  - ``cleanup``: cleanup, i.e. free variables if needed (called at the end)

**pypescript** accepts Python modules. These can either inherit from :class:`~pypescript.module.BaseModule`:

.. code-block:: python

  class MyModule(BaseModule):

    # Attributes: config_block (holds config parameters) and data_block (holds all data used in the run)

    def setup(self):
        # setup module (called at the beginning)

    def execute(self):
        # execute, i.e. do calculation (called at each iteration)

    def cleanup(self):
        # cleanup, i.e. free variables if needed (called at the end)

Or have the three functions in a file:

.. code-block:: python

  def setup(name, config_block, data_block):
      # setup module (called at the beginning)

  def execute(name, config_block, data_block):
      # execute, i.e. do calculation (called at each iteration)

  def cleanup(name, config_block, data_block):
      # cleanup, i.e. free variables if needed (called at the end)

``name`` is the module local name, set at run time.
``config_block`` and ``data_block`` inherit (are) from the dictionary-like :class:`~pypescript.block.DataBlock`,
where elements can be accessed through ``(section, name)``.
When creating new sections, it is good practice to add them to :root:`pypescript/section_names.yaml`, reinstall
and use the Python variable instead, e.g. ``section_names.my_section`` (to avoid typos).

It is also fairly easy to write modules for **pypescript** in C/C++/Fortran. Examples are provided in the template library :mod:`~pypescript.template_lib`:

  - C: :mod:`~pypescript.template_lib.module_c.module.c`
  - C++: :mod:`~pypescript.template_lib.module_cpp.module.cpp`
  - Fortran: :mod:`~pypescript.template_lib.module_f90.module.f90`

Information about these modules and how to compile them are provided in corresponding "{module_name}.yaml" files.


Inheritance diagram
-------------------

A :class:`~pypescript.module.BasePipeline` inherits from :class:`~pypescript.module.BaseModule` and can setup, execute and cleanup several modules.
Then, your own modules can inherit from these classes.
In the library **template_lib**, :class:`~pypescript.template_lib.likelihood.BaseLikelihood` is a :class:`~pypescript.module.BaseModule` that computes ``loglkl`` based on some data and model.

In diagrammatic representation (``BaseModule.plot_inheritance_graph(graph_fn)``):

  .. image:: ../static/inheritance.png

Then, one can script a pipeline linking different modules together in a tree structure.
An example of such a script is provided in :ref:`user-scripting`.
