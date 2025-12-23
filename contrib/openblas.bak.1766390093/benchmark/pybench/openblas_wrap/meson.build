# find numpy & f2py includes
inc_numpy = run_command(py3,
  ['-c', 'import os; os.chdir(".."); import numpy; print(numpy.get_include())'],
  check : true
).stdout().strip()

inc_f2py = run_command(py3,
    ['-c', 'import os; os.chdir(".."); import numpy.f2py; print(numpy.f2py.get_include())'],
    check : true
).stdout().strip()


inc_np = include_directories(inc_numpy, inc_f2py)
fortranobject_c = inc_f2py / 'fortranobject.c'


fortranobject_lib = static_library('_fortranobject',
  fortranobject_c,
#  c_args: numpy_nodepr_api,
  dependencies: py3_dep,
  include_directories: [inc_np, inc_f2py],
  gnu_symbol_visibility: 'hidden',
)
fortranobject_dep = declare_dependency(
  link_with: fortranobject_lib,
  include_directories: [inc_np, inc_f2py],
)


# f2py generated wrappers

flapack_module = custom_target('flapack_module',
  output: ['_flapackmodule.c'],
  input: 'blas_lapack.pyf.src',
  command: [generate_f2pymod, '@INPUT@', '-o', '@OUTDIR@'],
)

py3.extension_module('_flapack',
  flapack_module,
  link_args: [],  # version_link_args,
  dependencies: [openblas_dep, fortranobject_dep],
  install: true,
  subdir: 'openblas_wrap'
)


py3.install_sources(
  ['__init__.py'],
  subdir: 'openblas_wrap'
)
