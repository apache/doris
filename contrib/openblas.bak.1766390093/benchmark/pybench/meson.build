#
# Taken from SciPy (of course)
#
project(
  'openblas-wrap',
  'c', 'fortran',
  version: '0.1',
  license: 'BSD-3',
  meson_version: '>= 1.1.0',
  default_options: [
    'buildtype=debugoptimized',
    'b_ndebug=if-release',
    'c_std=c17',
    'fortran_std=legacy',
  ],
)

py3 = import('python').find_installation(pure: false)
py3_dep = py3.dependency()

cc = meson.get_compiler('c')

_global_c_args = cc.get_supported_arguments(
  '-Wno-unused-but-set-variable',
  '-Wno-unused-function',
  '-Wno-conversion',
  '-Wno-misleading-indentation',
)
add_project_arguments(_global_c_args, language : 'c')

# We need -lm for all C code (assuming it uses math functions, which is safe to
# assume for SciPy). For C++ it isn't needed, because libstdc++/libc++ is
# guaranteed to depend on it. For Fortran code, Meson already adds `-lm`.
m_dep = cc.find_library('m', required : false)
if m_dep.found()
  add_project_link_arguments('-lm', language : 'c')
endif

generate_f2pymod = find_program('openblas_wrap/generate_f2pymod.py')

openblas = dependency('openblas', method: 'pkg-config', required: true)
openblas_dep = declare_dependency(
  dependencies: openblas,
  compile_args: []
)


subdir('openblas_wrap')
