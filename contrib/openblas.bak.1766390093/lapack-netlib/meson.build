# cd build
# meson --buildtype release --prefix=$HOME/.local/lapack ..
# ninja
# ninja install

project('LAPACK', 'fortran',
  default_options : ['default_library=static', 'libdir=lib/'],
  version : '3.8.0')

subdir('BLAS/SRC')
subdir('SRC')

prec = get_option('realkind')


if prec == 'd'
  bsrc = DBLAS1 + DBLAS2 + DBLAS3
  lsrc = DZLAUX + DSLASRC
elif prec == 's'
  bsrc = SBLAS1 + SBLAS2 + SBLAS3
  lsrc = SCLAUX + SLASRC
endif

blas = library('blas', bsrc,
  install : true)

lapack = library('lapack', lsrc, ALLAUX,
  install : true)
