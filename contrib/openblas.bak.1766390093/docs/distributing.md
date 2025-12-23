# Redistributing OpenBLAS

!!! note
    This document contains recommendations only - packagers and other
    redistributors are in charge of how OpenBLAS is built and distributed in their
    systems, and may have good reasons to deviate from the guidance given on this
    page. These recommendations are aimed at general packaging systems, with a user
    base that typically is large, open source (or freely available at least), and
    doesn't behave uniformly or that the packager is directly connected with.*

OpenBLAS has a large number of build-time options which can be used to change
how it behaves at runtime, how artifacts or symbols are named, etc. Variation
in build configuration can be necessary to acheive a given end goal within a
distribution or as an end user. However, such variation can also make it more
difficult to build on top of OpenBLAS and ship code or other packages in a way
that works across many different distros. Here we provide guidance about the
most important build options, what effects they may have when changed, and
which ones to default to.

The Make and CMake build systems provide equivalent options and yield more or
less the same artifacts, but not exactly (the CMake builds are still
experimental). You can choose either one and the options will function in the
same way, however the CMake outputs may require some renaming. To review
available build options, see `Makefile.rule` or `CMakeLists.txt` in the root of
the repository.

Build options typically fall into two categories: (a) options that affect the
user interface, such as library and symbol names or APIs that are made
available, and (b) options that affect performance and runtime behavior, such
as threading behavior or CPU architecture-specific code paths. The user
interface options are more important to keep aligned between distributions,
while for the performance-related options there are typically more reasons to
make choices that deviate from the defaults.

Here are recommendations for user interface related packaging choices where it
is not likely to be a good idea to deviate (typically these are the default
settings):

1. Include CBLAS. The CBLAS interface is widely used and it doesn't affect
   binary size much, so don't turn it off.
2. Include LAPACK and LAPACKE. The LAPACK interface is also widely used, and
   while it does make up a significant part of the binary size of the installed
   library, that does not outweigh the regression in usability when deviating
   from the default here.[^1]
3. Always distribute the pkg-config (`.pc`) and CMake `.cmake`) dependency
   detection files. These files are used by build systems when users want to
   link against OpenBLAS, and there is no benefit of leaving them out.
4. Provide the LP64 interface by default, and if in addition to that you choose
   to provide an ILP64 interface build as well, use a symbol suffix to avoid
   symbol name clashes (see the next section).

[^1]: All major distributions do include LAPACK as of mid 2023 as far as we
know. Older versions of Arch Linux did not, and that was known to cause
problems.


## ILP64 interface builds

The LP64 (32-bit integer) interface is the default build, and has
well-established C and Fortran APIs as determined by the reference (Netlib)
BLAS and LAPACK libraries. The ILP64 (64-bit integer) interface however does
not have a standard API: symbol names and shared/static library names can be
produced in multiple ways, and this tends to make it difficult to use.
As of today there is an agreed-upon way of choosing names for OpenBLAS between
a number of key users/redistributors, which is the closest thing to a standard
that there is now. However, there is an ongoing standardization effort in the
reference BLAS and LAPACK libraries, which differs from the current OpenBLAS
agreed-upon convention. In this section we'll aim to explain both.

Those two methods are fairly similar, and have a key thing in common: *using a
symbol suffix*. This is good practice; it is recommended that if you distribute
an ILP64 build, to have it use a symbol suffix containing `64` in the name.
This avoids potential symbol clashes when different packages which depend on
OpenBLAS load both an LP64 and an ILP64 library into memory at the same time.

### The current OpenBLAS agreed-upon ILP64 convention

This convention comprises the shared library name and the symbol suffix in the
shared library. The symbol suffix to use is `64_`, implying that the library
name will be `libopenblas64_.so` and the symbols in that library end in `64_`.
The central issue where this was discussed is
[openblas#646](https://github.com/xianyi/OpenBLAS/issues/646), and adopters
include Fedora, Julia, NumPy and SciPy - SuiteSparse already used it as well.

To build shared and static libraries with the currently recommended ILP64
conventions with Make:
```bash
$ make INTERFACE64=1 SYMBOLSUFFIX=64_
```

This will produce libraries named `libopenblas64_.so|a`, a pkg-config file
named `openblas64.pc`, and CMake and header files.

Installing locally and inspecting the output will show a few more details:
```bash
$ make install PREFIX=$PWD/../openblas/make64 INTERFACE64=1 SYMBOLSUFFIX=64_
$ tree .  # output slightly edited down
.
├── include
│   ├── cblas.h
│   ├── f77blas.h
│   ├── lapacke_config.h
│   ├── lapacke.h
│   ├── lapacke_mangling.h
│   ├── lapacke_utils.h
│   ├── lapack.h
│   └── openblas_config.h
└── lib
    ├── cmake
    │   └── openblas
    │       ├── OpenBLASConfig.cmake
    │       └── OpenBLASConfigVersion.cmake
    ├── libopenblas64_.a
    ├── libopenblas64_.so
    └── pkgconfig
        └── openblas64.pc
```

A key point are the symbol names. These will equal the LP64 symbol names, then
(for Fortran only) the compiler mangling, and then the `64_` symbol suffix.
Hence to obtain the final symbol names, we need to take into account which
Fortran compiler we are using. For the most common cases (e.g., gfortran, Intel
Fortran, or Flang), that means appending a single underscore. In that case, the
result is:

| base API name | binary symbol name | call from Fortran code | call from C code      |
|---------------|--------------------|------------------------|-----------------------|
| `dgemm`       | `dgemm_64_`        | `dgemm_64(...)`        | `dgemm_64_(...)`      |
| `cblas_dgemm` | `cblas_dgemm64_`   | n/a                    | `cblas_dgemm64_(...)` |

It is quite useful to have these symbol names be as uniform as possible across
different packaging systems.

The equivalent build options with CMake are:
```bash
$ mkdir build && cd build
$ cmake .. -DINTERFACE64=1 -DSYMBOLSUFFIX=64_ -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON
$ cmake --build . -j
```

Note that the result is not 100% identical to the Make result. For example, the
library name ends in `_64` rather than `64_` - it is recommended to rename them
to match the Make library names (also update the `libsuffix` entry in
`openblas64.pc` to match that rename).
```bash
$ cmake --install . --prefix $PWD/../../openblas/cmake64
$ tree .
.
├── include
│   └── openblas64
│       ├── cblas.h
│       ├── f77blas.h
│       ├── lapacke_config.h
│       ├── lapacke_example_aux.h
│       ├── lapacke.h
│       ├── lapacke_mangling.h
│       ├── lapacke_utils.h
│       ├── lapack.h
│       ├── openblas64
│       │   └── lapacke_mangling.h
│       └── openblas_config.h
└── lib
    ├── cmake
    │   └── OpenBLAS64
    │       ├── OpenBLAS64Config.cmake
    │       ├── OpenBLAS64ConfigVersion.cmake
    │       ├── OpenBLAS64Targets.cmake
    │       └── OpenBLAS64Targets-noconfig.cmake
    ├── libopenblas_64.a
    ├── libopenblas_64.so -> libopenblas_64.so.0
    └── pkgconfig
        └── openblas64.pc
```


### The upcoming standardized ILP64 convention

While the `64_` convention above got some adoption, it's slightly hacky and is
implemented through the use of `objcopy`. An effort is ongoing for a more
broadly adopted convention in the reference BLAS and LAPACK libraries, using
(a) the `_64` suffix, and (b) applying that suffix _before_ rather than after
Fortran compiler mangling. The central issue for this is
[lapack#666](https://github.com/Reference-LAPACK/lapack/issues/666).

For the most common cases of compiler mangling (a single `_` appended), the end
result will be:

| base API name | binary symbol name | call from Fortran code | call from C code      |
|---------------|--------------------|------------------------|-----------------------|
| `dgemm`       | `dgemm_64_`        | `dgemm_64(...)`        | `dgemm_64_(...)`      |
| `cblas_dgemm` | `cblas_dgemm_64`   | n/a                    | `cblas_dgemm_64(...)` |

For other compiler mangling schemes, replace the trailing `_` by the scheme in use.

The shared library name for this `_64` convention should be `libopenblas_64.so`.

Note: it is not yet possible to produce an OpenBLAS build which employs this
convention! Once reference BLAS and LAPACK with support for `_64` have been
released, a future OpenBLAS release will support it. For now, please use the
older `64_` scheme and avoid using the name `libopenblas_64.so`; it should be
considered reserved for future use of the `_64` standard as prescribed by
reference BLAS/LAPACK.


## Performance and runtime behavior related build options

For these options there are multiple reasonable or common choices.

### Threading related options

OpenBLAS can be built as a multi-threaded or single-threaded library, with the
default being multi-threaded. It's expected that the default `libopenblas`
library is multi-threaded; if you'd like to also distribute single-threaded
builds, consider naming them `libopenblas_sequential`.

OpenBLAS can be built with pthreads or OpenMP as the threading model, with the
default being pthreads. Both options are commonly used, and the choice here
should not influence the shared library name. The choice will be captured by
the `.pc` file. E.g.,:
```bash
$ pkg-config --libs openblas
-fopenmp -lopenblas

$ cat openblas.pc
...
openblas_config= ... USE_OPENMP=0 MAX_THREADS=24
```

The maximum number of threads users will be able to use is determined at build
time by the `NUM_THREADS` build option. It defaults to 24, and there's a wide
range of values that are reasonable to use (up to 256). 64 is a typical choice
here; there is a memory footprint penalty that is linear in `NUM_THREADS`.
Please see `Makefile.rule` for more details.

### CPU architecture related options

OpenBLAS contains a lot of CPU architecture-specific optimizations, hence when
distributing to a user base with a variety of hardware, it is recommended to
enable CPU architecture runtime detection. This will dynamically select
optimized kernels for individual APIs. To do this, use the `DYNAMIC_ARCH=1`
build option. This is usually done on all common CPU families, except when
there are known issues.

In case the CPU architecture is known (e.g. you're building binaries for macOS
M1 users), it is possible to specify the target architecture directly with the
`TARGET=` build option.

`DYNAMIC_ARCH` and `TARGET` are covered in more detail in the main `README.md`
in this repository.


## Real-world examples

OpenBLAS is likely to be distributed in one of these distribution models:

1. As a standalone package, or multiple packages, in a packaging ecosystem like
   a Linux distro, Homebrew, conda-forge or MSYS2.
2. Vendored as part of a larger package, e.g. in Julia, NumPy, SciPy, or R.
3. Locally, e.g. making available as a build on a single HPC cluster.

The guidance on this page is most important for models (1) and (2). These links
to build recipes for a representative selection of packaging systems may be
helpful as a reference:

- [Fedora](https://src.fedoraproject.org/rpms/openblas/blob/rawhide/f/openblas.spec)
- [Debian](https://salsa.debian.org/science-team/openblas/-/blob/master/debian/rules)
- [Homebrew](https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/openblas.rb)
- [MSYS2](https://github.com/msys2/MINGW-packages/blob/master/mingw-w64-openblas/PKGBUILD)
- [conda-forge](https://github.com/conda-forge/openblas-feedstock/blob/main/recipe/build.sh)
- [NumPy/SciPy](https://github.com/MacPython/openblas-libs/blob/main/tools/build_openblas.sh)
- [Nixpkgs](https://github.com/NixOS/nixpkgs/blob/master/pkgs/development/libraries/science/math/openblas/default.nix)
