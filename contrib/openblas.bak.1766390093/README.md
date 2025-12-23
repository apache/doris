# OpenBLAS

[![Join the chat at https://gitter.im/xianyi/OpenBLAS](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/xianyi/OpenBLAS?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Cirrus CI: [![Build Status](https://api.cirrus-ci.com/github/xianyi/OpenBLAS.svg?branch=develop)](https://cirrus-ci.com/github/xianyi/OpenBLAS)



[![Build Status](https://dev.azure.com/xianyi/OpenBLAS/_apis/build/status/xianyi.OpenBLAS?branchName=develop)](https://dev.azure.com/xianyi/OpenBLAS/_build/latest?definitionId=1&branchName=develop)

OSUOSL POWERCI [![Build Status](https://powerci.osuosl.org/buildStatus/icon?job=OpenBLAS_gh%2Fdevelop)](http://powerci.osuosl.org/job/OpenBLAS_gh/job/develop/)

OSUOSL IBMZ-CI [![Build Status](http://ibmz-ci.osuosl.org/buildStatus/icon?job=OpenBLAS-Z%2Fdevelop)](http://ibmz-ci.osuosl.org/job/OpenBLAS-Z/job/develop/)
## Introduction

OpenBLAS is an optimized BLAS (Basic Linear Algebra Subprograms) library based on GotoBLAS2 1.13 BSD version.

For more information about OpenBLAS, please see:

- The documentation at [openmathlib.org/OpenBLAS/docs/](http://www.openmathlib.org/OpenBLAS/docs),
- The home page at [openmathlib.org/OpenBLAS/](http://www.openmathlib.org/OpenBLAS).

For a general introduction to the BLAS routines, please refer to the extensive documentation of their reference implementation hosted at netlib:
<https://www.netlib.org/blas>. On that site you will likewise find documentation for the reference implementation of the higher-level library LAPACK - the **L**inear **A**lgebra **Pack**age that comes included with OpenBLAS. If you are looking for a general primer or refresher on Linear Algebra, the set of six
20-minute lecture videos by Prof. Gilbert Strang on either MIT OpenCourseWare [here](https://ocw.mit.edu/resources/res-18-010-a-2020-vision-of-linear-algebra-spring-2020/) or YouTube [here](https://www.youtube.com/playlist?list=PLUl4u3cNGP61iQEFiWLE21EJCxwmWvvek) may be helpful.

## Binary Packages

We provide official binary packages for the following platform:

  * Windows x86/x86_64

You can download them from [file hosting on sourceforge.net](https://sourceforge.net/projects/openblas/files/) or from the [Releases section of the GitHub project page](https://github.com/OpenMathLib/OpenBLAS/releases).

OpenBLAS is also packaged for many package managers - see [the installation section of the docs](http://www.openmathlib.org/OpenBLAS/docs/install/) for details.

## Installation from Source

Obtain the source code from https://github.com/OpenMathLib/OpenBLAS/. Note that the default branch
is `develop` (a `master` branch is still present, but far out of date).

Build-time parameters can be chosen in `Makefile.rule`, see there for a short description of each option.
Most options can also be given directly on the command line as parameters to your `make` or `cmake` invocation.

### Dependencies

Building OpenBLAS requires the following to be installed:

* GNU Make or CMake
* A C compiler, e.g. GCC or Clang 
* A Fortran compiler (optional, for LAPACK)

In general, using a recent version of the compiler is strongly recommended.
If a Fortran compiler is not available, it is possible to compile an older version of the included LAPACK
that has been machine-translated to C.

### Normal compile

Simply invoking `make` (or `gmake` on BSD) will detect the CPU automatically.
To set a specific target CPU, use `make TARGET=xxx`, e.g. `make TARGET=NEHALEM`.
The full target list is in the file `TargetList.txt`, other build optionss are documented in Makefile.rule and
can either be set there (typically by removing the comment character from the respective line), or used on the
`make` command line. 
Note that when you run `make install` after building, you need to repeat all command line options you provided to `make`
in the build step, as some settings like the supported maximum number of threads are automatically derived from the
build host by default, which might not be what you want.
For building with `cmake`, the usual conventions apply, i.e. create a build directory either underneath the toplevel
OpenBLAS source directory or separate from it, and invoke `cmake` there with the path to the source tree and any 
build options you plan to set.

For more details, see the [Building from source](http://www.openmathlib.org/OpenBLAS/docs/install/#building-from-source)
section in the docs.

### Cross compile

Set `CC` and `FC` to point to the cross toolchains, and if you use `make`, also set `HOSTCC` to your host C compiler.
The target must be specified explicitly when cross compiling.

Examples:

* On a Linux system, cross-compiling to an older MIPS64 router board:
  ```sh
  make BINARY=64 CC=mipsisa64r6el-linux-gnuabi64-gcc FC=mipsisa64r6el-linux-gnuabi64-gfortran HOSTCC=gcc TARGET=P6600
  ```
*  or to a Windows x64 host: 
  ```sh
  make CC="i686-w64-mingw32-gcc -Bstatic" FC="i686-w64-mingw32-gfortran -static-libgfortran" TARGET=HASWELL BINARY=32 CROSS=1 NUM_THREADS=20 CONSISTENT_FPCSR=1 HOSTCC=gcc
  ```

You can find instructions for other cases both in the "Supported Systems" section below and in
the [Building from source docs](http://www.openmathlib.org/OpenBLAS/docs/install).
The `.yml` scripts included with the sources (which contain the
build scripts for the "continuous integration" (CI) build tests automatically run on every proposed change to the sources) may also provide additional hints.

When compiling for a more modern CPU target of the same architecture, e.g. `TARGET=SKYLAKEX` on a `HASWELL` host, option `CROSS=1` can be used to suppress the automatic invocation of the tests at the end of the build.

### Debug version

A debug version can be built using `make DEBUG=1`.

### Compile with MASS support on Power CPU (optional)

The [IBM MASS](https://www.ibm.com/support/home/product/W511326D80541V01/other_software/mathematical_acceleration_subsystem) library consists of a set of mathematical functions for C, C++, and Fortran applications that are tuned for optimum performance on POWER architectures.
OpenBLAS with MASS requires a 64-bit, little-endian OS on POWER.
The libr