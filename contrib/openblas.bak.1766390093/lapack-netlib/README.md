# LAPACK

[![Build Status](https://travis-ci.org/Reference-LAPACK/lapack.svg?branch=master)](https://travis-ci.org/Reference-LAPACK/lapack)
![CMake](https://github.com/Reference-LAPACK/lapack/actions/workflows/cmake.yml/badge.svg)
![Makefile](https://github.com/Reference-LAPACK/lapack/actions/workflows/makefile.yml/badge.svg)
[![Appveyor](https://ci.appveyor.com/api/projects/status/bh38iin398msrbtr?svg=true)](https://ci.appveyor.com/project/langou/lapack/)
[![codecov](https://codecov.io/gh/Reference-LAPACK/lapack/branch/master/graph/badge.svg)](https://codecov.io/gh/Reference-LAPACK/lapack)
[![Packaging status](https://repology.org/badge/tiny-repos/lapack.svg)](https://repology.org/metapackage/lapack/versions)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/Reference-LAPACK/lapack/badge)](https://securityscorecards.dev/viewer/?uri=github.com/Reference-LAPACK/lapack)

* VERSION 1.0   :  February 29, 1992
* VERSION 1.0a  :  June 30, 1992
* VERSION 1.0b  :  October 31, 1992
* VERSION 1.1   :  March 31, 1993
* VERSION 2.0   :  September 30, 1994
* VERSION 3.0   :  June 30, 1999
* VERSION 3.0 + update :  October 31, 1999
* VERSION 3.0 + update :  May 31, 2000
* VERSION 3.1   : November 2006
* VERSION 3.1.1 : February 2007
* VERSION 3.2   : November 2008
* VERSION 3.2.1 : April 2009
* VERSION 3.2.2 : June 2010
* VERSION 3.3.0 : November 2010
* VERSION 3.3.1 : April 2011
* VERSION 3.4.0 : November 2011
* VERSION 3.4.1 : April 2012
* VERSION 3.4.2 : September 2012
* VERSION 3.5.0 : November 2013
* VERSION 3.6.0 : November 2015
* VERSION 3.6.1 : June 2016
* VERSION 3.7.0 : December 2016
* VERSION 3.7.1 : June 2017
* VERSION 3.8.0 : November 2017
* VERSION 3.9.0 : November 2019
* VERSION 3.9.1 : April 2021
* VERSION 3.10.0 : June 2021
* VERSION 3.10.1 : April 2022
* VERSION 3.11.0 : November 2022
* VERSION 3.12.0 : November 2023

LAPACK is a library of Fortran subroutines for solving the most commonly
occurring problems in numerical linear algebra.

LAPACK is a freely-available software package. It can be included in commercial
software packages (and has been). We only ask that that proper credit be given
to the authors, for example by citing the LAPACK Users' Guide. The license used
for the software is the [modified BSD license](https://github.com/Reference-LAPACK/lapack/blob/master/LICENSE).

Like all software, it is copyrighted. It is not trademarked, but we do ask the
following: if you modify the source for these routines we ask that you change
the name of the routine and comment the changes made to the original.

We will gladly answer any questions regarding the software. If a modification
is done, however, it is the responsibility of the person who modified the
routine to provide support.

LAPACK is [available from GitHub](https://github.com/Reference-LAPACK/lapack).
LAPACK releases are also [available on netlib](http://www.netlib.org/lapack/).

The distribution contains (1) the Fortran source for LAPACK, and (2) its
testing programs.  It also contains (3) the Fortran reference implementation of
the Basic Linear Algebra Subprograms (the Level 1, 2, and 3 BLAS) needed by
LAPACK.  However this code is intended for use only if there is no other
implementation of the BLAS already available on your machine; the efficiency of
LAPACK depends very much on the efficiency of the BLAS.  It also contains (4)
CBLAS, a C interface to the BLAS, and (5) LAPACKE, a C interface to LAPACK.

## Installation

 - LAPACK can be installed with `make`. The configuration must be set in the
   `make.inc` file. A `make.inc.example` for a Linux machine running GNU compilers
   is given in the main directory. Some specific `make.inc` are also available in
   the `INSTALL` directory.
 - LAPACK includes also the [CMake](https://cmake.org/) build.  You will need
   to have CMake installed on your machine.  CMake will allow an easy
   installation on a Windows Machine.  An example CMake build to install the
   LAPACK library under `$HOME/.local/lapack/` is:
   ```sh
   mkdir build
   cd build
   cmake -DCMAKE_INSTALL_LIBDIR=$HOME/.local/lapack ..
   cmake --build . -j --target install
   ```
 - LAPACK can be built and installed using [vcpkg](https://github.com/Microsoft/vcpkg/) dependency manager:
   ```sh
   git clone https://github.com/Microsoft/vcpkg.git
   cd vcpkg
   ./bootstrap-vcpkg.sh  # ./bootstrap-vcpkg.bat for Windows
   ./vcpkg integrate install
   ./vcpkg install lapack
   ```
   The lapack port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please [create an issue or pull request](https://github.com/Microsoft/vcpkg) on the vcpkg repository.

## User Support

LAPACK has been thoroughly tested, on many different types of computers. The
LAPACK project supports the package in the sense that reports of errors or poor
performance will gain immediate attention from the developers. Such reports,
descriptions of interesting applications, and other comments should be sent by
email to [the LAPACK team](mailto:lapack@icl.utk.edu).

A list of known problems, bugs, and compiler errors for LAPACK is
[maintained on netlib](http://www.netlib.org/lapack/release_notes.html).
Please see as well the [GitHub issue tracker](https://github.com/Reference-LAPACK/lapack/issues).

For further information on LAPACK please read our [FAQ](http://www.netlib.org/lapack/faq.html)
and [Users' Guide](http://www.netlib.org/lapack/lug/lapack_lug.html).
A [user forum](http://icl.cs.utk.edu/lapack-forum/) and specific information for
[running LAPACK under Windows](http://icl.cs.utk.edu/lapack-for-windows/lapack/).
is also available to help you with the LAPACK library.


## Testing

LAPACK includes a thorough test suite. We recommend that, after compilation,
you run the test suite.

For complete information on the LAPACK Testing please consult LAPACK Working
Note 41 "Installation Guide for LAPACK".


## LAPACKE

LAPACK now includes the [LAPACKE](http://www.netlib.org/lapack/lapacke.html)
package.  LAPACKE is a Standard C language API for LAPACK that was born from a
collaboration of the LAPACK and INTEL Math Kernel Library teams.
