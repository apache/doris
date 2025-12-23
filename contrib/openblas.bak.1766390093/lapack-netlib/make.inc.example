####################################################################
#  LAPACK make include file.                                       #
#  LAPACK, Version 3.8.0                                           #
#  November 2017                                                   #
####################################################################

SHELL = /bin/sh

#  CC is the C compiler, normally invoked with options CFLAGS.
#
CC = gcc
CFLAGS = -O3

#  Modify the FC and FFLAGS definitions to the desired compiler
#  and desired compiler options for your machine.  NOOPT refers to
#  the compiler options desired when NO OPTIMIZATION is selected.
#
#  Note: During a regular execution, LAPACK might create NaN and Inf
#  and handle these quantities appropriately. As a consequence, one
#  should not compile LAPACK with flags such as -ffpe-trap=overflow.
#
FC = gfortran
FFLAGS = -O2 -frecursive
FFLAGS_DRV = $(FFLAGS)
FFLAGS_NOOPT = -O0 -frecursive

#  Define LDFLAGS to the desired linker options for your machine.
#
LDFLAGS =

#  The archiver and the flag(s) to use when building an archive
#  (library).  If your system has no ranlib, set RANLIB = echo.
#
AR = ar
ARFLAGS = cr
RANLIB = ranlib

#  Timer for the SECOND and DSECND routines
#
#  Default:  SECOND and DSECND will use a call to the
#  EXTERNAL FUNCTION ETIME
#TIMER = EXT_ETIME
#  For RS6K:  SECOND and DSECND will use a call to the
#  EXTERNAL FUNCTION ETIME_
#TIMER = EXT_ETIME_
#  For gfortran compiler:  SECOND and DSECND will use a call to the
#  INTERNAL FUNCTION ETIME
TIMER = INT_ETIME
#  If your Fortran compiler does not provide etime (like Nag Fortran
#  Compiler, etc...) SECOND and DSECND will use a call to the
#  INTERNAL FUNCTION CPU_TIME
#TIMER = INT_CPU_TIME
#  If none of these work, you can use the NONE value.
#  In that case, SECOND and DSECND will always return 0.
#TIMER = NONE

#  Uncomment the following line to include deprecated routines in
#  the LAPACK library.
#
#BUILD_DEPRECATED = Yes

#  LAPACKE has the interface to some routines from tmglib.
#  If LAPACKE_WITH_TMG is defined, add those routines to LAPACKE.
#
#LAPACKE_WITH_TMG = Yes

#  Location of the extended-precision BLAS (XBLAS) Fortran library
#  used for building and testing extended-precision routines.  The
#  relevant routines will be compiled and XBLAS will be linked only
#  if USEXBLAS is defined.
#
#USEXBLAS = Yes
#XBLASLIB = -lxblas

#  The location of the libraries to which you will link.  (The
#  machine-specific, optimized BLAS library should be used whenever
#  possible.)
#
BLASLIB      = $(TOPSRCDIR)/librefblas.a
CBLASLIB     = $(TOPSRCDIR)/libcblas.a
LAPACKLIB    = $(TOPSRCDIR)/liblapack.a
TMGLIB       = $(TOPSRCDIR)/libtmglib.a
LAPACKELIB   = $(TOPSRCDIR)/liblapacke.a
