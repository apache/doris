##
## Author: Hank Anderson <hank@statease.com>
## Copyright: (c) Stat-Ease, Inc.
## Created: 12/29/14
## Last Modified: 12/29/14
## Description: Ported from the OpenBLAS/f_check perl script.
##              This is triggered by prebuild.cmake and runs before any of the code is built.
##              Appends Fortran information to config.h and Makefile.conf.

# CMake vars set by this file:
# F_COMPILER
# FC
# BU
# NOFORTRAN
# NEED2UNDERSCORES
# FEXTRALIB

# Defines set by this file:
# BUNDERSCORE
# NEEDBUNDERSCORE
# NEED2UNDERSCORES

include(CheckLanguage)
check_language(Fortran)
if(CMAKE_Fortran_COMPILER)
  enable_language(Fortran)
else()
  set (NOFORTRAN 1)
  if (NOT NO_LAPACK)
     if (NOT XXXXX)
	message(STATUS "No Fortran compiler found, can build only BLAS and f2c-converted LAPACK")
	set(C_LAPACK 1)
	if (INTERFACE64)
	  set (CCOMMON_OPT "${CCOMMON_OPT} -DLAPACK_ILP64")
  	endif ()
	set(TIMER "NONE")
     else ()
       message(STATUS "No Fortran compiler found, can build only BLAS")
     endif()  
  endif()
endif()

if (NOT ONLY_CBLAS)
  # run f_check (appends to TARGET files)

  # TODO: detect whether underscore needed, set #defines and BU appropriately - use try_compile
  # TODO: set FEXTRALIB flags a la f_check?
  if (NOT (${CMAKE_SYSTEM_NAME} MATCHES "Windows" AND x${CMAKE_Fortran_COMPILER_ID} MATCHES "IntelLLVM"))
  set(BU "_")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define BUNDERSCORE _\n"
    "#define NEEDBUNDERSCORE 1\n"
    "#define NEED2UNDERSCORES 0\n")
  else ()
	  set (FCOMMON_OPT "${FCOMMON_OPT} /fp:precise /recursive /names:lowercase /assume:nounderscore")
  endif()
else ()

  #When we only build CBLAS, we set NOFORTRAN=2
  set(NOFORTRAN 2)
  set(NO_FBLAS 1)
  #set(F_COMPILER GFORTRAN) # CMake handles the fortran compiler
  set(BU "_")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define BUNDERSCORE _\n"
    "#define NEEDBUNDERSCORE 1\n")
endif()

if (CMAKE_Fortran_COMPILER)
get_filename_component(F_COMPILER ${CMAKE_Fortran_COMPILER} NAME_WE)
string(TOUPPER ${F_COMPILER} F_COMPILER)
endif()
