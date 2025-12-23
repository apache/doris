## Author: Hank Anderson <hank@statease.com>
## Description: Ported from portion of OpenBLAS/Makefile.system
##              Sets various variables based on architecture.

if (X86 OR X86_64)

  if (X86)
    if (NOT BINARY)
      set(NO_BINARY_MODE 1)
    endif ()
  endif ()

  if (NOT NO_EXPRECISION)
    if (${F_COMPILER} MATCHES "GFORTRAN")
      # N.B. I'm not sure if CMake differentiates between GCC and LSB -hpa
      if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU" OR ${CMAKE_C_COMPILER_ID} STREQUAL "LSB")
        set(EXPRECISION	1)
        set(CCOMMON_OPT "${CCOMMON_OPT} -DEXPRECISION -m128bit-long-double")
        set(FCOMMON_OPT	"${FCOMMON_OPT} -m128bit-long-double")
      endif ()
      if (${CMAKE_C_COMPILER_ID} STREQUAL "Clang")
        set(EXPRECISION	1)
        set(CCOMMON_OPT "${CCOMMON_OPT} -DEXPRECISION")
        set(FCOMMON_OPT	"${FCOMMON_OPT} -m128bit-long-double")
      endif ()
    endif ()
  endif ()
endif ()

if (${CMAKE_C_COMPILER_ID} STREQUAL "Intel")
  set(CCOMMON_OPT "${CCOMMON_OPT} -wd981")
endif ()

if (DYNAMIC_ARCH)
  if (ARM64)
    set(DYNAMIC_CORE ARMV8 CORTEXA53 CORTEXA57 THUNDERX THUNDERX2T99 TSV110 EMAG8180 NEOVERSEN1 THUNDERX3T110)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 10) # SVE ACLE supported in GCC >= 10
        set(DYNAMIC_CORE ${DYNAMIC_CORE} NEOVERSEV1 NEOVERSEN2 ARMV8SVE A64FX)
      endif ()
      if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 14) # SME ACLE supported in GCC >= 14
        set(DYNAMIC_CORE ${DYNAMIC_CORE} ARMV9SME)
      endif()
    elseif (${CMAKE_C_COMPILER_ID} MATCHES "Clang")
      if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 11) # SVE ACLE supported in LLVM >= 11
        set(DYNAMIC_CORE ${DYNAMIC_CORE} NEOVERSEV1 NEOVERSEN2 ARMV8SVE A64FX)
      endif ()
      if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 19) # SME ACLE supported in LLVM >= 19
        set(DYNAMIC_CORE ${DYNAMIC_CORE} ARMV9SME)
      endif()
    endif ()
    if (DYNAMIC_LIST)
	  set(DYNAMIC_CORE ARMV8 ${DYNAMIC_LIST})
    endif ()
  endif ()
  
  if (POWER)
	  set(DYNAMIC_CORE POWER6 POWER8 POWER9 POWER10)
	  set(CCOMMON_OPT "${CCOMMON_OPT} -DHAVE_P10_SUPPORT")
  endif ()
 
  if (RISCV64)
	  set(DYNAMIC_CORE RISCV64_GENERIC RISCV64_ZVL128B RISCV64_ZVL256B) 
  endif ()

  if (X86)
    set(DYNAMIC_CORE KATMAI COPPERMINE NORTHWOOD PRESCOTT BANIAS CORE2 PENRYN DUNNINGTON NEHALEM ATHLON OPTERON OPTERON_SSE3 BARCELONA BOBCAT ATOM NANO)
  endif ()

  if (X86_64)
    set(DYNAMIC_CORE PRESCOTT CORE2)
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} PENRYN DUNNINGTON)
    endif ()
    set (DYNAMIC_CORE ${DYNAMIC_CORE} NEHALEM)
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} OPTERON OPTERON_SSE3)
    endif ()
    set (DYNAMIC_CORE ${DYNAMIC_CORE} BARCELONA) 
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} BOBCAT ATOM NANO)
    endif ()
    if (NOT NO_AVX)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} SANDYBRIDGE BULLDOZER PILEDRIVER STEAMROLLER EXCAVATOR)
    endif ()
    if (NOT NO_AVX2)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} HASWELL ZEN)
    endif ()
    if (NOT NO_AVX512)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} SKYLAKEX COOPERLAKE SAPPHIRERAPIDS)
      string(REGEX REPLACE "-march=native" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    endif ()
    if (DYNAMIC_LIST)
      set(DYNAMIC_CORE PRESCOTT ${DYNAMIC_LIST})
    endif ()
  endif ()

  if (LOONGARCH64)
    set(DYNAMIC_CORE LA64_GENERIC LA264 LA464)
  endif ()

  if (EXISTS ${PROJECT_SOURCE_DIR}/config_kernel.h)
	  message (FATAL_ERROR "Your build directory contains a file config_kernel.h, probably from a previous compilation with make. This will conflict with the cmake compilation and cause strange compiler errors - please remove the file before trying again")
  endif ()

  if (NOT DYNAMIC_CORE)
    message (STATUS "DYNAMIC_ARCH is not supported on this architecture, removing from options")
    unset(DYNAMIC_ARCH CACHE)
  endif ()
endif ()

if (${ARCH} STREQUAL "ia64")
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)

  if (${F_COMPILER} MATCHES "GFORTRAN")
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      # EXPRECISION	= 1
      # CCOMMON_OPT	+= -DEXPRECISION
    endif ()
  endif ()
endif ()

if (MIPS32 OR MIPS64)
  set(NO_BINARY_MODE 1)
endif ()

if (LOONGARCH64)
  set(NO_BINARY_MODE 1)
endif ()

if (${ARCH} STREQUAL "alpha")
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

if (ARM)
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

if (ARM64)
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

if (RISCV64)
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

