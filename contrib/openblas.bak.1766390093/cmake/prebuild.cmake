##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from OpenBLAS/Makefile.prebuild
##              This is triggered by system.cmake and runs before any of the code is built.
##              Creates config.h and Makefile.conf by first running the c_check perl script (which creates those files).
##              Next it runs f_check and appends some fortran information to the files.
##              Then it runs getarch and getarch_2nd for even more environment information.
##              Finally it builds gen_config_h for use at build time to generate config.h.

# CMake vars set by this file:
# CORE
# LIBCORE
# NUM_CORES
# HAVE_MMX
# HAVE_SSE
# HAVE_SSE2
# HAVE_SSE3
# MAKE
# SBGEMM_UNROLL_M
# SBGEMM_UNROLL_N
# SGEMM_UNROLL_M
# SGEMM_UNROLL_N
# DGEMM_UNROLL_M
# DGEMM_UNROLL_M
# QGEMM_UNROLL_N
# QGEMM_UNROLL_N
# CGEMM_UNROLL_M
# CGEMM_UNROLL_M
# ZGEMM_UNROLL_N
# ZGEMM_UNROLL_N
# XGEMM_UNROLL_M
# XGEMM_UNROLL_N
# CGEMM3M_UNROLL_M
# CGEMM3M_UNROLL_N
# ZGEMM3M_UNROLL_M
# ZGEMM3M_UNROLL_M
# XGEMM3M_UNROLL_N
# XGEMM3M_UNROLL_N

# CPUIDEMU = ../../cpuid/table.o


if (DEFINED CPUIDEMU)
  set(EXFLAGS "-DCPUIDEMU -DVENDOR=99")
endif ()

if (BUILD_KERNEL)
  # set the C flags for just this file
  set(GETARCH2_FLAGS "-DBUILD_KERNEL")
  set(TARGET_CONF "config_kernel.h")
  set(TARGET_CONF_DIR ${PROJECT_BINARY_DIR}/kernel_config/${TARGET_CORE})
else()
  set(TARGET_CONF "config.h")
  set(TARGET_CONF_DIR ${PROJECT_BINARY_DIR})
endif ()

set(TARGET_CONF_TEMP "${PROJECT_BINARY_DIR}/${TARGET_CONF}.tmp")

# c_check
set(FU "")
if (APPLE OR (MSVC AND NOT (${CMAKE_C_COMPILER_ID} MATCHES "Clang" OR ${CMAKE_C_COMPILER_ID} MATCHES "IntelLLVM")))
  set(FU "_")
endif()
if(MINGW AND NOT MINGW64)
  set(FU "_")
endif()

set(COMPILER_ID ${CMAKE_C_COMPILER_ID})
if (${COMPILER_ID} STREQUAL "GNU")
  set(COMPILER_ID "GCC")
endif ()

string(TOUPPER ${ARCH} UC_ARCH)

file(WRITE ${TARGET_CONF_TEMP}
  "#define OS_${HOST_OS}\t1\n"
  "#define ARCH_${UC_ARCH}\t1\n"
  "#define C_${COMPILER_ID}\t1\n"
  "#define __${BINARY}BIT__\t1\n"
  "#define FUNDERSCORE\t${FU}\n")

if (${HOST_OS} STREQUAL "WINDOWSSTORE")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define OS_WINNT\t1\n")
endif ()

# f_check
if (NOT NOFORTRAN)
  include("${PROJECT_SOURCE_DIR}/cmake/f_check.cmake")
else ()
 file(APPEND ${TARGET_CONF_TEMP}
   "#define BUNDERSCORE _\n"
   "#define NEEDBUNDERSCORE 1\n")
 set(BU "_")
endif ()

# Cannot run getarch on target if we are cross-compiling
if (DEFINED CORE AND CMAKE_CROSSCOMPILING AND NOT (${HOST_OS} STREQUAL "WINDOWSSTORE"))
  # Write to config as getarch would
  if (DEFINED TARGET_CORE)
  set(TCORE ${TARGET_CORE})
  else()
  set(TCORE ${CORE})
  endif()

  # TODO: Set up defines that getarch sets up based on every other target
  # Perhaps this should be inside a different file as it grows larger
  file(APPEND ${TARGET_CONF_TEMP}
    "#define ${TCORE}\n"
    "#define CORE_${TCORE}\n"
    "#define CHAR_CORENAME \"${TCORE}\"\n")
  if ("${TCORE}" STREQUAL "CORE2")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t1048576\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t256\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define SLOCAL_BUFFER_SIZE\t16384\n"
      "#define DLOCAL_BUFFER_SIZE\t16384\n"
      "#define CLOCAL_BUFFER_SIZE\t16384\n"
      "#define ZLOCAL_BUFFER_SIZE\t16384\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "ATOM")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t24576\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define SLOCAL_BUFFER_SIZE\t16384\n"
      "#define DLOCAL_BUFFER_SIZE\t8192\n"
      "#define CLOCAL_BUFFER_SIZE\t16384\n"
      "#define ZLOCAL_BUFFER_SIZE\t8192\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 1)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "PRESCOTT")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t16384\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t1048576\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define SLOCAL_BUFFER_SIZE\t8192\n"
      "#define DLOCAL_BUFFER_SIZE\t8192\n"
      "#define CLOCAL_BUFFER_SIZE\t8192\n"
      "#define ZLOCAL_BUFFER_SIZE\t8192\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "NEHALEM")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define SLOCAL_BUFFER_SIZE\t65535\n"
      "#define DLOCAL_BUFFER_SIZE\t32768\n"
      "#define CLOCAL_BUFFER_SIZE\t65536\n"
      "#define ZLOCAL_BUFFER_SIZE\t32768\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 4)
      set(SGEMM_UNROLL_N 8)
      set(DGEMM_UNROLL_M 2)
      set(DGEMM_UNROLL_N 8)
      set(CGEMM_UNROLL_M 2)
      set(CGEMM_UNROLL_N 4)
      set(ZGEMM_UNROLL_M 1)
      set(ZGEMM_UNROLL_N 4)
      set(CGEMM3M_UNROLL_M 4)
      set(CGEMM3M_UNROLL_N 8)
      set(ZGEMM3M_UNROLL_M 2)
      set(ZGEMM3M_UNROLL_N 8)
  elseif ("${TCORE}" STREQUAL "SANDYBRIDGE")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_AVX\n"
      "#define SLOCAL_BUFFER_SIZE\t24576\n"
      "#define DLOCAL_BUFFER_SIZE\t16384\n"
      "#define CLOCAL_BUFFER_SIZE\t32768\n"
      "#define ZLOCAL_BUFFER_SIZE\t24576\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 1)
      set(ZGEMM_UNROLL_N 4)
      set(CGEMM3M_UNROLL_M 4)
      set(CGEMM3M_UNROLL_N 8)
      set(ZGEMM3M_UNROLL_M 2)
      set(ZGEMM3M_UNROLL_N 8)
  elseif ("${TCORE}" STREQUAL "HASWELL")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_AVX\n"
      "#define HAVE_AVX2\n"
      "#define HAVE_FMA3\n"
      "#define SLOCAL_BUFFER_SIZE\t20480\n"
      "#define DLOCAL_BUFFER_SIZE\t32768\n"
      "#define CLOCAL_BUFFER_SIZE\t16384\n"
      "#define ZLOCAL_BUFFER_SIZE\t12288\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(HAVE_AVX2 1)
      set(HAVE_FMA3 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 8)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "SKYLAKEX")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_AVX\n"
      "#define HAVE_AVX2\n"
      "#define HAVE_FMA3\n"
      "#define HAVE_AVX512VL\n"
      "#define SLOCAL_BUFFER_SIZE\t28672\n"
      "#define DLOCAL_BUFFER_SIZE\t12288\n"
      "#define CLOCAL_BUFFER_SIZE\t12288\n"
      "#define ZLOCAL_BUFFER_SIZE\t8192\n")
      set(HAVE_CMOV 1)
      set(HAVE_MMX 1)
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(HAVE_AVX2 1)
      set(HAVE_FMA3 1)
      set(HAVE_AVX512VL 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 16)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "COOPERLAKE")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_AVX\n"
      "#define HAVE_AVX2\n"
      "#define HAVE_FMA3\n"
      "#define HAVE_AVX512VL\n"
      "#define HAVE_AVX512BF16\n"
      "#define SLOCAL_BUFFER_SIZE\t20480\n"
      "#define DLOCAL_BUFFER_SIZE\t12288\n"
      "#define CLOCAL_BUFFER_SIZE\t12288\n"
      "#define ZLOCAL_BUFFER_SIZE\t8192\n")
      set(HAVE_CMOV 1)
      set(HAVE_MMX 1)
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(HAVE_AVX2 1)
      set(HAVE_FMA3 1)
      set(HAVE_AVX512VL 1)
      set(HAVE_AVX512BF16 1)
      set(SBGEMM_UNROLL_M 16)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 16)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "SAPPHIRERAPIDS")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_CMOV\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_AVX\n"
      "#define HAVE_AVX2\n"
      "#define HAVE_FMA3\n"
      "#define HAVE_AVX512VL\n"
      "#define HAVE_AVX512BF16\n"
      "#define SLOCAL_BUFFER_SIZE\t20480\n"
      "#define DLOCAL_BUFFER_SIZE\t12288\n"
      "#define CLOCAL_BUFFER_SIZE\t12288\n"
      "#define ZLOCAL_BUFFER_SIZE\t8192\n")
      set(HAVE_CMOV 1)
      set(HAVE_MMX 1)
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(HAVE_AVX2 1)
      set(HAVE_FMA3 1)
      set(HAVE_AVX512VL 1)
      set(HAVE_AVX512BF16 1)
      set(SBGEMM_UNROLL_M 32)
      set(SBGEMM_UNROLL_N 16)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 16)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "OPTERON")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t1048576\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t32\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_3DNOW\n"
      "#define HAVE_3DNOWEX\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define SLOCAL_BUFFER_SIZE\t15360\n"
      "#define DLOCAL_BUFFER_SIZE\t15360\n"
      "#define CLOCAL_BUFFER_SIZE\t15360\n"
      "#define ZLOCAL_BUFFER_SIZE\t15360\n")
      set(HAVE_3DNOW 1)
      set(HAVE_3DNOWEX 1)
      set(HAVE_MMX 1)
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "BARCELONA")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define SLOCAL_BUFFER_SIZE\t14336\n"
      "#define DLOCAL_BUFFER_SIZE\t14336\n"
      "#define CLOCAL_BUFFER_SIZE\t14336\n"
      "#define ZLOCAL_BUFFER_SIZE\t14336\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4A 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "BULLDOZER")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t49152\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t1024000\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t32\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_AVX\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define SLOCAL_BUFFER_SIZE\t5376\n"
      "#define DLOCAL_BUFFER_SIZE\t5376\n"
      "#define CLOCAL_BUFFER_SIZE\t14336\n"
      "#define ZLOCAL_BUFFER_SIZE\t14336\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4A 1)
      set(HAVE_AVX 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 2)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 2)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "PILEDRIVER")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t16384\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t2097152\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_AVX\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define HAVE_CFLUSH\n"
      "#define HAVE_FMA3\n"
      "#define SLOCAL_BUFFER_SIZE\t6144\n"
      "#define DLOCAL_BUFFER_SIZE\t5376\n"
      "#define CLOCAL_BUFFER_SIZE\t10752\n"
      "#define ZLOCAL_BUFFER_SIZE\t10752\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_SSE4A 1)
      set(HAVE_AVX 1)
      set(HAVE_FMA3 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(HAVE_CFLUSH 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 2)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "STEAMROLLER")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t16384\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t2097152\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_AVX\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define HAVE_CFLUSH\n"
      "#define HAVE_FMA3\n"
      "#define SLOCAL_BUFFER_SIZE\t6144\n"
      "#define DLOCAL_BUFFER_SIZE\t5120\n"
      "#define CLOCAL_BUFFER_SIZE\t10240\n"
      "#define ZLOCAL_BUFFER_SIZE\t10240\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_SSE4A 1)
      set(HAVE_AVX 1)
      set(HAVE_FMA3 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(HAVE_CFLUSH 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 2)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "EXCAVATOR")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t16384\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t2097152\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_AVX\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define HAVE_CFLUSH\n"
      "#define HAVE_FMA3\n"
      "#define SLOCAL_BUFFER_SIZE\t6144\n"
      "#define DLOCAL_BUFFER_SIZE\t5120\n"
      "#define CLOCAL_BUFFER_SIZE\t10240\n"
      "#define ZLOCAL_BUFFER_SIZE\t10240\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_SSE4A 1)
      set(HAVE_AVX 1)
      set(HAVE_FMA3 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(HAVE_CFLUSH 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 2)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 2)
      set(CGEMM_UNROLL_M 4)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 2)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "ZEN")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_MMX\n"
      "#define HAVE_SSE\n"
      "#define HAVE_SSE2\n"
      "#define HAVE_SSE3\n"
      "#define HAVE_SSE4_1\n"
      "#define HAVE_SSE4_2\n"
      "#define HAVE_SSE4A\n"
      "#define HAVE_MISALIGNSSE\n"
      "#define HAVE_128BITFPU\n"
      "#define HAVE_FASTMOVU\n"
      "#define HAVE_CFLUSH\n"
      "#define HAVE_AVX\n"
      "#define HAVE_AVX2\n"
      "#define HAVE_FMA3\n"
      "#define SLOCAL_BUFFER_SIZE\t20480\n"
      "#define DLOCAL_BUFFER_SIZE\t32768\n"
      "#define CLOCAL_BUFFER_SIZE\t16384\n"
      "#define ZLOCAL_BUFFER_SIZE\t12288\n")
      set(HAVE_SSE 1)
      set(HAVE_SSE2 1)
      set(HAVE_SSE3 1)
      set(HAVE_SSE4_1 1)
      set(HAVE_SSE4_2 1)
      set(HAVE_AVX 1)
      set(HAVE_AVX2 1)
      set(HAVE_FMA3 1)
      set(HAVE_SSE4A 1)
      set(HAVE_MISALIGNSSE 1)
      set(HAVE_128BITFPU 1)
      set(HAVE_FASTMOVU 1)
      set(HAVE_CFLUSH 1)
      set(SBGEMM_UNROLL_M 8)
      set(SBGEMM_UNROLL_N 4)
      set(SGEMM_UNROLL_M 8)
      set(SGEMM_UNROLL_N 4)
      set(DGEMM_UNROLL_M 4)
      set(DGEMM_UNROLL_N 8)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 2)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 2)
      set(CGEMM3M_UNROLL_M 8)
      set(CGEMM3M_UNROLL_N 4)
      set(ZGEMM3M_UNROLL_M 4)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "ARMV5")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t32\n"
      "#define L2_SIZE\t512488\n"
      "#define L2_LINESIZE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t4\n")
    set(SGEMM_UNROLL_M 2)
    set(SGEMM_UNROLL_N 2)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
  elseif ("${TCORE}" STREQUAL "ARMV6")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t32\n"
      "#define L2_SIZE\t512488\n"
      "#define L2_LINESIZE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t4\n"
      "#define HAVE_VFP\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 2)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
  elseif ("${TCORE}" STREQUAL "ARMV7")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t32\n"
      "#define L2_SIZE\t512488\n"
      "#define L2_LINESIZE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
  elseif ("${TCORE}" STREQUAL "ARMV8")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t32\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "CORTEXA57" OR "${TCORE}" STREQUAL "CORTEXA53" OR "${TCORE}" STREQUAL "CORTEXA55")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
if ("${TCORE}" STREQUAL "CORTEXA57")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
else ()
    set(SGEMM_UNROLL_M 8)
    set(SGEMM_UNROLL_N 8)
endif ()
if ("${TCORE}" STREQUAL "CORTEXA53")
    set(DGEMM_UNROLL_M 4)
else ()
    set(DGEMM_UNROLL_M 8)
endif ()    
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "CORTEXA72" OR "${TCORE}" STREQUAL "CORTEXA73" OR "${TCORE}" STREQUAL "CORTEXA76")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t49152\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "NEOVERSEN1")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t4\n"
      "#define L2_SIZE\t1048576\n\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t48\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "NEOVERSEV1")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t4\n"
      "#define L2_SIZE\t1048576\n\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t48\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define HAVE_SVE\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 8)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 8)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "NEOVERSEN2" OR "${TCORE}" STREQUAL "ARMV9SME")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t1048576\n\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t48\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define HAVE_SVE\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "FALKOR")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t128\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "THUNDERX")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t128\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t167772164\n"
      "#define L2_LINESIZE\t128\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "THUNDERX2T99")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t8\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t8\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define L3_SIZE\t33554432\n"
      "#define L3_LINESIZE\t64\n"
      "#define L3_ASSOCIATIVE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "THUNDERX3T110")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define THUNDERX3T110\n"
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t8\n"
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t8\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define L3_SIZE\t94371840\n"
      "#define L3_LINESIZE\t64\n"
      "#define L3_ASSOCIATIVE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "TSV110")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define ARMV8\n"
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t4\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "EMAG8180")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define ARMV8\n"
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t4\n"
      "#define L2_SIZE\t5262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "VORTEX")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define ARMV8\n"
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t4\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t4\n"
      "#define L2_SIZE\t5262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "A64FX")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t256\n"
      "#define L1_CODE_ASSOCIATIVE\t8\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t256\n"
      "#define L1_DATA_ASSOCIATIVE\t8\n"
      "#define L2_SIZE\t8388608\n\n"
      "#define L2_LINESIZE\t256\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define L3_SIZE\t0\n\n"
      "#define L3_LINESIZE\t0\n\n"
      "#define L3_ASSOCIATIVE\t0\n\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define HAVE_SVE\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 8)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 8)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "ARMV8SVE" OR "${TCORE}" STREQUAL "CORTEXA510" OR "${TCORE}" STREQUAL "CORTEXX2" OR "${TCORE}" STREQUAL "ARMV9")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t32\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 8)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 8)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "P5600")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L2_SIZE 1048576\n"
      "#define DTB_SIZE 4096\n"
      "#define DTB_DEFAULT_ENTRIES 64\n")
    set(SGEMM_UNROLL_M 2)
    set(SGEMM_UNROLL_N 2)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
    set(SYMV_P 16)
  elseif ("${TCORE}" MATCHES "MIPS")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L2_SIZE 262144\n"
      "#define DTB_SIZE 4096\n"
      "#define DTB_DEFAULT_ENTRIES 64\n")
    set(SGEMM_UNROLL_M 2)
    set(SGEMM_UNROLL_N 2)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
    set(SYMV_P 16)
  elseif ("${TCORE}" STREQUAL "POWER6")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE 32768\n"
      "#define L1_DATA_LINESIZE 128\n"
      "#define L2_SIZE 524288\n"
      "#define L2_LINESIZE 128 \n"
      "#define DTB_DEFAULT_ENTRIES 128\n"
      "#define DTB_SIZE 4096\n"
      "#define L2_ASSOCIATIVE 8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 4)
    set(SYMV_P 8)
  elseif ("${TCORE}" STREQUAL "POWER8")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE 32768\n"
      "#define L1_DATA_LINESIZE 128\n"
      "#define L2_SIZE 524288\n"
      "#define L2_LINESIZE 128 \n"
      "#define DTB_DEFAULT_ENTRIES 128\n"
      "#define DTB_SIZE 4096\n"
      "#define L2_ASSOCIATIVE 8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 8)
    set(DGEMM_UNROLL_M 16)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 8)
    set(ZGEMM_UNROLL_N 2)
    set(SYMV_P 8)
  elseif ("${TCORE}" STREQUAL "POWER9" OR "${TCORE}" STREQUAL "POWER10")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE 32768\n"
      "#define L1_DATA_LINESIZE 128\n"
      "#define L2_SIZE 524288\n"
      "#define L2_LINESIZE 128 \n"
      "#define DTB_DEFAULT_ENTRIES 128\n"
      "#define DTB_SIZE 4096\n"
      "#define L2_ASSOCIATIVE 8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 8)
    set(DGEMM_UNROLL_M 16)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 8)
    set(ZGEMM_UNROLL_N 2)
    set(SYMV_P 8)
  elseif ("${TCORE}" STREQUAL "GENERIC")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE 32768\n"
      "#define L1_DATA_LINESIZE 128\n"
      "#define L2_SIZE 524288\n"
      "#define L2_LINESIZE 128 \n"
      "#define DTB_DEFAULT_ENTRIES 128\n"
      "#define DTB_SIZE 4096\n"
      "#define L2_ASSOCIATIVE 8\n")
  elseif ("${TCORE}" STREQUAL "RISCV64_GENERIC")
    file(APPEND ${TARGET_CONF_TEMP}
        "#define L1_DATA_SIZE 32768\n"
      "#define L1_DATA_LINESIZE 32\n"
      "#define L2_SIZE 1048576\n"
      "#define L2_LINESIZE 32 \n"
      "#define DTB_DEFAULT_ENTRIES 128\n"
      "#define DTB_SIZE 4096\n"
      "#define L2_ASSOCIATIVE 4\n")
  elseif ("${TCORE}" STREQUAL "LA64_GENERIC")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define DTB_DEFAULT_ENTRIES 64\n")
      set(SGEMM_UNROLL_M 2)
      set(SGEMM_UNROLL_N 8)
      set(DGEMM_UNROLL_M 2)
      set(DGEMM_UNROLL_N 8)
      set(CGEMM_UNROLL_M 1)
      set(CGEMM_UNROLL_N 4)
      set(ZGEMM_UNROLL_M 1)
      set(ZGEMM_UNROLL_N 4)
      set(CGEMM3M_UNROLL_M 2)
      set(CGEMM3M_UNROLL_N 8)
      set(ZGEMM3M_UNROLL_M 2)
      set(ZGEMM3M_UNROLL_N 8)
  elseif ("${TCORE}" STREQUAL "LA264")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define DTB_DEFAULT_ENTRIES 64\n")
      set(HAVE_LSX  1)
      set(SGEMM_UNROLL_M 2)
      set(SGEMM_UNROLL_N 8)
      set(DGEMM_UNROLL_M 8)
      set(DGEMM_UNROLL_N 4)
      set(CGEMM_UNROLL_M 8)
      set(CGEMM_UNROLL_N 4)
      set(ZGEMM_UNROLL_M 4)
      set(ZGEMM_UNROLL_N 4)
      set(CGEMM3M_UNROLL_M 2)
      set(CGEMM3M_UNROLL_N 8)
      set(ZGEMM3M_UNROLL_M 8)
      set(ZGEMM3M_UNROLL_N 4)
  elseif ("${TCORE}" STREQUAL "LA464")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define DTB_DEFAULT_ENTRIES 64\n")
      set(HAVE_LASX 1)
      set(HAVE_LSX  1)
      set(SGEMM_UNROLL_M 16)
      set(SGEMM_UNROLL_N 8)
      set(DGEMM_UNROLL_M 16)
      set(DGEMM_UNROLL_N 6)
      set(CGEMM_UNROLL_M 16)
      set(CGEMM_UNROLL_N 4)
      set(ZGEMM_UNROLL_M 8)
      set(ZGEMM_UNROLL_N 4)
      set(CGEMM3M_UNROLL_M 16)
      set(CGEMM3M_UNROLL_N 8)
      set(ZGEMM3M_UNROLL_M 16)
      set(ZGEMM3M_UNROLL_N 6)
  endif()
  set(SBGEMM_UNROLL_M 8)
  set(SBGEMM_UNROLL_N 4)

  # Or should this actually be NUM_CORES?
  if (${NUM_THREADS} GREATER 0)
    file(APPEND ${TARGET_CONF_TEMP} "#define NUM_CORES\t${NUM_THREADS}\n")
  endif()

  # GetArch_2nd
  foreach(float_char S;D;Q;C;Z;X)
    if (NOT DEFINED ${float_char}GEMM_UNROLL_M)
	    message(STATUS "setting unrollm=2")
      set(${float_char}GEMM_UNROLL_M 2)
    endif()
    if (NOT DEFINED ${float_char}GEMM_UNROLL_N)
	    message(STATUS "setting unrolln=2")
      set(${float_char}GEMM_UNROLL_N 2)
    endif()
  endforeach()
  file(APPEND ${TARGET_CONF_TEMP}
    "#define GEMM_MULTITHREAD_THRESHOLD\t${GEMM_MULTITHREAD_THRESHOLD}\n")
  # Move to where gen_config_h would place it
  file(MAKE_DIRECTORY ${TARGET_CONF_DIR})
  file(RENAME ${TARGET_CONF_TEMP} "${TARGET_CONF_DIR}/${TARGET_CONF}")

else(NOT CMAKE_CROSSCOMPILING)
  # compile getarch
  set(GETARCH_SRC
    ${PROJECT_SOURCE_DIR}/getarch.c
    ${CPUIDEMU}
  )

  if ("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    #Use generic for MSVC now
    message(STATUS "MSVC")
    set(GETARCH_FLAGS ${GETARCH_FLAGS} -DFORCE_GENERIC)
  else()
    if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin")
      list(APPEND GETARCH_SRC ${PROJECT_SOURCE_DIR}/cpuid.S)
    endif()
    if (DEFINED TARGET_CORE)
    set(GETARCH_FLAGS ${GETARCH_FLAGS} -DFORCE_${TARGET_CORE})
  endif ()
  endif ()

  if ("${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    # disable WindowsStore strict CRT checks
    set(GETARCH_FLAGS ${GETARCH_FLAGS} -D_CRT_SECURE_NO_WARNINGS)
  endif ()

  set(GETARCH_DIR "${PROJECT_BINARY_DIR}/getarch_build")
  set(GETARCH_BIN "getarch${CMAKE_EXECUTABLE_SUFFIX}")
  file(MAKE_DIRECTORY "${GETARCH_DIR}")
  configure_file("${TARGET_CONF_TEMP}" "${GETARCH_DIR}/${TARGET_CONF}" COPYONLY)
  if (NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    if (CMAKE_ASM_COMPILER_ID STREQUAL "")
      try_compile(GETARCH_RESULT "${GETARCH_DIR}"
        SOURCES ${GETARCH_SRC}
        CMAKE_FLAGS "-DCMAKE_ASM_COMPILER=${CMAKE_C_COMPILER}"
        COMPILE_DEFINITIONS ${EXFLAGS} ${GETARCH_FLAGS} -I"${GETARCH_DIR}" -I"${PROJECT_SOURCE_DIR}" -I"${PROJECT_BINARY_DIR}"
        OUTPUT_VARIABLE GETARCH_LOG
        COPY_FILE "${PROJECT_BINARY_DIR}/${GETARCH_BIN}"
      )
    else()
      try_compile(GETARCH_RESULT "${GETARCH_DIR}"
        SOURCES ${GETARCH_SRC}
        COMPILE_DEFINITIONS ${EXFLAGS} ${GETARCH_FLAGS} -I"${GETARCH_DIR}" -I"${PROJECT_SOURCE_DIR}" -I"${PROJECT_BINARY_DIR}"
        OUTPUT_VARIABLE GETARCH_LOG
        COPY_FILE "${PROJECT_BINARY_DIR}/${GETARCH_BIN}"
      )
    endif()
    if (NOT ${GETARCH_RESULT})
      MESSAGE(FATAL_ERROR "Compiling getarch failed ${GETARCH_LOG}")
    endif ()
  endif ()
  unset (HAVE_AVX2)
  unset (HAVE_AVX)
  unset (HAVE_FMA3)
  unset (HAVE_MMX)
  unset (HAVE_SSE)
  unset (HAVE_SSE2)
  unset (HAVE_SSE3)
  unset (HAVE_SSSE3)
  unset (HAVE_SSE4A)
  unset (HAVE_SSE4_1)
  unset (HAVE_SSE4_2)
  unset (HAVE_NEON)
  unset (HAVE_VFP)
  unset (HAVE_VFPV3)
  unset (HAVE_VFPV4)
  message(STATUS "Running getarch")

  # use the cmake binary w/ the -E param to run a shell command in a cross-platform way
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH_BIN}" 0 OUTPUT_VARIABLE GETARCH_MAKE_OUT)
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH_BIN}" 1 OUTPUT_VARIABLE GETARCH_CONF_OUT)

  message(STATUS "GETARCH results:\n${GETARCH_MAKE_OUT}")

  # append config data from getarch to the TARGET file and read in CMake vars
  file(APPEND "${TARGET_CONF_TEMP}" ${GETARCH_CONF_OUT})
  ParseGetArchVars(${GETARCH_MAKE_OUT})

  set(GETARCH2_DIR "${PROJECT_BINARY_DIR}/getarch2_build")
  set(GETARCH2_BIN "getarch_2nd${CMAKE_EXECUTABLE_SUFFIX}")
  file(MAKE_DIRECTORY "${GETARCH2_DIR}")
  configure_file("${TARGET_CONF_TEMP}" "${GETARCH2_DIR}/${TARGET_CONF}" COPYONLY)
  if (NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    try_compile(GETARCH2_RESULT "${GETARCH2_DIR}"
      SOURCES "${PROJECT_SOURCE_DIR}/getarch_2nd.c"
    COMPILE_DEFINITIONS ${EXFLAGS} ${GETARCH_FLAGS} ${GETARCH2_FLAGS} -I"${GETARCH2_DIR}" -I"${PROJECT_SOURCE_DIR}" -I"${PROJECT_BINARY_DIR}"
      OUTPUT_VARIABLE GETARCH2_LOG
      COPY_FILE "${PROJECT_BINARY_DIR}/${GETARCH2_BIN}"
    )

    if (NOT ${GETARCH2_RESULT})
      MESSAGE(FATAL_ERROR "Compiling getarch_2nd failed ${GETARCH2_LOG}")
    endif ()
  endif ()

  # use the cmake binary w/ the -E param to run a shell command in a cross-platform way
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH2_BIN}" 0 OUTPUT_VARIABLE GETARCH2_MAKE_OUT)
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH2_BIN}" 1 OUTPUT_VARIABLE GETARCH2_CONF_OUT)

  # append config data from getarch_2nd to the TARGET file and read in CMake vars
  file(APPEND "${TARGET_CONF_TEMP}" ${GETARCH2_CONF_OUT})

  configure_file("${TARGET_CONF_TEMP}" "${TARGET_CONF_DIR}/${TARGET_CONF}" COPYONLY)

  ParseGetArchVars(${GETARCH2_MAKE_OUT})

endif()
