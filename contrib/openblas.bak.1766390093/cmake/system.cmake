##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from OpenBLAS/Makefile.system
##
set(NETLIB_LAPACK_DIR "${PROJECT_SOURCE_DIR}/lapack-netlib")

# System detection, via CMake.
include("${PROJECT_SOURCE_DIR}/cmake/system_check.cmake")

if(CMAKE_CROSSCOMPILING AND NOT DEFINED TARGET)
  # Detect target without running getarch
  if (ARM64)
    set(TARGET "ARMV8")
  elseif(ARM)
    set(TARGET "ARMV7") # TODO: Ask compiler which arch this is
  else()
    message(FATAL_ERROR "When cross compiling, a TARGET is required.")
  endif()
endif()

# Other files expect CORE, which is actually TARGET and will become TARGET_CORE for kernel build. Confused yet?
# It seems we are meant to use TARGET as input and CORE internally as kernel.
if(NOT DEFINED CORE AND DEFINED TARGET)
  if (${TARGET} STREQUAL "LOONGSON3R5")
    set(CORE "LA464")
  elseif (${TARGET} STREQUAL "LOONGSON2K1000")
    set(CORE "LA264")
  elseif (${TARGET} STREQUAL "LOONGSONGENERIC")
    set(CORE "LA64_GENERIC)")
  else ()
    set(CORE ${TARGET})
  endif()
endif()

# TARGET_CORE will override TARGET which is used in DYNAMIC_ARCH=1.
if (DEFINED TARGET_CORE)
  set(TARGET ${TARGET_CORE})
endif ()

# Force fallbacks for 32bit
if (DEFINED BINARY AND DEFINED TARGET AND BINARY EQUAL 32)
  message(STATUS "Compiling a ${BINARY}-bit binary.")
  set(NO_AVX 1)
  if (${TARGET} STREQUAL "HASWELL" OR ${TARGET} STREQUAL "SANDYBRIDGE" OR ${TARGET} STREQUAL "SKYLAKEX" OR ${TARGET} STREQUAL "COOPERLAKE" OR ${TARGET} STREQUAL "SAPPHIRERAPIDS")
    set(TARGET "NEHALEM")
  endif ()
  if (${TARGET} STREQUAL "BULLDOZER" OR ${TARGET} STREQUAL "PILEDRIVER" OR ${TARGET} STREQUAL "ZEN")
    set(TARGET "BARCELONA")
  endif ()
  if (${TARGET} STREQUAL "ARMV8" OR ${TARGET} STREQUAL "CORTEXA57" OR ${TARGET} STREQUAL "CORTEXA53" OR ${TARGET} STREQUAL "CORTEXA55")
    set(TARGET "ARMV7")
  endif ()
  if (${TARGET} STREQUAL "POWER8" OR ${TARGET} STREQUAL "POWER9" OR ${TARGET} STREQUAL "POWER10")
    set(TARGET "POWER6")
  endif ()
endif ()


if (DEFINED TARGET)
  message(STATUS "-- -- -- -- -- -- -- -- -- -- -- -- --")
  message(STATUS "Targeting the ${TARGET} architecture.")
  set(GETARCH_FLAGS "-DFORCE_${TARGET}")
endif ()

# On x86_64 build getarch with march=native. This is required to detect AVX512 support in getarch.
if (X86_64 AND NOT (${CMAKE_C_COMPILER_ID} STREQUAL "PGI" OR ${CMAKE_C_COMPILER_ID} STREQUAL "NVHPC"))
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -march=native")
endif ()

# On x86 no AVX support is available
if (X86 OR X86_64)
if ((DEFINED BINARY AND BINARY EQUAL 32) OR ("$CMAKE_SIZEOF_VOID_P}" EQUAL "4"))
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -DNO_AVX -DNO_AVX2 -DNO_AVX512")
endif ()
endif ()

if (INTERFACE64)
  message(STATUS "Using 64-bit integers.")
  set(GETARCH_FLAGS	"${GETARCH_FLAGS} -DUSE64BITINT")
endif ()

if (NOT DEFINED GEMM_MULTITHREAD_THRESHOLD)
  set(GEMM_MULTITHREAD_THRESHOLD 4)
endif ()
message(STATUS "GEMM multithread threshold set to ${GEMM_MULTITHREAD_THRESHOLD}.")
set(GETARCH_FLAGS	"${GETARCH_FLAGS} -DGEMM_MULTITHREAD_THRESHOLD=${GEMM_MULTITHREAD_THRESHOLD}")

if (NO_AVX)
  message(STATUS "Disabling Advanced Vector Extensions (AVX).")
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -DNO_AVX")
endif ()

if (NO_AVX2)
  message(STATUS "Disabling Advanced Vector Extensions 2 (AVX2).")
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -DNO_AVX2")
endif ()

if (NO_AVX512)
  message(STATUS "Disabling Advanced Vector Extensions 512 (AVX512).")
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -DNO_AVX512")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(GETARCH_FLAGS "${GETARCH_FLAGS} ${CMAKE_C_FLAGS_DEBUG}")
endif ()

if (NOT DEFINED NO_PARALLEL_MAKE)
  set(NO_PARALLEL_MAKE 0)
endif ()
set(GETARCH_FLAGS	"${GETARCH_FLAGS} -DNO_PARALLEL_MAKE=${NO_PARALLEL_MAKE}")

if (CMAKE_C_COMPILER STREQUAL loongcc)
  set(GETARCH_FLAGS	"${GETARCH_FLAGS} -static")
endif ()

if (POWER)
  set(NO_WARMUP 1)
  set(HAVE_GAS 1)
  if (CMAKE_ASM_COMPILER_ID STREQUAL "GNU")
    set(HAVE_GAS 0)
  elseif (CMAKE_ASM_COMPILER_ID STREQUAL "Clang")
    set(CCOMMON_OPT "${CCOMMON_OPT} -fno-integrated-as")
    set(HAVE_GAS 0)
  endif ()
  set(GETARCH_FLAGS "${GETARCH_FLAGS} -DHAVE_GAS=${HAVE_GAS}")
endif ()

#if don't use Fortran, it will only compile CBLAS.
if (ONLY_CBLAS)
  set(NO_LAPACK 1)
else ()
  set(ONLY_CBLAS 0)
endif ()

# N.B. this is NUM_THREAD in Makefile.system which is probably a bug -hpa
if (NOT CMAKE_CROSSCOMPILING)
  if (NOT DEFINED NUM_CORES)
    include(ProcessorCount)
    ProcessorCount(NUM_CORES)
  endif()

endif()

if (NOT DEFINED NUM_PARALLEL)
  set(NUM_PARALLEL 1)
endif()

if (NOT DEFINED NUM_THREADS)
  if (DEFINED NUM_CORES AND NOT NUM_CORES EQUAL 0)
    # HT?
    set(NUM_THREADS ${NUM_CORES})
  else ()
    set(NUM_THREADS 0)
  endif ()
endif()

if (${NUM_THREADS} LESS 2)
  set(USE_THREAD 0)
elseif(NOT DEFINED USE_THREAD)
  set(USE_THREAD 1)
endif ()

if (USE_THREAD)
  message(STATUS "Multi-threading enabled with ${NUM_THREADS} threads.")
else()
  if (${USE_LOCKING})
    set(CCOMMON_OPT "${CCOMMON_OPT} -DUSE_LOCKING")
  endif ()
endif ()

if (C_LAPACK)
  if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
    set(CCOMMON_OPT "${CCOMMON_OPT} -Wno-error=incompatible-pointer-types")
  endif ()
endif ()

include("${PROJECT_SOURCE_DIR}/cmake/prebuild.cmake")
if (DEFINED TARGET)
  if (${TARGET} STREQUAL COOPERLAKE AND NOT NO_AVX512)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
        if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 10.09)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=cooperlake")
        else()
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512")
        endif()
    elseif (${CMAKE_C_COMPILER_ID} STREQUAL "Clang" OR ${CMAKE_C_COMPILER_ID} STREQUAL "AppleClang")
         if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 8.99)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=cooperlake -mllvm -exhaustive-register-search")
        else()
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512 -mllvm -exhaustive-register-search")
        endif()
    endif()    
  endif()
  if (${TARGET} STREQUAL SAPPHIRERAPIDS AND NOT NO_AVX512)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
        if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 11.0)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=sapphirerapids")
        else()
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512")
        endif()
    elseif (${CMAKE_C_COMPILER_ID} STREQUAL "Clang" OR ${CMAKE_C_COMPILER_ID} STREQUAL "AppleClang")
         if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 12.0)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=sapphirerapids -mllvm -exhaustive-register-search")
        else()
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512 -mllvm -exhaustive-register-search")
        endif()
    endif()    
  endif()
  if (${TARGET} STREQUAL SKYLAKEX AND NOT NO_AVX512)
    	  set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512")
  	  if (${CMAKE_C_COMPILER_ID} STREQUAL "Clang" OR ${CMAKE_C_COMPILER_ID} STREQUAL "AppleClang")
	 	set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mllvm -exhaustive-register-search")
  	  endif()
  endif()
  
  if (((${TARGET} STREQUAL ZEN) AND HAVE_AVX512VL) AND NOT NO_AVX512)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
        if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 12.99)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=znver4")
	else()
    	  set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512")
        endif()
    elseif (${CMAKE_C_COMPILER_ID} STREQUAL "Clang" OR ${CMAKE_C_COMPILER_ID} STREQUAL "AppleClang")
         if (${CMAKE_C_COMPILER_VERSION} VERSION_GREATER 15.99)
          set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=znver4")
	else()
    	  set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -march=skylake-avx512")
        endif()
	set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mllvm -exhaustive-register-search")
    endif()
  endif()
  
  if ((${TARGET} STREQUAL HASWELL OR (${TARGET} STREQUAL ZEN AND NOT HAVE_AVX512VL)) AND NOT NO_AVX2)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
	    if (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.7 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 4.7)
        set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mavx2")
      endif()
    elseif (${CMAKE_C_COMPILER_ID} STREQUAL "CLANG")
      set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mavx2 -mfma")
    endif()
  endif()
  if (DEFINED HAVE_AVX)
	if (NOT NO_AVX)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mavx")
	endif()
  endif()
  if (DEFINED HAVE_AVX2)
	if (NOT NO_AVX2)
      	  set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mavx2")
	endif()
  endif()
  #  if (DEFINED HAVE_FMA3)
  #	if (NOT NO_AVX2)
  #  set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mfma")
  #	endif()
  #  endif()
    if (DEFINED HAVE_SSE)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -msse")
  endif()
  if (DEFINED HAVE_SSE2)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -msse2")
  endif()
  if (DEFINED HAVE_SSE3)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -msse3")
  endif()
    if (DEFINED HAVE_SSSE3)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -mssse3")
  endif()
    if (DEFINED HAVE_SSE4_1)
    set (KERNEL_DEFINITIONS "${KERNEL_DEFINITIONS} -msse4.1")
  endif()

  if (${TARGET} STREQUAL POWER10)
    if (CMAKE_C_COMPILER_VERSION VERSION_GREATER 10.2 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 10.2)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -mcpu=power10 -mtune=power10 -mvsx -fno-fast-math")
    else ()
	    message(FATAL_ERROR "Compiler GCC ${CMAKE_C_COMPILER_VERSION} does not support Power10.")
    endif()
  endif()
  if (${TARGET} STREQUAL POWER9)
    if (CMAKE_C_COMPILER_VERSION VERSION_GREATER 5.0 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 5.0)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -mcpu=power9 -mtune=power9 -mvsx -fno-fast-math")
    else ()
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -mcpu=power8 -mtune=power8 -mvsx -fno-fast-math")
      message(WARNING "Compiler GCC ${CMAKE_C_COMPILER_VERSION} does not support fully Power9.")
    endif()
  endif()
  if (${TARGET} STREQUAL POWER8)
    set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -mcpu=power8 -mtune=power8 -mvsx -fno-fast-math")
  endif()

if (${TARGET} STREQUAL NEOVERSEV1)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "PGI" AND NOT NO_SVE)
	set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -Msve_intrinsics -march=armv8.4-a+sve -mtune=neoverse-v1")
    else ()
    if (CMAKE_C_COMPILER_VERSION VERSION_GREATER 10.4 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 10.4)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -march=armv8.4-a+sve -mtune=neoverse-v1")
    else ()
	    message(FATAL_ERROR "Compiler ${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_VERSION} does not support Neoverse V1.")
    endif()
    endif()
  endif()
  if (${TARGET} STREQUAL NEOVERSEN2)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "PGI" AND NOT NO_SVE)
	set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -Msve-intrinsics -march=armv8.5-a+sve+sve2+bf16 -mtune=neoverse-n2")
    else ()
    if (CMAKE_C_COMPILER_VERSION VERSION_GREATER 10.4 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 10.4)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -march=armv8.5-a+sve+sve2+bf16 -mtune=neoverse-n2")
    else ()
	    message(FATAL_ERROR "Compiler $${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_VERSION} does not support Neoverse N2.")
    endif()
    endif()
  endif()
  if (${TARGET} STREQUAL ARMV8SVE)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "PGI" AND NOT NO_SVE)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -Msve-intrinsics -march=armv8.2-a+sve")
    else ()
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -march=armv8.2-a+sve")
    endif()
  endif()
  if (${TARGET} STREQUAL ARMV9SME)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -march=armv9-a+sme -O3")
  endif()
  if (${TARGET} STREQUAL A64FX)
    if (${CMAKE_C_COMPILER_ID} STREQUAL "PGI" AND NOT NO_SVE)
      set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -Msve-intrinsics -march=armv8.2-a+sve -mtune=a64fx")
    else ()
      execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
      if (${GCC_VERSION} VERSION_GREATER 10.4 OR ${GCC_VERSION} VERSION_EQUAL 10.4)
        set (KERNEL_DEFINITIONS  "${KERNEL_DEFINITIONS} -march=armv8.2-a+sve -mtune=a64fx")
      else ()
        message(FATAL_ERROR "Compiler $${CMAKE_C_COMPILER} {GCC_VERSION} does not support A64FX.")
      endif()
    endif()
  endif()

endif()

if (DEFINED BINARY)
  message(STATUS "Compiling a ${BINARY}-bit binary.")
endif ()
if (NOT DEFINED NEED_PIC)
  set(NEED_PIC 1)
endif ()

# OS dependent settings
include("${PROJECT_SOURCE_DIR}/cmake/os.cmake")

# Architecture dependent settings
include("${PROJECT_SOURCE_DIR}/cmake/arch.cmake")

# C Compiler dependent settings
include("${PROJECT_SOURCE_DIR}/cmake/cc.cmake")

if (INTERFACE64)
  set(SUFFIX64 64)
  set(SUFFIX64_UNDERSCORE _64)
endif()

if (NOT NOFORTRAN)
  # Fortran Compiler dependent settings
  include("${PROJECT_SOURCE_DIR}/cmake/fc.cmake")
else ()
  if (NOT XXXX)
    set(C_LAPACK 1)
    if (INTERFACE64)
      set (CCOMMON_OPT "${CCOMMON_OPT} -DLAPACK_ILP64")
    endif ()
    set(TIMER "NONE")
  else ()
    set (NO_LAPACK 1)
  endif ()
endif ()

if (USE_OPENMP)
  find_package(OpenMP COMPONENTS C REQUIRED)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DUSE_OPENMP")
  if (NOT NOFORTRAN)
    find_package(OpenMP COMPONENTS Fortran REQUIRED)
    # Avoid mixed OpenMP linkage
    get_target_property(OMP_C_LIB OpenMP::OpenMP_C INTERFACE_LINK_LIBRARIES)
    get_target_property(OMP_Fortran_LIB OpenMP::OpenMP_Fortran INTERFACE_LINK_LIBRARIES)
    if (NOT OMP_C_LIB STREQUAL OMP_Fortran_LIB)
      message(FATAL_ERROR "Multiple OpenMP runtime libraries detected. Mixed OpenMP runtime linkage is dangerous. You may pass -DOpenMP_LANG_LIB_NAMES and -DOpenMP_omp_LIBRARY to manually choose the OpenMP library.")
    endif()
  endif ()
endif ()

if (BINARY64)
  if (INTERFACE64)
    # CCOMMON_OPT += -DUSE64BITINT
  endif ()
endif ()

if(EMBEDDED)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DOS_EMBEDDED")
  set(CCOMMON_OPT "${CCOMMON_OPT} -mthumb -mcpu=cortex-m4 -mfloat-abi=hard -mfpu=fpv4-sp-d16")
endif()

if (NEED_PIC)
  if (${CMAKE_C_COMPILER} STREQUAL "IBM")
    set(CCOMMON_OPT "${CCOMMON_OPT} -qpic=large")
  else ()
    set(CCOMMON_OPT "${CCOMMON_OPT} -fPIC")
  endif ()

  if (NOT NOFORTRAN)
    if (${F_COMPILER} STREQUAL "SUN")
      set(FCOMMON_OPT "${FCOMMON_OPT} -pic")
    elseif (${F_COMPILER} STREQUAL "NAGFOR")
      set(FCOMMON_OPT "${FCOMMON_OPT} -PIC")
    else ()
      set(FCOMMON_OPT "${FCOMMON_OPT} -fPIC")
    endif ()
  endif()
endif ()

if (X86_64 OR ${CORE} STREQUAL POWER10 OR ARM64 OR LOONGARCH64)
  set(SMALL_MATRIX_OPT TRUE)
endif ()
if (ARM64)
  set(GEMM_GEMV_FORWARD TRUE)
endif ()

if (GEMM_GEMV_FORWARD AND NOT ONLY_CBLAS)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DGEMM_GEMV_FORWARD")
endif ()
if (GEMM_GEMV_FORWARD_BF16 AND NOT ONLY_CBLAS)
    set(CCOMMON_OPT "${CCOMMON_OPT} -DGEMM_GEMV_FORWARD_BF16")
endif ()
if (SMALL_MATRIX_OPT)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DSMALL_MATRIX_OPT")
endif ()

if (DYNAMIC_ARCH)
  if (X86 OR X86_64 OR ARM64 OR POWER OR RISCV64 OR LOONGARCH64)
    set(CCOMMON_OPT "${CCOMMON_OPT} -DDYNAMIC_ARCH")
    if (DYNAMIC_OLDER)
      set(CCOMMON_OPT "${CCOMMON_OPT} -DDYNAMIC_OLDER")
    endif ()
  else ()
    unset (DYNAMIC_ARCH)
    message (STATUS "DYNAMIC_ARCH is not supported on the target architecture, removing")
  endif ()
endif ()

if (DYNAMIC_LIST)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DDYNAMIC_LIST")
  foreach(DCORE ${DYNAMIC_LIST})
    set(CCOMMON_OPT "${CCOMMON_OPT} -DDYN_${DCORE}")
  endforeach ()
endif ()

if (NO_LAPACK)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_LAPACK")
  #Disable LAPACK C interface
  set(NO_LAPACKE 1)
endif ()

if (NO_LAPACKE)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_LAPACKE")
endif ()

if (NO_AVX)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_AVX")
endif ()

if (X86)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_AVX")
endif ()

if (NO_AVX2)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_AVX2")
endif ()

if (NO_AVX512)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_AVX512")
endif ()

if (USE_THREAD)
  # USE_SIMPLE_THREADED_LEVEL3 = 1
  # NO_AFFINITY = 1
  set(CCOMMON_OPT "${CCOMMON_OPT} -DSMP_SERVER")

  if (MIPS64)
    if (NOT ${CORE} STREQUAL "LOONGSON3B")
      set(USE_SIMPLE_THREADED_LEVEL3 1)
    endif ()
  endif ()

  if (BIGNUMA)
    set(CCOMMON_OPT "${CCOMMON_OPT} -DBIGNUMA")
  endif ()
endif ()

if (NO_WARMUP)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_WARMUP")
endif ()

if (CONSISTENT_FPCSR)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DCONSISTENT_FPCSR")
endif ()

if (USE_TLS)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DUSE_TLS")
endif ()

# Only for development
# set(CCOMMON_OPT "${CCOMMON_OPT} -DPARAMTEST")
# set(CCOMMON_OPT "${CCOMMON_OPT} -DPREFETCHTEST")
# set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_SWITCHING")
# set(USE_PAPI 1)

if (USE_PAPI)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DUSE_PAPI")
  set(EXTRALIB "${EXTRALIB} -lpapi -lperfctr")
endif ()

if (DYNAMIC_THREADS)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DDYNAMIC_THREADS")
endif ()

set(CCOMMON_OPT "${CCOMMON_OPT} -DMAX_CPU_NUMBER=${NUM_THREADS}")

set(CCOMMON_OPT "${CCOMMON_OPT} -DMAX_PARALLEL_NUMBER=${NUM_PARALLEL}")

if (BUFFERSIZE)
set(CCOMMON_OPT "${CCOMMON_OPT} -DBUFFERSIZE=${BUFFERSIZE}")
endif ()

if (USE_SIMPLE_THREADED_LEVEL3)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DUSE_SIMPLE_THREADED_LEVEL3")
endif ()

if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
if (DEFINED MAX_STACK_ALLOC)
if (NOT ${MAX_STACK_ALLOC} EQUAL 0)
set(CCOMMON_OPT "${CCOMMON_OPT} -DMAX_STACK_ALLOC=${MAX_STACK_ALLOC}")
endif ()
else ()
set(CCOMMON_OPT "${CCOMMON_OPT} -DMAX_STACK_ALLOC=2048")
endif ()
endif ()
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
if (DEFINED BLAS3_MEM_ALLOC_THRESHOLD)
if (NOT ${BLAS3_MEM_ALLOC_THRESHOLD} EQUAL 32)
set(CCOMMON_OPT "${CCOMMON_OPT} -DBLAS3_MEM_ALLOC_THRESHOLD=${BLAS3_MEM_ALLOC_THRESHOLD}")
endif()
endif()
endif()
  
set(LIBPREFIX "lib${LIBNAMEPREFIX}openblas")

if (DEFINED LIBNAMESUFFIX)
  set(LIBPREFIX "${LIBNAMEPREFIX}_${LIBNAMESUFFIX}")
endif ()

if (NOT DEFINED SYMBOLPREFIX)
  set(SYMBOLPREFIX "")
endif ()

if (NOT DEFINED SYMBOLSUFFIX)
  set(SYMBOLSUFFIX "")
endif ()

set(KERNELDIR	"${PROJECT_SOURCE_DIR}/kernel/${ARCH}")

# TODO: need to convert these Makefiles
# include ${PROJECT_SOURCE_DIR}/cmake/${ARCH}.cmake

if (${CORE} STREQUAL "PPC440")
  set(CCOMMON_OPT "${CCOMMON_OPT} -DALLOC_QALLOC")
endif ()

if (${CORE} STREQUAL "PPC440FP2")
  set(STATIC_ALLOCATION 1)
endif ()

if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  set(NO_AFFINITY 1)
endif ()

if (NOT X86_64 AND NOT X86 AND NOT ${CORE} STREQUAL "LOONGSON3B")
  set(NO_AFFINITY 1)
endif ()

if (NO_AFFINITY)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DNO_AFFINITY")
endif ()

if (FUNCTION_PROFILE)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DFUNCTION_PROFILE")
endif ()

if (HUGETLB_ALLOCATION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DALLOC_HUGETLB")
endif ()

if (DEFINED HUGETLBFILE_ALLOCATION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DALLOC_HUGETLBFILE -DHUGETLB_FILE_NAME=${HUGETLBFILE_ALLOCATION})")
endif ()

if (STATIC_ALLOCATION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DALLOC_STATIC")
endif ()

if (DEVICEDRIVER_ALLOCATION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DALLOC_DEVICEDRIVER -DDEVICEDRIVER_NAME=\"/dev/mapper\"")
endif ()

if (MIXED_MEMORY_ALLOCATION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DMIXED_MEMORY_ALLOCATION")
endif ()

set(CCOMMON_OPT "${CCOMMON_OPT} -DVERSION=\"\\\"${OpenBLAS_VERSION}\\\"\"")

set(REVISION "-r${OpenBLAS_VERSION}")
set(MAJOR_VERSION ${OpenBLAS_MAJOR_VERSION})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CCOMMON_OPT}")

if (NOT BUILD_SINGLE AND NOT BUILD_DOUBLE AND NOT BUILD_COMPLEX AND NOT BUILD_COMPLEX16)
	set (BUILD_SINGLE ON)
	set (BUILD_DOUBLE ON)
	set (BUILD_COMPLEX ON)
	set (BUILD_COMPLEX16 ON)
endif()
if (BUILD_SINGLE)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBUILD_SINGLE")
endif()
if (BUILD_DOUBLE)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBUILD_DOUBLE")
endif()
if (BUILD_COMPLEX)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBUILD_COMPLEX")
endif()
if (BUILD_COMPLEX16)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBUILD_COMPLEX16")
endif()
if (BUILD_BFLOAT16)
       set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBUILD_BFLOAT16")
endif()
if(NOT MSVC)
set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} ${CCOMMON_OPT}")
endif()
# TODO: not sure what PFLAGS is -hpa
set(PFLAGS "${PFLAGS} ${CCOMMON_OPT} -I${TOPDIR} -DPROFILE ${COMMON_PROF}")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")

if ("${F_COMPILER}" STREQUAL "FLANG")
if (${CMAKE_Fortran_COMPILER_VERSION} VERSION_LESS_EQUAL 3)
  set(CMAKE_Fortran_FLAGS_RELEASE "${CMAKE_Fortran_FLAGS_RELEASE} -fno-unroll-loops")
endif ()
endif ()
if (ARM64 AND CMAKE_Fortran_COMPILER_ID MATCHES "LLVMFlang.*" AND CMAKE_SYSTEM_NAME STREQUAL "Windows")
  set(CMAKE_Fortran_FLAGS_RELEASE "${CMAKE_Fortran_FLAGS_RELEASE} -O2")
endif ()
endif ()


set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} ${FCOMMON_OPT}")
# TODO: not sure what FPFLAGS is -hpa
set(FPFLAGS "${FPFLAGS} ${FCOMMON_OPT} ${COMMON_PROF}")

#For LAPACK Fortran codes.
set(LAPACK_FFLAGS "${LAPACK_FFLAGS} ${CMAKE_Fortran_FLAGS}" )
if (LAPACK_STRLEN)
	set (LAPACK_FFLAGS "${LAPACK_FFLAGS} -DLAPACK_STRLEN=${LAPACK_STRLEN}")
endif()
set(LAPACK_FPFLAGS "${LAPACK_FPFLAGS} ${FPFLAGS}")

if (CMAKE_Fortran_COMPILER)
  if ("${F_COMPILER}" STREQUAL "NAGFOR" OR "${F_COMPILER}" STREQUAL "CRAY" OR CMAKE_Fortran_COMPILER_ID MATCHES "LLVMFlang.*")
    set(FILTER_FLAGS "-msse3;-mssse3;-msse4.1;-mavx;-mavx2,-mskylake-avx512")
    if (CMAKE_Fortran_COMPILER_ID MATCHES "LLVMFlang.*")
      message(STATUS "removing fortran flags not supported by the compiler")
      set(FILTER_FLAGS "${FILTER_FLAGS};-m32;-m64")
    endif ()
    foreach (FILTER_FLAG ${FILTER_FLAGS})
      string(REPLACE ${FILTER_FLAG} "" LAPACK_FFLAGS ${LAPACK_FFLAGS})
      string(REPLACE ${FILTER_FLAG} "" LAPACK_FPFLAGS ${LAPACK_FPFLAGS})
    endforeach ()
  endif ()
endif ()

if ("${F_COMPILER}" STREQUAL "GFORTRAN")
  # lapack-netlib is rife with uninitialized warnings -hpa
  set(LAPACK_FFLAGS "${LAPACK_FFLAGS} -Wno-maybe-uninitialized")
endif ()

set(LAPACK_CFLAGS "${CMAKE_C_CFLAGS} -DHAVE_LAPACK_CONFIG_H")
if (INTERFACE64)
  set(LAPACK_CFLAGS "${LAPACK_CFLAGS} -DLAPACK_ILP64")
endif ()

if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(LAPACK_CFLAGS "${LAPACK_CFLAGS} -DOPENBLAS_OS_WINDOWS")
endif ()

if (${CMAKE_C_COMPILER} STREQUAL "LSB" OR ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(LAPACK_CFLAGS "${LAPACK_CFLAGS} -DLAPACK_COMPLEX_STRUCTURE")
endif ()
if (${CMAKE_C_COMPILER_ID} MATCHES "IntelLLVM" AND ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
	set(LAPACK_CFLAGS "${LAPACK_CFLAGS} -DNOCHANGE")
endif ()


if (NOT DEFINED SUFFIX)
  set(SUFFIX o)
endif ()

if (NOT DEFINED PSUFFIX)
  set(PSUFFIX po)
endif ()

if (NOT DEFINED LIBSUFFIX)
  set(LIBSUFFIX a)
endif ()

if (DYNAMIC_ARCH)
  if (USE_THREAD)
    set(LIBNAME "${LIBPREFIX}p${REVISION}.${LIBSUFFIX}")
    set(LIBNAME_P	"${LIBPREFIX}p${REVISION}_p.${LIBSUFFIX}")
  else ()
    set(LIBNAME "${LIBPREFIX}${REVISION}.${LIBSUFFIX}")
    set(LIBNAME_P	"${LIBPREFIX}${REVISION}_p.${LIBSUFFIX}")
  endif ()
else ()
  if (USE_THREAD)
    set(LIBNAME "${LIBPREFIX}_${LIBCORE}p${REVISION}.${LIBSUFFIX}")
    set(LIBNAME_P	"${LIBPREFIX}_${LIBCORE}p${REVISION}_p.${LIBSUFFIX}")
  else ()
    set(LIBNAME	"${LIBPREFIX}_${LIBCORE}${REVISION}.${LIBSUFFIX}")
    set(LIBNAME_P	"${LIBPREFIX}_${LIBCORE}${REVISION}_p.${LIBSUFFIX}")
  endif ()
endif ()

if (DEFINED FIXED_LIBNAME)
  set (LIBNAME "${LIBPREFIX}.${LIBSUFFIX}")
  set (LIBNAME "${LIBPREFIX}_p.${LIBSUFFIX}")
endif()

set(LIBDLLNAME "${LIBPREFIX}.dll")
set(LIBSONAME "${LIBNAME}.${LIBSUFFIX}.so")
set(LIBDYNNAME "${LIBNAME}.${LIBSUFFIX}.dylib")
set(LIBDEFNAME "${LIBNAME}.${LIBSUFFIX}.def")
set(LIBEXPNAME "${LIBNAME}.${LIBSUFFIX}.exp")
set(LIBZIPNAME "${LIBNAME}.${LIBSUFFIX}.zip")

set(LIBS "${PROJECT_SOURCE_DIR}/${LIBNAME}")
set(LIBS_P "${PROJECT_SOURCE_DIR}/${LIBNAME_P}")


set(LIB_COMPONENTS BLAS)
if (NOT NO_CBLAS)
  set(LIB_COMPONENTS "${LIB_COMPONENTS} CBLAS")
endif ()

if (NOT NO_LAPACK)
  set(LIB_COMPONENTS "${LIB_COMPONENTS} LAPACK")
  if (NOT NO_LAPACKE)
    set(LIB_COMPONENTS "${LIB_COMPONENTS} LAPACKE")
  endif ()
  if (BUILD_RELAPACK)
    set(LIB_COMPONENTS "${LIB_COMPONENTS} ReLAPACK")
  endif ()
endif ()

if (ONLY_CBLAS)
  set(LIB_COMPONENTS CBLAS)
endif ()


# For GEMM3M
set(USE_GEMM3M 0)

if (DEFINED ARCH)
  if (X86 OR X86_64 OR ${ARCH} STREQUAL "ia64" OR MIPS64)
    set(USE_GEMM3M 1)
  endif ()

  if (${CORE} STREQUAL "generic")
    set(USE_GEMM3M 0)
  endif ()
endif ()


#export OSNAME
#export ARCH
#export CORE
#export LIBCORE
#export PGCPATH
#export CONFIG
#export CC
#export FC
#export BU
#export FU
#export NEED2UNDERSCORES
#export USE_THREAD
#export NUM_THREADS
#export NUM_CORES
#export SMP
#export MAKEFILE_RULE
#export NEED_PIC
#export BINARY
#export BINARY32
#export BINARY64
#export F_COMPILER
#export C_COMPILER
#export USE_OPENMP
#export CROSS
#export CROSS_SUFFIX
#export NOFORTRAN
#export NO_FBLAS
#export EXTRALIB
#export CEXTRALIB
#export FEXTRALIB
#export HAVE_SSE
#export HAVE_SSE2
#export HAVE_SSE3
#export HAVE_SSSE3
#export HAVE_SSE4_1
#export HAVE_SSE4_2
#export HAVE_SSE4A
#export HAVE_SSE5
#export HAVE_AVX
#export HAVE_VFP
#export HAVE_VFPV3
#export HAVE_VFPV4
#export HAVE_NEON
#export KERNELDIR
#export FUNCTION_PROFILE
#export TARGET_CORE
#
#export SBGEMM_UNROLL_M
#export SBGEMM_UNROLL_N
#export SGEMM_UNROLL_M
#export SGEMM_UNROLL_N
#export DGEMM_UNROLL_M
#export DGEMM_UNROLL_N
#export QGEMM_UNROLL_M
#export QGEMM_UNROLL_N
#export CGEMM_UNROLL_M
#export CGEMM_UNROLL_N
#export ZGEMM_UNROLL_M
#export ZGEMM_UNROLL_N
#export XGEMM_UNROLL_M
#export XGEMM_UNROLL_N
#export CGEMM3M_UNROLL_M
#export CGEMM3M_UNROLL_N
#export ZGEMM3M_UNROLL_M
#export ZGEMM3M_UNROLL_N
#export XGEMM3M_UNROLL_M
#export XGEMM3M_UNROLL_N


#if (USE_CUDA)
#  export CUDADIR
#  export CUCC
#  export CUFLAGS
#  export CULIB
#endif
