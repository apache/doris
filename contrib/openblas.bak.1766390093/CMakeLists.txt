##
## Author: Hank Anderson <hank@statease.com>
##

cmake_minimum_required(VERSION 3.16.0)

set (CMAKE_ASM_SOURCE_FILE_EXTENSIONS "S")
project(OpenBLAS C ASM)

set(OpenBLAS_MAJOR_VERSION 0)
set(OpenBLAS_MINOR_VERSION 3)
set(OpenBLAS_PATCH_VERSION 30.dev)

set(OpenBLAS_VERSION "${OpenBLAS_MAJOR_VERSION}.${OpenBLAS_MINOR_VERSION}.${OpenBLAS_PATCH_VERSION}")

# A known problem of OpenBLAS.
# It can not be built with -O0 optimization level.
# https://stackoverflow.com/questions/63023087/inline-asm-fails-to-compile-without-optimization?utm_source=chatgpt.com
add_compile_options(-Wno-everything -O3)

# Adhere to GNU filesystem layout conventions
include(GNUInstallDirs)

include(CMakePackageConfigHelpers)

#######
option(BUILD_WITHOUT_LAPACK "Do not build LAPACK and LAPACKE (Only BLAS or CBLAS)" OFF)

option(BUILD_WITHOUT_LAPACKE "Do not build the C interface to LAPACK)" OFF)

option(BUILD_LAPACK_DEPRECATED "When building LAPACK, include also some older, deprecated routines" ON)

set(LAPACK_STRLEN "" CACHE STRING "When building LAPACK, use this type (e.g. \"int\") for character lengths (defaults to size_t)")

option(BUILD_TESTING "Build LAPACK testsuite when building LAPACK" ON)

option(BUILD_BENCHMARKS "Build the collection of BLAS/LAPACK benchmarks" OFF)

option(C_LAPACK "Build LAPACK from C sources instead of the original Fortran" OFF)

option(BUILD_WITHOUT_CBLAS "Do not build the C interface (CBLAS) to the BLAS functions" OFF)

option(DYNAMIC_ARCH "Include support for multiple CPU targets, with automatic selection at runtime (x86/x86_64, aarch64, ppc or RISCV64-RVV1.0 only)" OFF)

option(DYNAMIC_OLDER "Include specific support for older x86 cpu models (Penryn,Dunnington,Atom,Nano,Opteron) with DYNAMIC_ARCH" OFF)

option(BUILD_RELAPACK "Build with ReLAPACK (recursive implementation of several LAPACK functions on top of standard LAPACK)" OFF)

option(USE_LOCKING "Use locks even in single-threaded builds to make them callable from multiple threads" OFF)

option(USE_PERL "Use the older PERL scripts for build preparation instead of universal shell scripts" OFF)

option(NO_WARMUP "Do not run a benchmark on each startup just to find the best location for the memory buffer" ON)

option(FIXED_LIBNAME "Use a non-versioned name for the library and no symbolic linking to variant names" OFF)

set(LIBNAMEPREFIX "" CACHE STRING  "Add a prefix to the openblas part of the library name" )
set(LIBNAMESUFFIX "" CACHE STRING  "Add a suffix after the openblas part of the library name" )

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  option(NO_AFFINITY "Disable support for CPU affinity masks to avoid binding processes from e.g. R or numpy/scipy to a single core" ON)
else()
  set(NO_AFFINITY 1)
endif()

option(CPP_THREAD_SAFETY_TEST "Run a massively parallel DGEMM test to confirm thread safety of the library (requires OpenMP and about 1.3GB of RAM)" OFF)

option(CPP_THREAD_SAFETY_GEMV "Run a massively parallel DGEMV test to confirm thread safety of the library (requires OpenMP)" OFF)
option(BUILD_STATIC_LIBS "Build static library" OFF)
option(BUILD_SHARED_LIBS "Build shared library" OFF)
if(NOT BUILD_STATIC_LIBS AND NOT BUILD_SHARED_LIBS)
  set(BUILD_STATIC_LIBS ON CACHE BOOL "Build static library" FORCE)
endif()
if((BUILD_STATIC_LIBS AND BUILD_SHARED_LIBS) AND MSVC)
  message(WARNING "Could not enable both BUILD_STATIC_LIBS and BUILD_SHARED_LIBS with MSVC, Disable BUILD_SHARED_LIBS")
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static library" FORCE)
endif()

# Add a prefix or suffix to all exported symbol names in the shared library.
# Avoids conflicts with other BLAS libraries, especially when using
# 64 bit integer interfaces in OpenBLAS.
set(SYMBOLPREFIX "" CACHE STRING  "Add a prefix to all exported symbol names in the shared library to avoid conflicts with other BLAS libraries" )

set(SYMBOLSUFFIX "" CACHE STRING  "Add a suffix to all exported symbol names in the shared library, e.g. _64 for INTERFACE64 builds" )

if (CMAKE_SYSTEM_NAME MATCHES "Windows" AND BUILD_SHARED_LIBS AND NOT ("${SYMBOLPREFIX}${SYMBOLSUFFIX}" STREQUAL ""))
set (DELETE_STATIC_LIBS "")
if (NOT BUILD_STATIC_LIBS)
	message (STATUS "forcing build of a temporary static library for symbol renaming")
	set (BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared library" FORCE)
	set (BUILD_STATIC_LIBS ON CACHE BOOL "Build static library" FORCE)
	set (DELETE_STATIC_LIBS  file (REMOVE $<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.lib))
endif ()
endif()


#######
if(BUILD_WITHOUT_LAPACK)
  set(NO_LAPACK 1)
  set(NO_LAPACKE 1)
endif()

if (BUILD_WITHOUT_LAPACKE)
  set(NO_LAPACKE 1)
endif()

if(BUILD_WITHOUT_CBLAS)
  set(NO_CBLAS 1)
endif()

#######

if(MSVC AND MSVC_STATIC_CRT)
  set(CompilerFlags
        CMAKE_CXX_FLAGS
        CMAKE_CXX_FLAGS_DEBUG
        CMAKE_CXX_FLAGS_RELEASE
        CMAKE_C_FLAGS
        CMAKE_C_FLAGS_DEBUG
        CMAKE_C_FLAGS_RELEASE
        )
  foreach(CompilerFlag ${CompilerFlags})
    string(REPLACE "/MD" "/MT" ${CompilerFlag} "${${CompilerFlag}}")
  endforeach()
endif()

message(WARNING "CMake support is experimental. It does not yet support all build options and may not produce the same Makefiles that OpenBLAS ships with.")

include("${PROJECT_SOURCE_DIR}/cmake/utils.cmake")
include("${PROJECT_SOURCE_DIR}/cmake/system.cmake")

string(FIND "${LIBNAMESUFFIX}" "${SUFFIX64_UNDERSCORE}" HAVE64)
if (${HAVE64} GREATER -1)
	set(OpenBLAS_LIBNAME ${LIBNAMEPREFIX}openblas${LIBNAMESUFFIX})
else ()
	set(OpenBLAS_LIBNAME ${LIBNAMEPREFIX}openblas${LIBNAMESUFFIX}${SUFFIX64_UNDERSCORE})
endif ()

set(BLASDIRS interface driver/level2 driver/level3 driver/others)

if (NOT DYNAMIC_ARCH)
  list(APPEND BLASDIRS kernel)
endif ()

if (DEFINED SANITY_CHECK)
  list(APPEND BLASDIRS reference)
endif ()

set(SUBDIRS	${BLASDIRS})
if (NOT NO_LAPACK)
  if(BUILD_RELAPACK)
    list(APPEND SUBDIRS relapack/src)
  endif()
  list(APPEND SUBDIRS lapack)
endif ()

if (NOT DEFINED BUILD_BFLOAT16)
 set (BUILD_BFLOAT16 false)
endif ()
# set which float types we want to build for
if (NOT DEFINED BUILD_SINGLE AND NOT DEFINED BUILD_DOUBLE AND NOT DEFINED BUILD_COMPLEX AND NOT DEFINED BUILD_COMPLEX16)
  # if none are defined, build for all
  # set(BUILD_BFLOAT16 true)
  set(BUILD_SINGLE true)
  set(BUILD_DOUBLE true)
  set(BUILD_COMPLEX true)
  set(BUILD_COMPLEX16 true)
endif ()

if (NOT DEFINED BUILD_MATGEN)
  set(BUILD_MATGEN true)
endif()

set(FLOAT_TYPES "")
if (BUILD_SINGLE)
  message(STATUS "Building Single Precision")
  list(APPEND FLOAT_TYPES "SINGLE") # defines nothing
endif ()

if (BUILD_DOUBLE)
  message(STATUS "Building Double Precision")
  list(APPEND FLOAT_TYPES "DOUBLE") # defines DOUBLE
endif ()

if (BUILD_COMPLEX)
  message(STATUS "Building Complex Precision")
  list(APPEND FLOAT_TYPES "COMPLEX") # defines COMPLEX
endif ()

if (BUILD_COMPLEX16)
  message(STATUS "Building Double Complex Precision")
  list(APPEND FLOAT_TYPES "ZCOMPLEX") # defines COMPLEX and DOUBLE
endif ()

if (BUILD_BFLOAT16)
  message(STATUS "Building Half Precision")
  #  list(APPEND FLOAT_TYPES "BFLOAT16") # defines nothing
endif ()

if (NOT DEFINED CORE OR "${CORE}" STREQUAL "UNKNOWN")
  message(FATAL_ERROR "Detecting CPU failed. Please set TARGET explicitly, e.g. make TARGET=your_cpu_target. Please read README for details.")
endif ()

#Set default output directory
set( CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set( CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
if(MSVC)
  set( CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG ${PROJECT_BINARY_DIR}/lib/Debug)
  set( CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${PROJECT_BINARY_DIR}/lib/Release)
endif ()

# get obj vars into format that add_library likes: $<TARGET_OBJS:objlib> (see http://www.cmake.org/cmake/help/v3.0/command/add_library.html)
set(TARGET_OBJS "")
foreach (SUBDIR ${SUBDIRS})
  add_subdirectory(${SUBDIR})
  string(REPLACE "/" "_" subdir_obj ${SUBDIR})
  list(APPEND TARGET_OBJS "$<TARGET_OBJECTS:${subdir_obj}>")
endforeach ()

# netlib:

# Can't just use lapack-netlib's CMake files, since they are set up to search for BLAS, build and install a binary. We just want to build a couple of lib files out of lapack and lapacke.
# Not using add_subdirectory here because lapack-netlib already has its own CMakeLists.txt. Instead include a cmake script with the sources we want.
if (NOT NO_LAPACK)
  include("${PROJECT_SOURCE_DIR}/cmake/lapack.cmake")
  if (NOT NO_LAPACKE)
    include("${PROJECT_SOURCE_DIR}/cmake/lapacke.cmake")
  endif ()
endif ()

# Only generate .def for dll on MSVC and always produce pdb files for debug and release
if(MSVC)
  if (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} VERSION_LESS 3.4)
    set(OpenBLAS_DEF_FILE "${PROJECT_BINARY_DIR}/openblas.def")
  endif()
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /Zi")
  set(CMAKE_SHARED_LINKER_FLAGS_RELEASE "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} /DEBUG /OPT:REF /OPT:ICF")
endif()

if (${DYNAMIC_ARCH})
  add_subdirectory(kernel)
  foreach(TARGET_CORE ${DYNAMIC_CORE})
    message("${TARGET_CORE}")
    list(APPEND TARGET_OBJS "$<TARGET_OBJECTS:kernel_${TARGET_CORE}>")
  endforeach()
endif ()

# add objects to the openblas lib
if(NOT NO_LAPACK)
  add_library(LAPACK_OVERRIDES OBJECT ${LA_SOURCES})
  if (USE_OPENMP AND (NOT NOFORTRAN))
    # Disable OpenMP for LAPACK Fortran codes on Windows.
    if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
      target_link_libraries(LAPACK_OVERRIDES OpenMP::OpenMP_Fortran)
    endif()
  endif()
  list(APPEND TARGET_OBJS "$<TARGET_OBJECTS:LAPACK_OVERRIDES>")
endif()
if(NOT NO_LAPACKE)
  add_library(LAPACKE OBJECT ${LAPACKE_SOURCES})
  list(APPEND TARGET_OBJS "$<TARGET_OBJECTS:LAPACKE>")
endif()
#if(BUILD_RELAPACK)
#  add_library(RELAPACK OBJECT ${RELA_SOURCES})
#  list(APPEND TARGET_OBJS "$<TARGET_OBJECTS:RELAPACK>")
#endif()
set(OpenBLAS_LIBS "")
if(BUILD_STATIC_LIBS)
  add_library(${OpenBLAS_LIBNAME}_static STATIC ${TARGET_OBJS} ${OpenBLAS_DEF_FILE})
  target_include_directories(${OpenBLAS_LIBNAME}_static INTERFACE $<INSTALL_INTERFACE:include/openblas${SUFFIX64}>)
  list(APPEND OpenBLAS_LIBS ${OpenBLAS_LIBNAME}_static)
endif()
if(BUILD_SHARED_LIBS)
  add_library(${OpenBLAS_LIBNAME}_shared SHARED ${TARGET_OBJS} ${OpenBLAS_DEF_FILE})
  target_include_directories(${OpenBLAS_LIBNAME}_shared INTERFACE $<INSTALL_INTERFACE:include/openblas${SUFFIX64}>)
  list(APPEND OpenBLAS_LIBS ${OpenBLAS_LIBNAME}_shared)
endif()
if(BUILD_STATIC_LIBS)
  add_library(${OpenBLAS_LIBNAME} ALIAS ${OpenBLAS_LIBNAME}_static)
else()
  add_library(${OpenBLAS_LIBNAME} ALIAS ${OpenBLAS_LIBNAME}_shared)
endif()

set_target_properties(${OpenBLAS_LIBS} PROPERTIES OUTPUT_NAME ${OpenBLAS_LIBNAME})

# Android needs to explicitly link against libm
if (${CMAKE_SYSTEM_NAME} MATCHES "AIX|Android|Linux|FreeBSD|OpenBSD|NetBSD|DragonFly|Darwin")
  if(BUILD_STATIC_LIBS)
    target_link_libraries(${OpenBLAS_LIBNAME}_static m)
  endif()
  if(BUILD_SHARED_LIBS)
    target_link_libraries(${OpenBLAS_LIBNAME}_shared m)
  endif()
endif()

if (USE_OPENMP)
  if(BUILD_STATIC_LIBS)
    if(NOFORTRAN)
      target_link_libraries(${OpenBLAS_LIBNAME}_static OpenMP::OpenMP_C)
    else()
      target_link_libraries(${OpenBLAS_LIBNAME}_static OpenMP::OpenMP_C OpenMP::OpenMP_Fortran)
    endif()
  endif()
  if(BUILD_SHARED_LIBS)
    if(NOFORTRAN)
      target_link_libraries(${OpenBLAS_LIBNAME}_shared OpenMP::OpenMP_C)
    else()
      target_link_libraries(${OpenBLAS_LIBNAME}_shared OpenMP::OpenMP_C OpenMP::OpenMP_Fortran)
    endif()
  endif()
endif()

# Fix "Argument list too long" for macOS with Intel CPUs and DYNAMIC_ARCH turned on
if(APPLE AND DYNAMIC_ARCH AND (NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64"))
  # Use response files
  set(CMAKE_C_USE_RESPONSE_FILE_FOR_OBJECTS 1)
  # Always build static library first
  if(BUILD_STATIC_LIBS)
    set(STATIC_PATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/lib${OpenBLAS_LIBNAME}.a")
  else()
    add_library(${OpenBLAS_LIBNAME}_static STATIC ${TARGET_OBJS} ${OpenBLAS_DEF_FILE})
    set(STATIC_PATH "lib${OpenBLAS_LIBNAME}.a")
  endif()
  set(CREATE_STATIC_LIBRARY_COMMAND
    "sh -c 'cat ${CMAKE_BINARY_DIR}/CMakeFiles/${OpenBLAS_LIBNAME}_static.dir/objects*.rsp | xargs -n 1024 ${CMAKE_AR} -ru ${STATIC_PATH} && exit 0' "
    "sh -c '${CMAKE_AR} -rs ${STATIC_PATH} ${CMAKE_BINARY_DIR}/driver/others/CMakeFiles/driver_others.dir/xerbla.c.o && exit 0' ")
  if(BUILD_SHARED_LIBS)
    add_dependencies(${OpenBLAS_LIBNAME}_shared ${OpenBLAS_LIBNAME}_static)
    set(SHARED_PATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libopenblas.${OpenBLAS_MAJOR_VERSION}.${OpenBLAS_MINOR_VERSION}.dylib")
  endif()
  if(USE_OPENMP)
    get_target_property(OMP_LIB OpenMP::OpenMP_C INTERFACE_LINK_LIBRARIES)
  else()
    set(OMP_LIB "")
  endif()
  if(NOT NOFORTRAN)
    set(CMAKE_Fortran_USE_RESPONSE_FILE_FOR_OBJECTS 1)
    set(CMAKE_Fortran_CREATE_STATIC_LIBRARY ${CREATE_STATIC_LIBRARY_COMMAND})
    if(BUILD_SHARED_LIBS)
      set(CMAKE_Fortran_CREATE_SHARED_LIBRARY
	"sh -c 'echo \"\" | ${CMAKE_Fortran_COMPILER} -o dummy.o -c -x f95-cpp-input - '"
	"sh -c '${CMAKE_Fortran_COMPILER} -fpic -shared -Wl,-all_load -Wl,-force_load,${STATIC_PATH} dummy.o -o ${SHARED_PATH} ${OMP_LIB}'")
    endif()
  else()
    set(CMAKE_C_CREATE_STATIC_LIBRARY ${CREATE_STATIC_LIBRARY_COMMAND})
    if(BUILD_SHARED_LIBS)
      set(CMAKE_C_CREATE_SHARED_LIBRARY
	"sh -c '${CMAKE_C_COMPILER} -fpic -shared -Wl,-all_load -Wl,-force_load,${STATIC_PATH} -o ${SHARED_PATH} ${OMP_LIB}'")
    endif()
  endif()
endif()

# Handle MSVC exports
if(MSVC AND BUILD_SHARED_LIBS)
  if (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} VERSION_LESS 3.4)
    include("${PROJECT_SOURCE_DIR}/cmake/export.cmake")
  else()
    # Creates verbose .def file (51KB vs 18KB)
    set_target_properties(${OpenBLAS_LIBNAME}_shared PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS true)
  endif()
endif()

# Set output for libopenblas
set_target_properties( ${OpenBLAS_LIBS} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set_target_properties( ${OpenBLAS_LIBS} PROPERTIES LIBRARY_OUTPUT_NAME_DEBUG "${OpenBLAS_LIBNAME}_d")
set_target_properties( ${OpenBLAS_LIBS} PROPERTIES EXPORT_NAME "OpenBLAS")

foreach (OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
  string( TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG )

  set_target_properties( ${OpenBLAS_LIBS} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${PROJECT_BINARY_DIR}/lib/${OUTPUTCONFIG} )
  set_target_properties( ${OpenBLAS_LIBS} PROPERTIES LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${PROJECT_BINARY_DIR}/lib/${OUTPUTCONFIG} )
  set_target_properties( ${OpenBLAS_LIBS} PROPERTIES ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${PROJECT_BINARY_DIR}/lib/${OUTPUTCONFIG} )
endforeach()

enable_testing()

if (USE_THREAD)
  # Add threading library to linker
  find_package(Threads)
  if (THREADS_HAVE_PTHREAD_ARG)
    set_target_properties(${OpenBLAS_LIBS} PROPERTIES
      COMPILE_OPTIONS "-pthread"
      INTERFACE_COMPILE_OPTIONS "-pthread"
    )
  endif()
  if(BUILD_STATIC_LIBS)
    target_link_libraries(${OpenBLAS_LIBNAME}_static ${CMAKE_THREAD_LIBS_INIT})
  endif()
  if(BUILD_SHARED_LIBS)
    target_link_libraries(${OpenBLAS_LIBNAME}_shared ${CMAKE_THREAD_LIBS_INIT})
  endif()
endif()

#if (MSVC OR NOT NOFORTRAN)
if (NOT NO_CBLAS)
  if (NOT ONLY_CBLAS)
  # Broken without fortran on unix
    add_subdirectory(utest)
endif()
endif()

if (NOT NOFORTRAN)
 if (NOT ONLY_CBLAS)
  # Build test and ctest
  add_subdirectory(test)
 endif()
 if (BUILD_TESTING AND NOT BUILD_WITHOUT_LAPACK)
    add_subdirectory(lapack-netlib/TESTING)
  endif()
endif()
  if(NOT NO_CBLAS)
   if (NOT ONLY_CBLAS)
    add_subdirectory(ctest)
   endif()
  endif()
  if (CPP_THREAD_SAFETY_TEST OR CPP_THREAD_SAFETY_GEMV)
    add_subdirectory(cpp_thread_test)
  endif()

if (NOT FIXED_LIBNAME)
set_target_properties(${OpenBLAS_LIBS} PROPERTIES
  VERSION ${OpenBLAS_MAJOR_VERSION}.${OpenBLAS_MINOR_VERSION}
  SOVERSION ${OpenBLAS_MAJOR_VERSION}
)
endif()
if (BUILD_SHARED_LIBS AND BUILD_RELAPACK)
  if (NOT MSVC)
    target_link_libraries(${OpenBLAS_LIBNAME}_shared "-Wl,-allow-multiple-definition")
  else()
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /FORCE:MULTIPLE")
  endif()
endif()

if (BUILD_SHARED_LIBS OR DELETE_STATIC_LIBS AND NOT ${SYMBOLPREFIX}${SYMBOLSUFFIX} STREQUAL "")
  if (NOT DEFINED ARCH)
    set(ARCH_IN "x86_64")
  else()
    set(ARCH_IN ${ARCH})
  endif()

  if (${CORE} STREQUAL "generic")
    set(ARCH_IN "GENERIC")
  endif ()

  if (NOT DEFINED EXPRECISION)
    set(EXPRECISION_IN 0)
  else()
    set(EXPRECISION_IN ${EXPRECISION})
  endif()

  if (NOT DEFINED NO_CBLAS)
    set(NO_CBLAS_IN 0)
  else()
    set(NO_CBLAS_IN ${NO_CBLAS})
  endif()

  if (NOT DEFINED NO_LAPACK)
    set(NO_LAPACK_IN 0)
  else()
    set(NO_LAPACK_IN ${NO_LAPACK})
  endif()

  if (NOT DEFINED NO_LAPACKE)
    set(NO_LAPACKE_IN 0)
  else()
    set(NO_LAPACKE_IN ${NO_LAPACKE})
  endif()

  if (NOT DEFINED NEED2UNDERSCORES)
    set(NEED2UNDERSCORES_IN 0)
  else()
    set(NEED2UNDERSCORES_IN ${NEED2UNDERSCORES})
  endif()

  if (NOT DEFINED ONLY_CBLAS)
    set(ONLY_CBLAS_IN 0)
  else()
    set(ONLY_CBLAS_IN ${ONLY_CBLAS})
  endif()

  if (NOT DEFINED BU)
    set(BU _)
  endif()

  if (NOT ${SYMBOLPREFIX} STREQUAL "")
    message(STATUS "adding prefix ${SYMBOLPREFIX} to names of exported symbols in ${OpenBLAS_LIBNAME}")
  endif()
  if (NOT ${SYMBOLSUFFIX} STREQUAL "")
    message(STATUS "adding suffix ${SYMBOLSUFFIX} to names of exported symbols in ${OpenBLAS_LIBNAME}")
  endif()

  if (${BUILD_LAPACK_DEPRECATED})
	  set (BLD 1)
  else ()
	  set (BLD 0)
  endif()
  if (${BUILD_BFLOAT16})
	  set (BBF16 1)
  else ()
	  set (BBF16 0)
  endif()
  if (${BUILD_SINGLE})
	  set (BS 1)
  else ()
	  set (BS 0)
  endif()
  if (${BUILD_DOUBLE})
	  set (BD 1)
  else ()
	  set (BD 0)
  endif()
  if (${BUILD_COMPLEX})
	  set (BC 1)
  else ()
	  set (BC 0)
  endif()
  if (${BUILD_COMPLEX16})
	  set (BZ 1)
  else ()
	  set (BZ 0)
  endif()

  if (CMAKE_SYSTEM_NAME MATCHES "Windows")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
if (CMAKE_BUILD_TYPE MATCHES "Debug")
set (CRTLIB msvcrtd)
set (PDBOPT -debug -pdb:$<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.pdb)
set (PDB_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
else ()
set (CRTLIB msvcrt)
set (PDBOPT "")
endif()
 #if (USE_PERL)
message(STATUS "adding postbuild instruction to rename syms")
  add_custom_command(TARGET ${OpenBLAS_LIBNAME}_static POST_BUILD
	COMMAND perl ${PROJECT_SOURCE_DIR}/exports/gensymbol.pl "win2k" "${ARCH}" "${BU}" "${EXPRECISION_IN}" "${NO_CBLAS_IN}" "${NO_LAPACK_IN}" "${NO_LAPACKE_IN}" "${NEED2UNDERSCORES_IN}" "${ONLY_CBLAS_IN}" "${SYMBOLPREFIX}" "${SYMBOLSUFFIX}" "${BLD}" "${BBF16}" "${BS}" "${BD}" "${BC}" "${BZ}" > ${PROJECT_BINARY_DIR}/renamesyms.def
	COMMAND ${CMAKE_C_COMPILER} ${CMAKE_C_FLAGS} -I${PROJECT_SOURCE_DIR} -I${PROJECT_BINARY_DIR} -c -o ${PROJECT_BINARY_DIR}/dllinit.o ${PROJECT_SOURCE_DIR}/exports/dllinit.c
	COMMAND lld-link -nodefaultlib:libcmt -defaultlib:${CRTLIB} ${CMAKE_LINKER_FLAGS} -errorlimit:0 -def:${PROJECT_BINARY_DIR}/renamesyms.def ${PROJECT_BINARY_DIR}/dllinit.o $<TARGET_FILE:${OpenBLAS_LIBNAME}_static> -wholearchive:$<TARGET_FILE:${OpenBLAS_LIBNAME}_static> -dll -out:$<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.dll -implib:$<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.dll.a ${PDBOPT} 
	#COMMAND lld-link -nodefaultlib:libcmt -defaultlib:msvcrt ${CMAKE_LINKER_FLAGS} -errorlimit:0 -def:${PROJECT_BINARY_DIR}/renamesyms.def ${PROJECT_BINARY_DIR}/dllinit.o $<TARGET_FILE:${OpenBLAS_LIBNAME}_static> -wholearchive:$<TARGET_FILE:${OpenBLAS_LIBNAME}_static> -dll -out:$<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.dll -implib:$<TARGET_FILE_DIR:${OpenBLAS_LIBNAME}_static>/${OpenBLAS_LIBNAME}.dll.a
	 ${REMOVE_STATIC_LIB} VERBATIM
  )
  #endif ()
  else ()
  if (NOT USE_PERL)
  add_custom_command(TARGET ${OpenBLAS_LIBNAME}_shared POST_BUILD
	  COMMAND sh ${PROJECT_SOURCE_DIR}/exports/gensymbol "objcopy" "${ARCH}" "${BU}" "${EXPRECISION_IN}" "${NO_CBLAS_IN}" "${NO_LAPACK_IN}" "${NO_LAPACKE_IN}" "${NEED2UNDERSCORES_IN}" "${ONLY_CBLAS_IN}" \"${SYMBOLPREFIX}\" \"${SYMBOLSUFFIX}\" "${BLD}" "${BBF16}" "${BS}" "${BD}" "${BC}" "${BZ}" > ${PROJECT_BINARY_DIR}/objcopy.def
    COMMAND objcopy -v --redefine-syms ${PROJECT_BINARY_DIR}/objcopy.def  ${PROJECT_BINARY_DIR}/lib/${OpenBLAS_LIBNAME}.so
    COMMENT "renaming symbols"
    )
  else()
  add_custom_command(TARGET ${OpenBLAS_LIBNAME}_shared POST_BUILD
    COMMAND perl ${PROJECT_SOURCE_DIR}/exports/gensymbol.pl "objcopy" "${ARCH}" "${BU}" "${EXPRECISION_IN}" "${NO_CBLAS_IN}" "${NO_LAPACK_IN}" "${NO_LAPACKE_IN}" "${NEED2UNDERSCORES_IN}" "${ONLY_CBLAS_IN}" \"${SYMBOLPREFIX}\" \"${SYMBOLSUFFIX}\" "${BLD}" "${BBF16}" "${BS}" "${BD}" "${BC}" "${BZ}" > ${PROJECT_BINARY_DIR}/objcopy.def
    COMMAND objcopy -v --redefine-syms ${PROJECT_BINARY_DIR}/objcopy.def  ${PROJECT_BINARY_DIR}/lib/lib${OpenBLAS_LIBNAME}.so
    COMMENT "renaming symbols"
    )
  endif()
endif()
endif()

if (BUILD_BENCHMARKS)
	#find_package(OpenMP REQUIRED)
  file(GLOB SOURCES "benchmark/*.c")
  if (NOT USE_OPENMP)
  file(GLOB REMFILE "benchmark/smallscaling.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  endif()
  if (BUILD_WITHOUT_LAPACK)
  file(GLOB REMFILE "benchmark/cholesky.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/geev.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/gesv.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/getri.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/potrf.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/spmv.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/symv.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  file(GLOB REMFILE "benchmark/linpack.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  endif()
  if (NOT USE_GEMM3M)
  file(GLOB REMFILE "benchmark/gemm3m.c")
  list(REMOVE_ITEM SOURCES ${REMFILE})
  endif()
  foreach(source ${SOURCES})
    get_filename_component(name ${source} NAME_WE)
    if ((NOT ${name} STREQUAL "zdot-intel") AND (NOT ${name} STREQUAL "cula_wrapper"))
        set(defines DEFAULT COMPLEX DOUBLE "COMPLEX\;DOUBLE")
      foreach(define ${defines})
        set(target_name "benchmark_${name}")
        if (NOT "${define}" STREQUAL "DEFAULT")
          string(JOIN "_" define_str ${define})
          set(target_name "${target_name}_${define_str}")
        endif()
        if ((NOT ${target_name} STREQUAL "benchmark_imax_COMPLEX") AND (NOT ${target_name} STREQUAL "benchmark_imax_COMPLEX_DOUBLE") AND
            (NOT ${target_name} STREQUAL "benchmark_imin_COMPLEX") AND (NOT ${target_name} STREQUAL "benchmark_imin_COMPLEX_DOUBLE") AND
            (NOT ${target_name} STREQUAL "benchmark_max_COMPLEX") AND (NOT ${target_name} STREQUAL "benchmark_max_COMPLEX_DOUBLE") AND
            (NOT ${target_name} STREQUAL "benchmark_min_COMPLEX") AND (NOT ${target_name} STREQUAL "benchmark_min_COMPLEX_DOUBLE"))
          add_executable(${target_name} ${source})
          target_include_directories(${target_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
	    target_link_libraries(${target_name} ${OpenBLAS_LIBNAME} )
	  #  target_link_libraries(${target_name} ${OpenBLAS_LIBNAME} OpenMP::OpenMP_C)
          if (NOT "${define}" STREQUAL "DEFAULT")
            target_compile_definitions(${target_name} PRIVATE ${define})
          endif()
        endif()
      endforeach()
    endif()
  endforeach()
endif()


# Install project

# Install libraries
if(BUILD_SHARED_LIBS AND BUILD_STATIC_LIBS)
  install(TARGETS ${OpenBLAS_LIBNAME}_shared
    EXPORT "OpenBLAS${SUFFIX64}Targets"
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
  install(TARGETS ${OpenBLAS_LIBNAME}_static
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
else()
  install(TARGETS ${OpenBLAS_LIBS}
    EXPORT "OpenBLAS${SUFFIX64}Targets"
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
endif()

# Install headers
set(CMAKE_INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR}/openblas${SUFFIX64})
set(CMAKE_INSTALL_FULL_INCLUDEDIR ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})

message(STATUS "Generating openblas_config.h in ${CMAKE_INSTALL_INCLUDEDIR}")

set(OPENBLAS_CONFIG_H ${CMAKE_BINARY_DIR}/openblas_config.h)
file(WRITE  ${OPENBLAS_CONFIG_H} "#ifndef OPENBLAS_CONFIG_H\n")
file(APPEND ${OPENBLAS_CONFIG_H} "#define OPENBLAS_CONFIG_H\n")
file(STRINGS ${PROJECT_BINARY_DIR}/config.h __lines)
foreach(line ${__lines})
  string(REPLACE "#define " "" line ${line})
  file(APPEND ${OPENBLAS_CONFIG_H} "#define OPENBLAS_${line}\n")
endforeach()
file(APPEND ${OPENBLAS_CONFIG_H} "#define OPENBLAS_VERSION \"OpenBLAS ${OpenBLAS_VERSION}\"\n")
file(READ   ${CMAKE_CURRENT_SOURCE_DIR}/openblas_config_template.h OPENBLAS_CONFIG_TEMPLATE_H_CONTENTS)
file(APPEND ${OPENBLAS_CONFIG_H} "${OPENBLAS_CONFIG_TEMPLATE_H_CONTENTS}\n")
file(APPEND ${OPENBLAS_CONFIG_H} "#endif /* OPENBLAS_CONFIG_H */\n")
install (FILES ${OPENBLAS_CONFIG_H} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if(NOT NOFORTRAN)
  message(STATUS "Generating f77blas.h in ${CMAKE_INSTALL_INCLUDEDIR}")

  set(F77BLAS_H ${CMAKE_BINARY_DIR}/generated/f77blas.h)
  file(WRITE  ${F77BLAS_H} "#ifndef OPENBLAS_F77BLAS_H\n")
  file(APPEND ${F77BLAS_H} "#define OPENBLAS_F77BLAS_H\n")
  file(APPEND ${F77BLAS_H} "#include \"openblas_config.h\"\n")
  file(READ ${CMAKE_CURRENT_SOURCE_DIR}/common_interface.h COMMON_INTERFACE_H_CONTENTS)
  file(APPEND ${F77BLAS_H} "${COMMON_INTERFACE_H_CONTENTS}\n")
  file(APPEND ${F77BLAS_H} "#endif")
  install (FILES ${F77BLAS_H} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

if(NOT NO_CBLAS)
  message (STATUS "Generating cblas.h in ${CMAKE_INSTALL_INCLUDEDIR}")
  set(CBLAS_H ${CMAKE_BINARY_DIR}/generated/cblas.h)
  file(READ ${CMAKE_CURRENT_SOURCE_DIR}/cblas.h CBLAS_H_CONTENTS)
  string(REPLACE "common" "openblas_config" CBLAS_H_CONTENTS_NEW "${CBLAS_H_CONTENTS}")
  if (NOT ${SYMBOLPREFIX} STREQUAL "")
    string(REPLACE " cblas" " ${SYMBOLPREFIX}cblas" CBLAS_H_CONTENTS	"${CBLAS_H_CONTENTS_NEW}")
    string(REPLACE " openblas" " ${SYMBOLPREFIX}openblas" CBLAS_H_CONTENTS_NEW	"${CBLAS_H_CONTENTS}")
    string (REPLACE " ${SYMBOLPREFIX}openblas_complex" " openblas_complex" CBLAS_H_CONTENTS	"${CBLAS_H_CONTENTS_NEW}")
    string(REPLACE " goto" " ${SYMBOLPREFIX}goto" CBLAS_H_CONTENTS_NEW "${CBLAS_H_CONTENTS}")
  endif()
  if (NOT ${SYMBOLSUFFIX} STREQUAL "")
    string(REGEX REPLACE "(cblas[^ (]*)" "\\1${SYMBOLSUFFIX}" CBLAS_H_CONTENTS	"${CBLAS_H_CONTENTS_NEW}")
    string(REGEX REPLACE "(openblas[^ (]*)" "\\1${SYMBOLSUFFIX}" CBLAS_H_CONTENTS_NEW "${CBLAS_H_CONTENTS}")
    string(REGEX REPLACE "(openblas_complex[^ ]*)${SYMBOLSUFFIX}" "\\1" CBLAS_H_CONTENTS	"${CBLAS_H_CONTENTS_NEW}")
    string(REGEX REPLACE "(goto[^ (]*)" "\\1${SYMBOLSUFFIX}" CBLAS_H_CONTENTS_NEW	"${CBLAS_H_CONTENTS}")
  endif()
  file(WRITE ${CBLAS_H} "${CBLAS_H_CONTENTS_NEW}")
  install (FILES ${CBLAS_H} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

if(NOT NO_LAPACKE)
  message (STATUS "Copying LAPACKE header files to ${CMAKE_INSTALL_INCLUDEDIR}")
  if(BUILD_STATIC_LIBS)
    add_dependencies( ${OpenBLAS_LIBNAME}_static genlapacke)
  endif()
  if(BUILD_SHARED_LIBS)
    add_dependencies( ${OpenBLAS_LIBNAME}_shared genlapacke)
  endif()
  FILE(GLOB_RECURSE INCLUDE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/lapack-netlib/LAPACKE/*.h")
  install (FILES ${INCLUDE_FILES} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

  ADD_CUSTOM_TARGET(genlapacke
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/lapack-netlib/LAPACKE/include/lapacke_mangling_with_flags.h.in "${CMAKE_BINARY_DIR}/lapacke_mangling.h"
  )
  install (FILES ${CMAKE_BINARY_DIR}/lapacke_mangling.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

# Install pkg-config files
configure_file(${PROJECT_SOURCE_DIR}/cmake/openblas.pc.in ${PROJECT_BINARY_DIR}/openblas${SUFFIX64}.pc @ONLY)
install (FILES ${PROJECT_BINARY_DIR}/openblas${SUFFIX64}.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig/)


set(PN OpenBLAS)
set(CMAKECONFIG_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PN}${SUFFIX64}")
configure_package_config_file(cmake/${PN}Config.cmake.in
                              "${CMAKE_CURRENT_BINARY_DIR}/${PN}${SUFFIX64}Config.cmake"
                              INSTALL_DESTINATION ${CMAKECONFIG_INSTALL_DIR})
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PN}ConfigVersion.cmake
                                 VERSION ${${PN}_VERSION}
                                 COMPATIBILITY AnyNewerVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PN}${SUFFIX64}Config.cmake
        DESTINATION ${CMAKECONFIG_INSTALL_DIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PN}ConfigVersion.cmake
        RENAME ${PN}${SUFFIX64}ConfigVersion.cmake
        DESTINATION ${CMAKECONFIG_INSTALL_DIR})
install(EXPORT "${PN}${SUFFIX64}Targets"
        NAMESPACE "${PN}${SUFFIX64}::"
        DESTINATION ${CMAKECONFIG_INSTALL_DIR})

