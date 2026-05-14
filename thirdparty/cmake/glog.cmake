# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# glog — hand-written add_library (no add_subdirectory)
set(GLOG_SRC_DIR ${TP_SOURCE_DIR}/glog-0.6.0)
set(GLOG_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/glog)

# ---------- Template variables for header generation (Linux x86_64) ----------
set(ac_cv_cxx11_chrono 1)
set(ac_cv_cxx11_atomic 1)
set(ac_cv_have_unistd_h 1)
set(ac_cv_have_stdint_h 1)
set(ac_cv_have_systypes_h 1)
set(ac_cv_have_inttypes_h 1)
set(ac_cv_have_libgflags 0)
set(ac_cv_have_uint16_t 1)
set(ac_cv_have_u_int16_t 1)
set(ac_cv_have___uint16 0)
set(ac_cv_have___builtin_expect 1)
set(ac_cv_have_glog_export 1)
set(ac_cv_cxx11_nullptr_t 1)
set(ac_cv_cxx11_constexpr 1)
set(ac_cv___attribute___noreturn "__attribute__((noreturn))")
set(ac_cv___attribute___noinline "__attribute__((noinline))")
set(ac_cv_cxx_using_operator 1)
set(ac_google_start_namespace "namespace google {")
set(ac_google_end_namespace "}")
set(ac_google_namespace "google")

# ---------- configure_file: public headers ----------
file(MAKE_DIRECTORY ${GLOG_GEN_DIR}/glog)

configure_file(
    "${GLOG_SRC_DIR}/src/glog/logging.h.in"
    "${GLOG_GEN_DIR}/glog/logging.h"
    @ONLY
)
configure_file(
    "${GLOG_SRC_DIR}/src/glog/raw_logging.h.in"
    "${GLOG_GEN_DIR}/glog/raw_logging.h"
    @ONLY
)
configure_file(
    "${GLOG_SRC_DIR}/src/glog/stl_logging.h.in"
    "${GLOG_GEN_DIR}/glog/stl_logging.h"
    @ONLY
)
configure_file(
    "${GLOG_SRC_DIR}/src/glog/vlog_is_on.h.in"
    "${GLOG_GEN_DIR}/glog/vlog_is_on.h"
    @ONLY
)

# ---------- config.h: write directly (too many #cmakedefine to template) ----------
set(GOOGLE_NAMESPACE google)
set(HAVE_SNPRINTF 1)
set(HAVE_DLFCN_H 1)
set(HAVE_EXECINFO_H 1)
set(HAVE_FCNTL 1)
set(HAVE_GLOB_H 1)
set(HAVE_INTTYPES_H 1)
set(HAVE_MEMORY_H 1)
set(HAVE_NAMESPACES 1)
set(HAVE_PREAD 1)
set(HAVE_PTHREAD 1)
set(HAVE_PWD_H 1)
set(HAVE_PWRITE 1)
set(HAVE_RWLOCK 1)
set(HAVE_SIGACTION 1)
set(HAVE_SIGALTSTACK 1)
set(HAVE_STDINT_H 1)
set(HAVE_STRINGS_H 1)
set(HAVE_SYSCALL_H 1)
set(HAVE_SYSLOG_H 1)
set(HAVE_SYS_STAT_H 1)
set(HAVE_SYS_SYSCALL_H 1)
set(HAVE_SYS_TIME_H 1)
set(HAVE_SYS_TYPES_H 1)
set(HAVE_SYS_UTSNAME_H 1)
set(HAVE_SYS_WAIT_H 1)
set(HAVE_UCONTEXT_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_UNWIND_H 1)
set(HAVE__UNWIND_BACKTRACE 1)
set(HAVE_USING_OPERATOR 1)
set(HAVE___ATTRIBUTE__ 1)
set(HAVE___BUILTIN_EXPECT 1)
set(HAVE___SYNC_VAL_COMPARE_AND_SWAP 1)
set(HAVE_SYMBOLIZE 1)
set(HAVE_LOCALTIME_R 1)
set(SIZEOF_VOID_P 8)
set(STL_NAMESPACE std)
set(_START_GOOGLE_NAMESPACE_ "namespace google {")
set(_END_GOOGLE_NAMESPACE_ "}")
set(HAVE_ALIGNED_STORAGE 1)
set(HAVE_CXX11_ATOMIC 1)
set(HAVE_CXX11_CHRONO 1)
set(HAVE_CXX11_NULLPTR_T 1)

configure_file(
    "${GLOG_SRC_DIR}/src/config.h.cmake.in"
    "${GLOG_GEN_DIR}/config.h"
)

# ---------- export.h: generate directly ----------
include(GenerateExportHeader)
file(WRITE "${GLOG_GEN_DIR}/glog/export.h" "\n#ifndef GLOG_EXPORT_H\n#define GLOG_EXPORT_H\n\n#ifdef GLOG_STATIC_DEFINE\n#  define GLOG_EXPORT\n#  define GLOG_NO_EXPORT\n#else\n#  ifndef GLOG_EXPORT\n#    ifdef GOOGLE_GLOG_IS_A_DLL\n#      define GLOG_EXPORT\n#    else\n#      define GLOG_EXPORT\n#    endif\n#  endif\n#  ifndef GLOG_NO_EXPORT\n#    define GLOG_NO_EXPORT\n#  endif\n#endif\n\n#ifndef GLOG_DEPRECATED\n#  define GLOG_DEPRECATED __attribute__ ((__deprecated__))\n#endif\n\n#ifndef GLOG_DEPRECATED_EXPORT\n#  define GLOG_DEPRECATED_EXPORT GLOG_EXPORT GLOG_DEPRECATED\n#endif\n\n#ifndef GLOG_DEPRECATED_NO_EXPORT\n#  define GLOG_DEPRECATED_NO_EXPORT GLOG_NO_EXPORT GLOG_DEPRECATED\n#endif\n\n#if 0 /* DEFINE_NO_DEPRECATED */\n#  ifndef GLOG_NO_DEPRECATED\n#    define GLOG_NO_DEPRECATED\n#  endif\n#endif\n\n#endif /* GLOG_EXPORT_H */\n")

# ---------- add_library ----------
add_library(glog STATIC
    ${GLOG_SRC_DIR}/src/demangle.cc
    ${GLOG_SRC_DIR}/src/logging.cc
    ${GLOG_SRC_DIR}/src/raw_logging.cc
    ${GLOG_SRC_DIR}/src/symbolize.cc
    ${GLOG_SRC_DIR}/src/utilities.cc
    ${GLOG_SRC_DIR}/src/vlog_is_on.cc
    ${GLOG_SRC_DIR}/src/signalhandler.cc
)

target_include_directories(glog SYSTEM PUBLIC
    ${GLOG_SRC_DIR}/src
    ${GLOG_GEN_DIR}
)

target_include_directories(glog PRIVATE
    ${GLOG_SRC_DIR}/src
    ${GLOG_GEN_DIR}
)

target_compile_options(glog PRIVATE -fPIC -w)
target_compile_definitions(glog PUBLIC
    GLOG_CUSTOM_PREFIX_SUPPORT
)

find_package(Threads REQUIRED)
target_link_libraries(glog PRIVATE Threads::Threads)

set_target_properties(glog PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

add_library(glog::glog ALIAS glog)
