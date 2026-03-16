# libevent
set(EVENT__DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(EVENT__DISABLE_SAMPLES ON CACHE BOOL "" FORCE)
set(EVENT__DISABLE_BENCHMARK ON CACHE BOOL "" FORCE)
set(EVENT__LIBRARY_TYPE "STATIC" CACHE STRING "" FORCE)
set(EVENT__DISABLE_OPENSSL OFF CACHE BOOL "" FORCE)
# Point to our openssl
set(OPENSSL_ROOT_DIR ${TP_INSTALL_DIR} CACHE PATH "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/doris-thirdparty-libevent-2.1.12.1 ${CMAKE_CURRENT_BINARY_DIR}/libevent EXCLUDE_FROM_ALL)
# Alias targets to match Doris link names
if(TARGET event_core_static)
    add_library(libevent_core ALIAS event_core_static)
    add_library(libevent ALIAS event_static)
    add_library(libevent_pthreads ALIAS event_pthreads_static)
endif()
if(TARGET event_openssl_static)
    add_library(libevent_openssl ALIAS event_openssl_static)
endif()
