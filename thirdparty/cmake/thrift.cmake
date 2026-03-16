# thrift
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_COMPILER OFF CACHE BOOL "" FORCE)
set(BUILD_TUTORIALS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(WITH_SHARED_LIB OFF CACHE BOOL "" FORCE)
set(WITH_STATIC_LIB ON CACHE BOOL "" FORCE)
set(WITH_JAVA OFF CACHE BOOL "" FORCE)
set(WITH_PYTHON OFF CACHE BOOL "" FORCE)
set(WITH_HASKELL OFF CACHE BOOL "" FORCE)
set(WITH_C_GLIB OFF CACHE BOOL "" FORCE)
set(WITH_CPP ON CACHE BOOL "" FORCE)
set(WITH_LIBEVENT ON CACHE BOOL "" FORCE)
set(WITH_OPENSSL ON CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl CACHE PATH "" FORCE)

set(_THRIFT_SRC "${TP_SOURCE_DIR}/thrift-0.16.0")

# Patch 1: rename uninstall target to avoid conflict
file(READ "${_THRIFT_SRC}/CMakeLists.txt" _THRIFT_CMAKELISTS)
if(_THRIFT_CMAKELISTS MATCHES "add_custom_target\\(uninstall")
    string(REPLACE
        "add_custom_target(uninstall"
        "add_custom_target(thrift_uninstall"
        _THRIFT_CMAKELISTS "${_THRIFT_CMAKELISTS}")
    file(WRITE "${_THRIFT_SRC}/CMakeLists.txt" "${_THRIFT_CMAKELISTS}")
endif()

# Patch 2: remove export() calls from ThriftMacros.cmake BEFORE add_subdirectory
# thriftz target depends on zlibstatic which is not in thrift's export set
set(_THRIFT_MACROS "${_THRIFT_SRC}/build/cmake/ThriftMacros.cmake")
if(EXISTS "${_THRIFT_MACROS}")
    file(READ "${_THRIFT_MACROS}" _MACROS_CONTENT)
    if(_MACROS_CONTENT MATCHES "export\\(EXPORT")
        string(REGEX REPLACE "export\\(EXPORT[^)]*\\)" "# export disabled for source build" _MACROS_CONTENT "${_MACROS_CONTENT}")
        file(WRITE "${_THRIFT_MACROS}" "${_MACROS_CONTENT}")
    endif()
endif()
# Pre-set Boost for thrift — thrift needs boost headers
set(Boost_INCLUDE_DIR "${TP_SOURCE_DIR}/boost_1_81_0" CACHE PATH "" FORCE)
set(Boost_INCLUDE_DIRS "${TP_SOURCE_DIR}/boost_1_81_0" CACHE PATH "" FORCE)
set(BOOST_INCLUDEDIR "${TP_SOURCE_DIR}/boost_1_81_0" CACHE PATH "" FORCE)
set(BOOST_ROOT "${TP_SOURCE_DIR}/boost_1_81_0" CACHE PATH "" FORCE)
set(Boost_FOUND TRUE CACHE BOOL "" FORCE)
set(Boost_NO_SYSTEM_PATHS ON CACHE BOOL "" FORCE)


# Pre-set Libevent for thrift — thrift calls find_package(Libevent REQUIRED)
# Our libevent is built via add_subdirectory, so we pre-set the variables
set(LIBEVENT_FOUND TRUE CACHE BOOL "" FORCE)
set(Libevent_FOUND TRUE CACHE BOOL "" FORCE)
if(TARGET event_core_static)
    set(LIBEVENT_LIBRARIES event_core_static event_extra_static CACHE STRING "" FORCE)
    set(LIBEVENT_INCLUDE_DIRS
        "${TP_SOURCE_DIR}/libevent-release-2.1.12-stable/include"
        "${CMAKE_CURRENT_BINARY_DIR}/libevent/include"
        CACHE STRING "" FORCE)
endif()

add_subdirectory(${_THRIFT_SRC} ${CMAKE_CURRENT_BINARY_DIR}/thrift EXCLUDE_FROM_ALL)
if(TARGET thrift_static)
    add_library(thrift ALIAS thrift_static)
    target_include_directories(thrift_static PRIVATE
        ${TP_SOURCE_DIR}/boost_1_81_0
    )
endif()
if(TARGET thriftnb_static)
    add_library(thriftnb ALIAS thriftnb_static)
endif()
