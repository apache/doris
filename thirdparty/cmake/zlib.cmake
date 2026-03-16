# zlib
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SKIP_INSTALL_ALL ON CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/zlib-1.3.1 ${CMAKE_CURRENT_BINARY_DIR}/zlib EXCLUDE_FROM_ALL)
# zlib's CMakeLists exports 'zlibstatic' target, alias it
if(TARGET zlibstatic)
    add_library(z ALIAS zlibstatic)
    add_library(libz ALIAS zlibstatic)
endif()
