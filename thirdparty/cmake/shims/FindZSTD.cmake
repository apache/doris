# Find module shim for ZSTD (uppercase, used by arrow find_package(ZSTD))
# Redirects to our source-built zstd (libzstd_static target)
if(TARGET libzstd_static)
    get_target_property(_zstd_src_dir libzstd_static SOURCE_DIR)
    # SOURCE_DIR is .../zstd-1.5.7/build/cmake, zstd.h is in .../zstd-1.5.7/lib/
    get_filename_component(_zstd_root "${_zstd_src_dir}" DIRECTORY)  # build
    get_filename_component(_zstd_root "${_zstd_root}" DIRECTORY)     # zstd-1.5.7
    set(ZSTD_FOUND TRUE)
    set(ZSTD_INCLUDE_DIR "${_zstd_root}/lib")
    # Use target name so cmake can track build dependencies correctly
    set(ZSTD_LIBRARY libzstd_static)
    set(ZSTD_STATIC_LIB libzstd_static)
    set(ZSTD_LIBRARIES libzstd_static)
    message(STATUS "Found the zstd header: ${ZSTD_INCLUDE_DIR}/zstd.h")
    message(STATUS "Found the zstd library: libzstd_static (target)")
    if(NOT TARGET zstd::libzstd_static)
        add_library(zstd::libzstd_static ALIAS libzstd_static)
    endif()
endif()
