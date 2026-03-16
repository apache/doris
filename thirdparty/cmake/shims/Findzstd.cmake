# Find module shim for zstd (lowercase, used by rocksdb)
if(TARGET libzstd_static)
    get_target_property(_zstd_src_dir libzstd_static SOURCE_DIR)
    # SOURCE_DIR is .../zstd-1.5.7/build/cmake, headers in .../zstd-1.5.7/lib/
    get_filename_component(_zstd_root "${_zstd_src_dir}" DIRECTORY)  # build
    get_filename_component(_zstd_root "${_zstd_root}" DIRECTORY)     # zstd-1.5.7
    set(ZSTD_FOUND TRUE)
    set(zstd_FOUND TRUE)
    set(ZSTD_INCLUDE_DIR "${_zstd_root}/lib")
    set(ZSTD_INCLUDE_DIRS "${_zstd_root}/lib")
    set(ZSTD_LIBRARIES libzstd_static)
    set(ZSTD_LIBRARY libzstd_static)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zstd DEFAULT_MSG ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)
    if(NOT TARGET zstd::libzstd_static)
        add_library(zstd::libzstd_static ALIAS libzstd_static)
    endif()
endif()
