# Find module shim for zlib (lowercase, used by rocksdb)
# Delegates to FindZLIB.cmake which handles the actual target detection
if(TARGET zlibstatic)
    get_target_property(_zlib_src_dir zlibstatic SOURCE_DIR)
    get_target_property(_zlib_bin_dir zlibstatic BINARY_DIR)
    set(ZLIB_FOUND TRUE)
    set(zlib_FOUND TRUE)
    set(ZLIB_INCLUDE_DIR "${_zlib_src_dir}")
    set(ZLIB_INCLUDE_DIRS "${_zlib_src_dir}" "${_zlib_bin_dir}")
    set(ZLIB_LIBRARIES zlibstatic)
    set(ZLIB_LIBRARY zlibstatic)
    if(NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB INTERFACE IMPORTED GLOBAL)
        set_target_properties(ZLIB::ZLIB PROPERTIES
            INTERFACE_LINK_LIBRARIES zlibstatic
            INTERFACE_INCLUDE_DIRECTORIES "${_zlib_src_dir};${_zlib_bin_dir}"
        )
    endif()
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(zlib DEFAULT_MSG ZLIB_LIBRARIES ZLIB_INCLUDE_DIR)
endif()
