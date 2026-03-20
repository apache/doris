# Find module shim for ZLIB
# Creates ZLIB::ZLIB as an INTERFACE IMPORTED library that links to zlibstatic.
# This works across try_compile boundaries because IMPORTED targets are global.
if(TARGET zlibstatic)
    set(ZLIB_FOUND TRUE)
    get_target_property(_zlib_src_dir zlibstatic SOURCE_DIR)
    get_target_property(_zlib_bin_dir zlibstatic BINARY_DIR)
    set(ZLIB_INCLUDE_DIR "${_zlib_src_dir}")
    set(ZLIB_INCLUDE_DIRS "${_zlib_src_dir}" "${_zlib_bin_dir}")
    set(ZLIB_LIBRARIES zlibstatic)
    set(ZLIB_VERSION_STRING "1.3.1")
    # Point to our static lib -- used by ORC's ThirdpartyToolchain
    set(ZLIB_LIBRARY "${_zlib_bin_dir}/libz.a")
    set(ZLIB_STATIC_LIB "${_zlib_bin_dir}/libz.a")

    if(NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB INTERFACE IMPORTED GLOBAL)
        set_target_properties(ZLIB::ZLIB PROPERTIES
            INTERFACE_LINK_LIBRARIES zlibstatic
            INTERFACE_INCLUDE_DIRECTORIES "${_zlib_src_dir};${_zlib_bin_dir}"
        )
    endif()

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(ZLIB DEFAULT_MSG ZLIB_LIBRARY ZLIB_INCLUDE_DIR)
endif()
