# Find module shim for lz4
# lz4 is built via add_subdirectory, target lz4_static exists
if(TARGET lz4_static)
    get_target_property(_lz4_src_dir lz4_static SOURCE_DIR)
    # SOURCE_DIR is .../lz4-1.9.4/build/cmake, headers in .../lz4-1.9.4/lib/
    get_filename_component(_lz4_root "${_lz4_src_dir}" DIRECTORY)
    get_filename_component(_lz4_root "${_lz4_root}" DIRECTORY)
    set(LZ4_FOUND TRUE)
    set(lz4_FOUND TRUE)
    set(LZ4_INCLUDE_DIR "${_lz4_root}/lib")
    set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
    set(LZ4_LIBRARY lz4_static)
    set(LZ4_LIBRARIES lz4_static)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(lz4 DEFAULT_MSG LZ4_LIBRARY LZ4_INCLUDE_DIR)
    if(NOT TARGET LZ4::lz4)
        add_library(LZ4::lz4 ALIAS lz4_static)
    endif()
endif()
