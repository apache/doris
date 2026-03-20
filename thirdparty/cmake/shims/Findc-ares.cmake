# Find module shim for c-ares
# Provides variables expected by grpc's Findc-ares.cmake
# Since c-ares is already added via add_subdirectory, we just need to
# prevent grpc from trying to create targets that already exist.

# c-ares add_subdirectory creates targets: c-ares::cares, c-ares::cares_static
# We set the found variables so grpc's Findc-ares doesn't try to create them again.
if(TARGET c-ares::cares OR TARGET c-ares::cares_static)
    set(c-ares_FOUND TRUE)
    # Get the include dir from the target
    get_target_property(_CARES_INC c-ares INTERFACE_INCLUDE_DIRECTORIES)
    if(_CARES_INC)
        set(c-ares_INCLUDE_DIR ${_CARES_INC})
    endif()
    # Provide library variable pointing to the static lib
    set(c-ares_LIBRARY "c-ares::cares")
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(c-ares DEFAULT_MSG c-ares_LIBRARY c-ares_INCLUDE_DIR)
endif()
