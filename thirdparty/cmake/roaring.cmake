# CRoaring
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_LTO OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/CRoaring-2.1.2 ${CMAKE_CURRENT_BINARY_DIR}/roaring EXCLUDE_FROM_ALL)
# Mark CRoaring includes as SYSTEM to suppress -Wundef/-Wshorten-64-to-32 warnings
if(TARGET roaring)
    get_target_property(_roaring_inc roaring INTERFACE_INCLUDE_DIRECTORIES)
    if(_roaring_inc)
        set_target_properties(roaring PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "")
        target_include_directories(roaring SYSTEM INTERFACE ${_roaring_inc})
    endif()
endif()
