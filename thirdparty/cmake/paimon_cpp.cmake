# paimon_cpp - has CMake, build from source
if(ENABLE_PAIMON_CPP AND EXISTS "${TP_SOURCE_DIR}/paimon-cpp")
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
    add_subdirectory(${TP_SOURCE_DIR}/paimon-cpp ${CMAKE_CURRENT_BINARY_DIR}/paimon_cpp EXCLUDE_FROM_ALL)
endif()
