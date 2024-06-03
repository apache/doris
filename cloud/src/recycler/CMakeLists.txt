
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

file(GLOB_RECURSE SRC_LIST CONFIGURE_DEPENDS *.cpp)

if(BUILD_CHECK_META STREQUAL "OFF")
    list(REMOVE_ITEM SRC_LIST ${CMAKE_CURRENT_SOURCE_DIR}/meta_checker.cpp)
endif ()

add_library(Recycler ${SRC_LIST})
