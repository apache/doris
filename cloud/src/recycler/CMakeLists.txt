
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/recycler")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

set (SRC_LIST
    checker.cpp
    recycler.cpp
    recycler_service.cpp
    s3_accessor.cpp
    util.cpp
    white_black_list.cpp
)

if(BUILD_CHECK_META STREQUAL "ON")
    set (SRC_LIST ${SRC_LIST} meta_checker.cpp)
endif ()

add_library(Recycler ${SRC_LIST})
