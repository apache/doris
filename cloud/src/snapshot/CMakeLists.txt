set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/snapshot")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/snapshot")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")
add_library(Snapshot snapshot_manager.cpp)
