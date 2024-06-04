
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/resource-manager")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/resource-manager")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

add_library(ResourceManager
    resource_manager.cpp
)
