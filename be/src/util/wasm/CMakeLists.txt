cmake_minimum_required(VERSION 3.16.3)

project(wasm_cpp)
set(CMAKE_CXX_STANDARD 17)

set(WASMTIME_LOCAL_PATH "${CMAKE_SOURCE_DIR}/../wasmtime-c-api")
set(WASMTIME_INCLUDE_DIR ${WASMTIME_LOCAL_PATH}/include)
set(WASMTIME_LIBRARY_DIR ${WASMTIME_LOCAL_PATH}/lib)
set(WASMTIME_LIBRARYS 
    libwasmtime.a 
    libwasmtime.so 
    stdc++ 
    pthread 
    dl
)

include_directories(${WASMTIME_INCLUDE_DIR})
link_directories(${WASMTIME_LIBRARY_DIR})

# TestWasm
add_executable(
    wasm_test 
    wasm_test.cpp
)

target_include_directories(wasm_test INTERFACE ${WASMTIME_LIBRARY_DIR})
target_link_libraries(wasm_test ${WASMTIME_LIBRARYS})

set(
    WASM_MANAGER_SRC
    wasm_manager.cpp
)

add_library(
        wasm_manager_obj
        ${WASM_MANAGER_SRC}
)

add_executable(
    wasm_manager_test
    wasm_manager_test.cpp
    $<TARGET_OBJECTS:wasm_manager_obj>
)

target_include_directories(wasm_manager_test INTERFACE ${WASMTIME_LIBRARY_DIR})
target_link_libraries(wasm_manager_test ${WASMTIME_LIBRARYS})