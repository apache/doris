# Ali SDK (Alibaba Cloud SDK) - has CMake, build from source
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl CACHE PATH "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/aliyun-openapi-cpp-sdk-1.36.1586 ${CMAKE_CURRENT_BINARY_DIR}/ali_sdk EXCLUDE_FROM_ALL)
