# grpc
# gRPC has many dependencies and a complex CMake. Use add_subdirectory.
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
# Use "package" provider — our shim Find modules in cmake/shims/ will
# bridge find_package() to our add_subdirectory-built targets
set(gRPC_ZLIB_PROVIDER "package" CACHE STRING "" FORCE)
set(gRPC_CARES_PROVIDER "package" CACHE STRING "" FORCE)
set(gRPC_PROTOBUF_PROVIDER "package" CACHE STRING "" FORCE)
set(gRPC_SSL_PROVIDER "package" CACHE STRING "" FORCE)
set(gRPC_ABSL_PROVIDER "package" CACHE STRING "" FORCE)
set(gRPC_RE2_PROVIDER "package" CACHE STRING "" FORCE)
# Point OpenSSL to our source-built location
set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl CACHE PATH "" FORCE)
set(OPENSSL_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl/include CACHE PATH "" FORCE)
set(OPENSSL_SSL_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libssl.a CACHE FILEPATH "" FORCE)
set(OPENSSL_CRYPTO_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libcrypto.a CACHE FILEPATH "" FORCE)

# Create protobuf:: namespace aliases for gRPC's find_package(Protobuf) path.
# Our protobuf is built via add_subdirectory which creates targets without the
# protobuf:: prefix. gRPC's cmake/protobuf.cmake checks for protobuf::libprotobuf,
# protobuf::libprotoc, and protobuf::protoc.
if(NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf ALIAS libprotobuf)
endif()
if(NOT TARGET protobuf::libprotoc)
    add_library(protobuf::libprotoc ALIAS libprotoc)
    # Set INTERFACE_INCLUDE_DIRECTORIES so gRPC can find well-known .proto files
    set_target_properties(libprotoc PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${TP_SOURCE_DIR}/protobuf-21.11/src")
endif()
if(NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc ALIAS protoc)
endif()
# Pre-set Protobuf_FOUND so find_package(Protobuf) succeeds
set(Protobuf_FOUND TRUE)
set(PROTOBUF_FOUND TRUE)
set(Protobuf_INCLUDE_DIRS "${TP_SOURCE_DIR}/protobuf-21.11/src")
set(PROTOBUF_INCLUDE_DIRS "${TP_SOURCE_DIR}/protobuf-21.11/src")
set(Protobuf_LIBRARIES libprotobuf)
set(PROTOBUF_LIBRARIES libprotobuf)
set(Protobuf_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
set(PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)

add_subdirectory(${TP_SOURCE_DIR}/grpc-1.54.3 ${CMAKE_CURRENT_BINARY_DIR}/grpc EXCLUDE_FROM_ALL)
