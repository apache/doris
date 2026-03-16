# librdkafka
set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_SSL ON CACHE BOOL "" FORCE)
set(WITH_SASL ON CACHE BOOL "" FORCE)
set(WITH_ZLIB ON CACHE BOOL "" FORCE)
set(WITH_ZSTD ON CACHE BOOL "" FORCE)
set(WITH_LIBDL ON CACHE BOOL "" FORCE)
set(WITH_PLUGINS OFF CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR "${CMAKE_CURRENT_BINARY_DIR}/openssl" CACHE PATH "" FORCE)

# Create OpenSSL::SSL and OpenSSL::Crypto IMPORTED targets if they don't exist
# Use source-built OpenSSL from thirdparty build directory
if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libssl.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()
if(NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libcrypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()

# Pre-set dependency vars so rdkafka cmake can find zstd/lz4/sasl
# Create IMPORTED targets for ZSTD and LZ4 so rdkafka find_package succeeds
# and rdkafka gets correct link dependencies through cmake targets.

# ZSTD — create ZSTD::ZSTD imported target
if(NOT TARGET ZSTD::ZSTD)
    add_library(ZSTD::ZSTD ALIAS libzstd_static)
endif()
set(ZSTD_FOUND TRUE CACHE BOOL "" FORCE)
set(ZSTD_INCLUDE_DIRS "${TP_SOURCE_DIR}/zstd-1.5.7/lib" CACHE PATH "" FORCE)
set(ZSTD_INCLUDE_DIR "${TP_SOURCE_DIR}/zstd-1.5.7/lib" CACHE PATH "" FORCE)

# LZ4 — create LZ4::LZ4 imported target
if(NOT TARGET LZ4::LZ4)
    add_library(LZ4::LZ4 ALIAS lz4_static)
endif()
set(LZ4_FOUND TRUE CACHE BOOL "" FORCE)
set(LZ4_INCLUDE_DIRS "${TP_SOURCE_DIR}/lz4-1.9.4/lib" CACHE PATH "" FORCE)
set(LZ4_INCLUDE_DIR "${TP_SOURCE_DIR}/lz4-1.9.4/lib" CACHE PATH "" FORCE)

# SASL - sasl.h is installed to thirdparty/installed/include by external build
set(SASL_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../thirdparty/installed/include" CACHE PATH "" FORCE)

add_subdirectory(${TP_SOURCE_DIR}/librdkafka-2.11.0 ${CMAKE_CURRENT_BINARY_DIR}/rdkafka EXCLUDE_FROM_ALL)
if(TARGET rdkafka)
    add_library(rdkafka_cpp ALIAS rdkafka++)
    # Link rdkafka against actual zstd/lz4 targets so the libraries are found
    target_link_libraries(rdkafka PRIVATE libzstd_static lz4_static)
    # Add include dirs as PRIVATE to avoid polluting global scope
    target_include_directories(rdkafka PRIVATE
        ${TP_SOURCE_DIR}/zstd-1.5.7/lib
        ${TP_SOURCE_DIR}/lz4-1.9.4/lib
        ${CMAKE_SOURCE_DIR}/../thirdparty/installed/include
    )
endif()
