# s2geometry
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(WITH_PYTHON OFF CACHE BOOL "" FORCE)
set(S2_USE_SYSTEM_INCLUDES OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(WITH_GLOG OFF CACHE BOOL "" FORCE)

# s2geometry uses find_package(absl) and find_package(OpenSSL), but we build
# them via add_subdirectory. Create shim config files so find_package succeeds.
set(_S2_SHIM_DIR ${CMAKE_CURRENT_BINARY_DIR}/s2_cmake_shims)
file(MAKE_DIRECTORY ${_S2_SHIM_DIR})

# Shim for absl - targets already exist from add_subdirectory
file(WRITE ${_S2_SHIM_DIR}/abslConfig.cmake "# Shim: absl targets provided by add_subdirectory\n")
# Shim for OpenSSL
file(WRITE ${_S2_SHIM_DIR}/OpenSSLConfig.cmake
    "# Shim: OpenSSL targets provided by our openssl.cmake\n"
    "set(OPENSSL_FOUND TRUE)\n"
    "set(OPENSSL_INCLUDE_DIR \"${CMAKE_CURRENT_BINARY_DIR}/openssl/include\")\n"
    "set(OPENSSL_CRYPTO_LIBRARY \"${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libcrypto.a\")\n"
    "set(OPENSSL_SSL_LIBRARY \"${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libssl.a\")\n"
    "if(NOT TARGET OpenSSL::SSL)\n"
    "  add_library(OpenSSL::SSL STATIC IMPORTED)\n"
    "  set_target_properties(OpenSSL::SSL PROPERTIES IMPORTED_LOCATION \"\${OPENSSL_SSL_LIBRARY}\")\n"
    "  target_include_directories(OpenSSL::SSL INTERFACE \"\${OPENSSL_INCLUDE_DIR}\")\n"
    "endif()\n"
    "if(NOT TARGET OpenSSL::Crypto)\n"
    "  add_library(OpenSSL::Crypto STATIC IMPORTED)\n"
    "  set_target_properties(OpenSSL::Crypto PROPERTIES IMPORTED_LOCATION \"\${OPENSSL_CRYPTO_LIBRARY}\")\n"
    "  target_include_directories(OpenSSL::Crypto INTERFACE \"\${OPENSSL_INCLUDE_DIR}\")\n"
    "endif()\n"
)

# Prepend the shim directory to CMAKE_MODULE_PATH so find_package finds our shims
list(PREPEND CMAKE_PREFIX_PATH ${_S2_SHIM_DIR})

add_subdirectory(${TP_SOURCE_DIR}/s2geometry-0.10.0 ${CMAKE_CURRENT_BINARY_DIR}/s2geometry EXCLUDE_FROM_ALL)
