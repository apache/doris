# openssl - uses ./Configure (not CMake)
# Build from source using execute_process during cmake configure phase
set(OPENSSL_SRC ${TP_SOURCE_DIR}/openssl-OpenSSL_1_1_1s)
set(OPENSSL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl)

if(NOT EXISTS "${OPENSSL_BUILD_DIR}/libssl.a")
    message(STATUS "[contrib] Building openssl from source...")
    file(MAKE_DIRECTORY ${OPENSSL_BUILD_DIR})
    execute_process(
        COMMAND ${OPENSSL_SRC}/Configure
            --prefix=${OPENSSL_BUILD_DIR}
            --openssldir=${OPENSSL_BUILD_DIR}
            --libdir=lib
            no-shared
            no-tests
            -fPIC
            linux-x86_64
        WORKING_DIRECTORY ${OPENSSL_BUILD_DIR}
        RESULT_VARIABLE OPENSSL_CONF_RESULT
        OUTPUT_QUIET
    )
    if(NOT OPENSSL_CONF_RESULT EQUAL 0)
        message(FATAL_ERROR "openssl Configure failed")
    endif()

    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND make -j${NPROC}
        WORKING_DIRECTORY ${OPENSSL_BUILD_DIR}
        RESULT_VARIABLE OPENSSL_BUILD_RESULT
        OUTPUT_QUIET
    )
    if(NOT OPENSSL_BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "openssl build failed")
    endif()
    # Note: make install_sw skipped because --prefix == WORKING_DIRECTORY
    # causes "same file" error. libssl.a and libcrypto.a are in build root.
    message(STATUS "[contrib] openssl build complete")
endif()

# libs are in build root (not lib/ subdir) after make
add_library(openssl STATIC IMPORTED GLOBAL)
set_target_properties(openssl PROPERTIES IMPORTED_LOCATION "${OPENSSL_BUILD_DIR}/libssl.a")
target_include_directories(openssl INTERFACE ${OPENSSL_BUILD_DIR}/include)

add_library(crypto STATIC IMPORTED GLOBAL)
set_target_properties(crypto PROPERTIES IMPORTED_LOCATION "${OPENSSL_BUILD_DIR}/libcrypto.a")
target_include_directories(crypto INTERFACE ${OPENSSL_BUILD_DIR}/include)

# Create OpenSSL::SSL / OpenSSL::Crypto targets for libs that use find_package(OpenSSL)
if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${OPENSSL_BUILD_DIR}/libssl.a"
        INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_BUILD_DIR}/include"
    )
endif()
if(NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${OPENSSL_BUILD_DIR}/libcrypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_BUILD_DIR}/include"
    )
endif()
set(OPENSSL_ROOT_DIR "${OPENSSL_BUILD_DIR}" CACHE PATH "" FORCE)
set(OPENSSL_INCLUDE_DIR "${OPENSSL_BUILD_DIR}/include" CACHE PATH "" FORCE)
set(OPENSSL_SSL_LIBRARY "${OPENSSL_BUILD_DIR}/libssl.a" CACHE FILEPATH "" FORCE)
set(OPENSSL_CRYPTO_LIBRARY "${OPENSSL_BUILD_DIR}/libcrypto.a" CACHE FILEPATH "" FORCE)
set(OPENSSL_FOUND TRUE)
