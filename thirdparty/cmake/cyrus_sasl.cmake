# cyrus-sasl - autoconf based, build from source
# Reference: build-thirdparty.sh build_cyrus_sasl()
set(CYRUS_SASL_SRC ${TP_SOURCE_DIR}/cyrus-sasl-2.1.27)
set(CYRUS_SASL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/cyrus-sasl)
set(KRB5_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/krb5)
set(OPENSSL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl)

if(NOT EXISTS "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")
    message(STATUS "[contrib] Building cyrus-sasl from source...")
    file(MAKE_DIRECTORY ${CYRUS_SASL_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)

    # CFLAGS/LIBS from build-thirdparty.sh
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env
            "CFLAGS=-fPIC -std=gnu89 -Wno-implicit-function-declaration"
            "CPPFLAGS=-I${OPENSSL_BUILD_DIR}/include -I${KRB5_BUILD_DIR}/include"
            "LDFLAGS=-L${OPENSSL_BUILD_DIR} -L${KRB5_BUILD_DIR}/lib"
            "LIBS=-lcrypto"
        ${CYRUS_SASL_SRC}/configure
            --prefix=${CYRUS_SASL_BUILD_DIR}
            --enable-static --enable-shared=no
            --with-openssl=${OPENSSL_BUILD_DIR}
            --with-pic
            --enable-gssapi=${KRB5_BUILD_DIR}
            --with-gss_impl=mit
            --with-dblib=none
        WORKING_DIRECTORY ${CYRUS_SASL_BUILD_DIR}
        RESULT_VARIABLE RESULT
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "cyrus-sasl configure failed (${RESULT})")
    endif()
    execute_process(
        COMMAND make -j${NPROC}
        WORKING_DIRECTORY ${CYRUS_SASL_BUILD_DIR}
        RESULT_VARIABLE BUILD_RESULT
    )
    if(NOT BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "cyrus-sasl build failed (${BUILD_RESULT})")
    endif()
    # Skip make install - may fail with "same file" error.
    # The .a file is in lib/.libs/libsasl2.a after make.
    message(STATUS "[contrib] cyrus-sasl build complete")
endif()

add_library(cyrus-sasl STATIC IMPORTED GLOBAL)
# libsasl2.a is in lib/.libs/ after make (autoconf libtool pattern)
if(EXISTS "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
elseif(EXISTS "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/.libs/libsasl2.a")
else()
    message(WARNING "[contrib] cyrus-sasl library not found, using placeholder path")
    set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION "${CYRUS_SASL_BUILD_DIR}/lib/libsasl2.a")
endif()
target_include_directories(cyrus-sasl INTERFACE ${CYRUS_SASL_BUILD_DIR}/include)
