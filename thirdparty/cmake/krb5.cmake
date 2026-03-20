# krb5 - autoconf based, build from source
# Reference: build-thirdparty.sh build_krb5()
set(KRB5_SRC ${TP_SOURCE_DIR}/krb5-1.19)
set(KRB5_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/krb5)

if(NOT EXISTS "${KRB5_BUILD_DIR}/lib/libkrb5.a")
    message(STATUS "[contrib] Building krb5 from source...")
    file(MAKE_DIRECTORY ${KRB5_BUILD_DIR})

    include(ProcessorCount)
    ProcessorCount(NPROC)

    # CFLAGS from build-thirdparty.sh: -fcommon -fPIC -std=gnu89
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env
            "CFLAGS=-fcommon -fPIC -std=gnu89"
            "CXXFLAGS=-fPIC"
        ${KRB5_SRC}/src/configure
            --prefix=${KRB5_BUILD_DIR}
            --enable-static
            --disable-shared
            --without-keyutils
        WORKING_DIRECTORY ${KRB5_BUILD_DIR}
        RESULT_VARIABLE KRB5_CONF_RESULT
    )
    if(NOT KRB5_CONF_RESULT EQUAL 0)
        message(FATAL_ERROR "krb5 configure failed (${KRB5_CONF_RESULT})")
    endif()
    execute_process(
        COMMAND make -j${NPROC}
        WORKING_DIRECTORY ${KRB5_BUILD_DIR}
        RESULT_VARIABLE KRB5_BUILD_RESULT
    )
    if(NOT KRB5_BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "krb5 build failed (${KRB5_BUILD_RESULT})")
    endif()
    # Skip make install - it fails with "same file" error when --prefix == WORKING_DIRECTORY.
    # The .a files are already in ${KRB5_BUILD_DIR}/lib/ after make.
    message(STATUS "[contrib] krb5 build complete")
endif()

foreach(_lib krb5support krb5 com_err gssapi_krb5 k5crypto)
    add_library(${_lib} STATIC IMPORTED GLOBAL)
    set_target_properties(${_lib} PROPERTIES IMPORTED_LOCATION "${KRB5_BUILD_DIR}/lib/lib${_lib}.a")
    target_include_directories(${_lib} INTERFACE ${KRB5_BUILD_DIR}/include)
endforeach()
