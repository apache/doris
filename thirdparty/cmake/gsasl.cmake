# libgsasl - autoconf based, build from source
set(GSASL_SRC ${TP_SOURCE_DIR}/libgsasl-1.8.0)
set(GSASL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/gsasl)

if(NOT EXISTS "${GSASL_BUILD_DIR}/lib/libgsasl.a")
    message(STATUS "[contrib] Building gsasl from source...")
    file(MAKE_DIRECTORY ${GSASL_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${GSASL_SRC}/configure
            --prefix=${GSASL_BUILD_DIR}
            --enable-static --disable-shared
            --with-gssapi-impl=mit --with-pic CFLAGS=-fPIC
        WORKING_DIRECTORY ${GSASL_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "gsasl configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${GSASL_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${GSASL_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] gsasl build complete")
endif()

add_library(gsasl STATIC IMPORTED GLOBAL)
set_target_properties(gsasl PROPERTIES IMPORTED_LOCATION "${GSASL_BUILD_DIR}/lib/libgsasl.a")
target_include_directories(gsasl INTERFACE ${GSASL_BUILD_DIR}/include)
