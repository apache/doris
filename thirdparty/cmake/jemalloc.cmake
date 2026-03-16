# jemalloc - autoconf + heavily customized, build from source
set(JEMALLOC_SRC ${TP_SOURCE_DIR}/jemalloc-5.3.0)
set(JEMALLOC_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/jemalloc)

if(NOT EXISTS "${JEMALLOC_BUILD_DIR}/lib/libjemalloc_doris.a")
    message(STATUS "[contrib] Building jemalloc from source...")
    file(MAKE_DIRECTORY ${JEMALLOC_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${JEMALLOC_SRC}/configure
            --prefix=${JEMALLOC_BUILD_DIR}
            --with-jemalloc-prefix=je --with-install-suffix=_doris
            --enable-prof --disable-cxx --disable-libdl
            --disable-shared --enable-static CFLAGS=-fPIC
        WORKING_DIRECTORY ${JEMALLOC_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "jemalloc configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${JEMALLOC_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${JEMALLOC_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] jemalloc build complete")
endif()

add_library(jemalloc STATIC IMPORTED GLOBAL)
set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_BUILD_DIR}/lib/libjemalloc_doris.a")
target_include_directories(jemalloc INTERFACE ${JEMALLOC_BUILD_DIR}/include)
