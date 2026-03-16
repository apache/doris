# gperftools - autoconf, build from source
set(GPERFTOOLS_SRC ${TP_SOURCE_DIR}/gperftools-2.10)
set(GPERFTOOLS_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/gperftools)

if(NOT EXISTS "${GPERFTOOLS_BUILD_DIR}/lib/libtcmalloc.a")
    message(STATUS "[contrib] Building gperftools from source...")
    file(MAKE_DIRECTORY ${GPERFTOOLS_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${GPERFTOOLS_SRC}/configure
            --prefix=${GPERFTOOLS_BUILD_DIR}
            --enable-static --disable-shared
            --enable-frame-pointers --with-pic
            CFLAGS=-fPIC CXXFLAGS=-fPIC
        WORKING_DIRECTORY ${GPERFTOOLS_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "gperftools configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${GPERFTOOLS_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${GPERFTOOLS_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] gperftools build complete")
endif()

add_library(pprof STATIC IMPORTED GLOBAL)
set_target_properties(pprof PROPERTIES IMPORTED_LOCATION "${GPERFTOOLS_BUILD_DIR}/lib/libprofiler.a")
target_include_directories(pprof INTERFACE ${GPERFTOOLS_BUILD_DIR}/include)
add_library(tcmalloc STATIC IMPORTED GLOBAL)
set_target_properties(tcmalloc PROPERTIES IMPORTED_LOCATION "${GPERFTOOLS_BUILD_DIR}/lib/libtcmalloc.a")
target_include_directories(tcmalloc INTERFACE ${GPERFTOOLS_BUILD_DIR}/include)
