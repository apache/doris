# libidn - autoconf based, build from source
set(IDN_SRC ${TP_SOURCE_DIR}/libidn-1.38)
set(IDN_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/idn)

if(NOT EXISTS "${IDN_BUILD_DIR}/lib/libidn.a")
    message(STATUS "[contrib] Building libidn from source...")
    file(MAKE_DIRECTORY ${IDN_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${IDN_SRC}/configure
            --prefix=${IDN_BUILD_DIR}
            --enable-static --disable-shared --with-pic CFLAGS=-fPIC
        WORKING_DIRECTORY ${IDN_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "libidn configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${IDN_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${IDN_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] libidn build complete")
endif()

add_library(idn STATIC IMPORTED GLOBAL)
set_target_properties(idn PROPERTIES IMPORTED_LOCATION "${IDN_BUILD_DIR}/lib/libidn.a")
target_include_directories(idn INTERFACE ${IDN_BUILD_DIR}/include)
