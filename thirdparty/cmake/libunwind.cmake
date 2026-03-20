# libunwind - autoconf based, build from source
set(LIBUNWIND_SRC ${TP_SOURCE_DIR}/libunwind-1.6.2)
set(LIBUNWIND_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/libunwind)

if(NOT EXISTS "${LIBUNWIND_BUILD_DIR}/lib/libunwind.a")
    message(STATUS "[contrib] Building libunwind from source...")
    file(MAKE_DIRECTORY ${LIBUNWIND_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${LIBUNWIND_SRC}/configure
            --prefix=${LIBUNWIND_BUILD_DIR}
            --enable-static --disable-shared
            --disable-minidebuginfo --with-pic CFLAGS=-fPIC
        WORKING_DIRECTORY ${LIBUNWIND_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "libunwind configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${LIBUNWIND_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${LIBUNWIND_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] libunwind build complete")
endif()

add_library(libunwind STATIC IMPORTED GLOBAL)
set_target_properties(libunwind PROPERTIES IMPORTED_LOCATION "${LIBUNWIND_BUILD_DIR}/lib/libunwind.a")
target_include_directories(libunwind INTERFACE ${LIBUNWIND_BUILD_DIR}/include)
