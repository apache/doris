# libuuid - autoconf, build from source
set(UUID_SRC ${TP_SOURCE_DIR}/libuuid-1.0.3)
set(UUID_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/libuuid)

if(NOT EXISTS "${UUID_BUILD_DIR}/lib/libuuid.a")
    message(STATUS "[contrib] Building libuuid from source...")
    file(MAKE_DIRECTORY ${UUID_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${UUID_SRC}/configure
            --prefix=${UUID_BUILD_DIR}
            --enable-static --disable-shared --with-pic CFLAGS=-fPIC
        WORKING_DIRECTORY ${UUID_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "libuuid configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${UUID_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${UUID_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] libuuid build complete")
endif()

add_library(libuuid STATIC IMPORTED GLOBAL)
set_target_properties(libuuid PROPERTIES IMPORTED_LOCATION "${UUID_BUILD_DIR}/lib/libuuid.a")
target_include_directories(libuuid INTERFACE ${UUID_BUILD_DIR}/include)
