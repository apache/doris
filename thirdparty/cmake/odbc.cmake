# ODBC (unixODBC) - autoconf based, build from source
set(ODBC_SRC ${TP_SOURCE_DIR}/unixODBC-2.3.7)
set(ODBC_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/odbc)

if(NOT EXISTS "${ODBC_BUILD_DIR}/lib/libodbc.a")
    message(STATUS "[contrib] Building unixODBC from source...")
    file(MAKE_DIRECTORY ${ODBC_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E env
            "CFLAGS=-fPIC -Wno-int-conversion -std=gnu89 -Wno-implicit-function-declaration"
        ${ODBC_SRC}/configure
            --prefix=${ODBC_BUILD_DIR}
            --with-included-ltdl
            --enable-static=yes --disable-shared
        WORKING_DIRECTORY ${ODBC_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "unixODBC configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${ODBC_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${ODBC_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] unixODBC build complete")
endif()

add_library(odbc STATIC IMPORTED GLOBAL)
set_target_properties(odbc PROPERTIES IMPORTED_LOCATION "${ODBC_BUILD_DIR}/lib/libodbc.a")
target_include_directories(odbc INTERFACE ${ODBC_BUILD_DIR}/include)
