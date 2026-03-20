# ICU - autoconf based, build from source
set(ICU_SRC ${TP_SOURCE_DIR}/icu-release-69-1/icu4c)
set(ICU_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/icu)

if(NOT EXISTS "${ICU_BUILD_DIR}/lib/libicuuc.a")
    message(STATUS "[contrib] Building ICU from source...")
    file(MAKE_DIRECTORY ${ICU_BUILD_DIR})
    include(ProcessorCount)
    ProcessorCount(NPROC)
    execute_process(
        COMMAND ${ICU_SRC}/source/configure
            --prefix=${ICU_BUILD_DIR}
            --enable-static --disable-shared
            --disable-tests --disable-samples --disable-extras
            --with-data-packaging=static
            CFLAGS=-fPIC CXXFLAGS=-fPIC
        WORKING_DIRECTORY ${ICU_BUILD_DIR}
        RESULT_VARIABLE RESULT
        OUTPUT_QUIET
    )
    if(NOT RESULT EQUAL 0)
        message(FATAL_ERROR "ICU configure failed")
    endif()
    execute_process(COMMAND make -j${NPROC} WORKING_DIRECTORY ${ICU_BUILD_DIR} OUTPUT_QUIET)
    execute_process(COMMAND make install WORKING_DIRECTORY ${ICU_BUILD_DIR} OUTPUT_QUIET)
    message(STATUS "[contrib] ICU build complete")
endif()

add_library(icuuc STATIC IMPORTED GLOBAL)
set_target_properties(icuuc PROPERTIES IMPORTED_LOCATION "${ICU_BUILD_DIR}/lib/libicuuc.a")
target_include_directories(icuuc INTERFACE ${ICU_BUILD_DIR}/include)
add_library(icui18n STATIC IMPORTED GLOBAL)
set_target_properties(icui18n PROPERTIES IMPORTED_LOCATION "${ICU_BUILD_DIR}/lib/libicui18n.a")
add_library(icudata STATIC IMPORTED GLOBAL)
set_target_properties(icudata PROPERTIES IMPORTED_LOCATION "${ICU_BUILD_DIR}/lib/libicudata.a")
