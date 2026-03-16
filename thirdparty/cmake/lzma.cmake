# lzma (xz-utils) - no CMakeLists.txt, use execute_process
set(_LZMA_SRC "${TP_SOURCE_DIR}/liblzma-master")
set(_LZMA_BUILD "${CMAKE_CURRENT_BINARY_DIR}/lzma_build")
set(_LZMA_INSTALL "${CMAKE_CURRENT_BINARY_DIR}/lzma")

if(NOT EXISTS "${_LZMA_INSTALL}/lib/liblzma.a")
    file(MAKE_DIRECTORY ${_LZMA_BUILD})
    execute_process(
        COMMAND ${_LZMA_SRC}/autogen.sh
        WORKING_DIRECTORY ${_LZMA_SRC}
        OUTPUT_QUIET ERROR_QUIET
    )
    execute_process(
        COMMAND ${_LZMA_SRC}/configure
            --prefix=${_LZMA_INSTALL}
            --disable-shared
            --enable-static
            --disable-xz
            --disable-xzdec
            --disable-lzmadec
            --disable-lzmainfo
            --disable-scripts
            --disable-doc
        WORKING_DIRECTORY ${_LZMA_BUILD}
        OUTPUT_QUIET ERROR_QUIET
        RESULT_VARIABLE _LZMA_CONF_RES
    )
    if(NOT _LZMA_CONF_RES EQUAL 0)
        message(WARNING "[contrib] liblzma configure failed (${_LZMA_CONF_RES}), skipping")
    else()
        execute_process(
            COMMAND make -j8
            WORKING_DIRECTORY ${_LZMA_BUILD}
            OUTPUT_QUIET ERROR_QUIET
        )
        execute_process(
            COMMAND make install
            WORKING_DIRECTORY ${_LZMA_BUILD}
            OUTPUT_QUIET ERROR_QUIET
        )
    endif()
endif()

if(EXISTS "${_LZMA_INSTALL}/lib/liblzma.a")
    add_library(lzma STATIC IMPORTED GLOBAL)
    set_target_properties(lzma PROPERTIES
        IMPORTED_LOCATION "${_LZMA_INSTALL}/lib/liblzma.a"
        INTERFACE_INCLUDE_DIRECTORIES "${_LZMA_INSTALL}/include"
    )
endif()
