# libxml2 - no CMakeLists.txt, use configure/make (autoconf)
set(_XML2_SRC "${TP_SOURCE_DIR}/libxml2-v2.9.10")
set(_XML2_BUILD "${CMAKE_CURRENT_BINARY_DIR}/xml2_build")
set(_XML2_INSTALL "${CMAKE_CURRENT_BINARY_DIR}/xml2")

if(NOT EXISTS "${_XML2_INSTALL}/lib/libxml2.a")
    file(MAKE_DIRECTORY ${_XML2_BUILD})
    execute_process(
        COMMAND ${_XML2_SRC}/configure
            --prefix=${_XML2_INSTALL}
            --disable-shared
            --enable-static
            --without-python
            --without-iconv
            --without-lzma
            --with-zlib
        WORKING_DIRECTORY ${_XML2_BUILD}
        OUTPUT_QUIET ERROR_QUIET
        RESULT_VARIABLE _XML2_CONF_RES
    )
    if(NOT _XML2_CONF_RES EQUAL 0)
        message(WARNING "[contrib] libxml2 configure failed (${_XML2_CONF_RES}), skipping")
    else()
        execute_process(
            COMMAND make -j8
            WORKING_DIRECTORY ${_XML2_BUILD}
            OUTPUT_QUIET ERROR_QUIET
        )
        execute_process(
            COMMAND make install
            WORKING_DIRECTORY ${_XML2_BUILD}
            OUTPUT_QUIET ERROR_QUIET
        )
    endif()
endif()

if(EXISTS "${_XML2_INSTALL}/lib/libxml2.a")
    add_library(xml2 STATIC IMPORTED GLOBAL)
    set_target_properties(xml2 PROPERTIES
        IMPORTED_LOCATION "${_XML2_INSTALL}/lib/libxml2.a"
        INTERFACE_INCLUDE_DIRECTORIES "${_XML2_INSTALL}/include/libxml2"
    )
    # Set variables for find_package(LibXml2)
    set(LIBXML2_FOUND TRUE CACHE BOOL "" FORCE)
    set(LIBXML2_INCLUDE_DIR "${_XML2_INSTALL}/include/libxml2" CACHE PATH "" FORCE)
    set(LIBXML2_LIBRARY "${_XML2_INSTALL}/lib/libxml2.a" CACHE FILEPATH "" FORCE)
    set(LIBXML2_LIBRARIES "${_XML2_INSTALL}/lib/libxml2.a" CACHE STRING "" FORCE)
endif()
