# minizip - from zlib contrib
# minizip provides unzOpen/unzClose etc. used by user_function_cache.cpp
set(MINIZIP_SRC ${TP_SOURCE_DIR}/zlib-1.3.1/contrib/minizip)

add_library(minizip STATIC
    ${MINIZIP_SRC}/ioapi.c
    ${MINIZIP_SRC}/unzip.c
    ${MINIZIP_SRC}/zip.c
    ${MINIZIP_SRC}/mztools.c
)
target_include_directories(minizip PUBLIC ${MINIZIP_SRC})
# minizip depends on zlib
if(TARGET zlibstatic)
    target_link_libraries(minizip PRIVATE zlibstatic)
    target_include_directories(minizip PRIVATE
        ${TP_SOURCE_DIR}/zlib-1.3.1
        ${CMAKE_CURRENT_BINARY_DIR}/zlib)
endif()
set_target_properties(minizip PROPERTIES POSITION_INDEPENDENT_CODE ON)
