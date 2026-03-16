# bzip2 - simple makefile-based, compile manually
set(BZIP2_SRC ${TP_SOURCE_DIR}/bzip2-1.0.8)
add_library(libbz2 STATIC
    ${BZIP2_SRC}/blocksort.c
    ${BZIP2_SRC}/huffman.c
    ${BZIP2_SRC}/crctable.c
    ${BZIP2_SRC}/randtable.c
    ${BZIP2_SRC}/compress.c
    ${BZIP2_SRC}/decompress.c
    ${BZIP2_SRC}/bzlib.c
)
target_include_directories(libbz2 PUBLIC ${BZIP2_SRC})
set_target_properties(libbz2 PROPERTIES POSITION_INDEPENDENT_CODE ON)
