# bitshuffle - with AVX2/AVX512 variant builds (mimics build-thirdparty.sh)
set(BITSHUFFLE_SRC ${TP_SOURCE_DIR}/bitshuffle-0.5.1)
set(BITSHUFFLE_SOURCES
    ${BITSHUFFLE_SRC}/src/bitshuffle_core.c
    ${BITSHUFFLE_SRC}/src/bitshuffle.c
    ${BITSHUFFLE_SRC}/src/iochain.c
)
set(BITSHUFFLE_INCLUDES ${BITSHUFFLE_SRC}/src ${BITSHUFFLE_SRC}/lz4)

# --- Default (no special arch flags) ---
add_library(bitshuffle_default STATIC ${BITSHUFFLE_SOURCES})
target_include_directories(bitshuffle_default PRIVATE ${BITSHUFFLE_INCLUDES})
set_target_properties(bitshuffle_default PROPERTIES POSITION_INDEPENDENT_CODE ON C_STANDARD 99)

if(ARCH_AMD64)
    # --- AVX2 variant ---
    add_library(bitshuffle_avx2_lib STATIC ${BITSHUFFLE_SOURCES})
    target_include_directories(bitshuffle_avx2_lib PRIVATE ${BITSHUFFLE_INCLUDES})
    target_compile_options(bitshuffle_avx2_lib PRIVATE -mavx2)
    set_target_properties(bitshuffle_avx2_lib PROPERTIES POSITION_INDEPENDENT_CODE ON C_STANDARD 99)

    # --- AVX512 variant ---
    add_library(bitshuffle_avx512_lib STATIC ${BITSHUFFLE_SOURCES})
    target_include_directories(bitshuffle_avx512_lib PRIVATE ${BITSHUFFLE_INCLUDES})
    target_compile_options(bitshuffle_avx512_lib PRIVATE -mavx512bw -mavx512f)
    set_target_properties(bitshuffle_avx512_lib PROPERTIES POSITION_INDEPENDENT_CODE ON C_STANDARD 99)

    set(BS_WORK ${CMAKE_CURRENT_BINARY_DIR}/bitshuffle_work)
    set(BITSHUFFLE_COMBINED_LIB ${CMAKE_CURRENT_BINARY_DIR}/libbitshuffle_combined.a)

    # Use file(GENERATE) so $<TARGET_FILE:...> generator expressions are expanded
    file(GENERATE OUTPUT ${BS_WORK}/merge_bitshuffle.sh CONTENT
"#!/bin/bash
set -e
WORK='${BS_WORK}'
DEFAULT_LIB='$<TARGET_FILE:bitshuffle_default>'
AVX2_LIB='$<TARGET_FILE:bitshuffle_avx2_lib>'
AVX512_LIB='$<TARGET_FILE:bitshuffle_avx512_lib>'
OUTPUT='${BITSHUFFLE_COMBINED_LIB}'

mkdir -p \"$WORK/default\" \"$WORK/avx2\" \"$WORK/avx512\"
cd \"$WORK/default\" && rm -f *.o && ar x \"$DEFAULT_LIB\"
cd \"$WORK/avx2\"    && rm -f *.o && ar x \"$AVX2_LIB\"
cd \"$WORK/avx512\"  && rm -f *.o && ar x \"$AVX512_LIB\"

ld -r -o \"$WORK/default_merged.o\" \"$WORK\"/default/*.o
ld -r -o \"$WORK/avx2_tmp.o\" \"$WORK\"/avx2/*.o
ld -r -o \"$WORK/avx512_tmp.o\" \"$WORK\"/avx512/*.o

nm --defined-only --extern-only \"$WORK/avx2_tmp.o\" | awk '{print $3 \" \" $3 \"_avx2\"}' > \"$WORK/avx2_renames.txt\"
objcopy --redefine-syms=\"$WORK/avx2_renames.txt\" \"$WORK/avx2_tmp.o\" \"$WORK/avx2_merged.o\"

nm --defined-only --extern-only \"$WORK/avx512_tmp.o\" | awk '{print $3 \" \" $3 \"_avx512\"}' > \"$WORK/avx512_renames.txt\"
objcopy --redefine-syms=\"$WORK/avx512_renames.txt\" \"$WORK/avx512_tmp.o\" \"$WORK/avx512_merged.o\"

rm -f \"$OUTPUT\"
ar rs \"$OUTPUT\" \"$WORK/default_merged.o\" \"$WORK/avx2_merged.o\" \"$WORK/avx512_merged.o\"
")

    add_custom_command(
        OUTPUT ${BITSHUFFLE_COMBINED_LIB}
        COMMAND bash ${BS_WORK}/merge_bitshuffle.sh
        DEPENDS bitshuffle_default bitshuffle_avx2_lib bitshuffle_avx512_lib
        COMMENT "Merging bitshuffle default/avx2/avx512 variants"
    )

    add_custom_target(bitshuffle_combined DEPENDS ${BITSHUFFLE_COMBINED_LIB})

    add_library(bitshuffle STATIC IMPORTED GLOBAL)
    set_target_properties(bitshuffle PROPERTIES
        IMPORTED_LOCATION ${BITSHUFFLE_COMBINED_LIB}
        INTERFACE_INCLUDE_DIRECTORIES "${BITSHUFFLE_SRC}/src"
    )
    add_dependencies(bitshuffle bitshuffle_combined)

else()
    # Non-AMD64: just use the default build
    add_library(bitshuffle ALIAS bitshuffle_default)
    target_include_directories(bitshuffle_default PUBLIC ${BITSHUFFLE_SRC}/src)
endif()
