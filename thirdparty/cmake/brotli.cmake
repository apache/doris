# brotli
set(BROTLI_DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(BROTLI_BUNDLED_MODE ON CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/brotli-1.0.9 ${CMAKE_CURRENT_BINARY_DIR}/brotli EXCLUDE_FROM_ALL)
# In bundled mode, brotli creates brotlicommon, brotlidec, brotlienc targets directly.
# Only alias from -static suffix if the short names don't already exist.
foreach(_blib brotlicommon brotlidec brotlienc)
    if(NOT TARGET ${_blib} AND TARGET ${_blib}-static)
        add_library(${_blib} ALIAS ${_blib}-static)
    endif()
endforeach()
