# lzo2 - autoconf based, compile source directly
set(LZO2_SRC ${TP_SOURCE_DIR}/lzo-2.10)
file(GLOB LZO2_SOURCES ${LZO2_SRC}/src/*.c)
add_library(lzo2 STATIC ${LZO2_SOURCES})
target_include_directories(lzo2 PUBLIC ${LZO2_SRC}/include)
target_compile_definitions(lzo2 PRIVATE -DLZO_HAVE_CONFIG_H=0)
set_target_properties(lzo2 PROPERTIES POSITION_INDEPENDENT_CODE ON)
