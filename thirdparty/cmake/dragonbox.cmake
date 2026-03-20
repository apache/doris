# dragonbox - header + source
set(DRAGONBOX_SRC ${TP_SOURCE_DIR}/dragonbox-1.1.3)
add_library(dragonbox INTERFACE)
target_include_directories(dragonbox INTERFACE ${DRAGONBOX_SRC}/include)
