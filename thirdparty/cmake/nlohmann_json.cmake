# nlohmann_json - header only
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP_SOURCE_DIR}/json-3.10.1 ${CMAKE_CURRENT_BINARY_DIR}/nlohmann_json EXCLUDE_FROM_ALL)
