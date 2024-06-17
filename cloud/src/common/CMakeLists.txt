# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/common")

set(COMMON_FILES
    configbase.cpp
    sync_point.cpp
    util.cpp
    logging.cpp
    bvars.cpp
    encryption_util.cpp
    metric.cpp
    kms.cpp
    network_util.cpp
)

if (USE_JEMALLOC AND USE_MEM_TRACKER AND USE_JEMALLOC_HOOK)
    set(COMMON_FILES ${COMMON_FILES}
        jemalloc_hook.cpp
        )
endif()

add_library(Common STATIC
    ${COMMON_FILES}
    )
