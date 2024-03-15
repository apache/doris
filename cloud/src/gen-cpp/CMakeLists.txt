set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/gen_cpp")

set (GEN_CPP_DIR ${GENSRC_DIR}/gen_cpp)
set(SRC_FILES
    ${GEN_CPP_DIR}/cloud.pb.cc
    ${GEN_CPP_DIR}/olap_file.pb.cc
    ${GEN_CPP_DIR}/types.pb.cc
    ${GEN_CPP_DIR}/segment_v2.pb.cc
    ${GEN_CPP_DIR}/olap_common.pb.cc
)

# keep everything in one library, the object files reference
# each other
add_library(CloudGen STATIC ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds
# work correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)
