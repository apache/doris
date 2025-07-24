# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/meta-store")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/meta-store")

add_library(MetaStore STATIC
    blob_message.cpp
    codec.cpp
    document_message.cpp
    keys.cpp
    mem_txn_kv.cpp
    meta_reader.cpp
    txn_kv.cpp
    versioned_value.cpp
)
