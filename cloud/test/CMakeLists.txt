# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test")

set(TEST_LINK_LIBS ResourceManager ${TEST_LINK_LIBS})

#add_executable(sync_point_test sync_point_test.cpp)

add_executable(codec_test codec_test.cpp)

add_executable(keys_test keys_test.cpp)

add_executable(doris_txn_test doris_txn_test.cpp)

add_executable(txn_kv_test txn_kv_test.cpp)
set_target_properties(txn_kv_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

add_executable(recycler_test recycler_test.cpp mock_accessor.cpp)

add_executable(mem_txn_kv_test mem_txn_kv_test.cpp)

add_executable(meta_service_test
   meta_service_test.cpp
   meta_service_job_test.cpp
   meta_service_http_test.cpp
   schema_kv_test.cpp
)

add_executable(meta_server_test meta_server_test.cpp)

add_executable(rate_limiter_test rate_limiter_test.cpp)

add_executable(s3_rate_limiter_test s3_rate_limiter_test.cpp)

add_executable(encryption_test encryption_test.cpp)

add_executable(metric_test metric_test.cpp)

add_executable(log_test log_test.cpp)

add_executable(resource_test resource_test.cpp)

add_executable(http_encode_key_test http_encode_key_test.cpp)

add_executable(fdb_injection_test fdb_injection_test.cpp)

add_executable(s3_accessor_test s3_accessor_test.cpp)

add_executable(hdfs_accessor_test hdfs_accessor_test.cpp)

add_executable(stopwatch_test stopwatch_test.cpp)

add_executable(util_test util_test.cpp)

add_executable(network_util_test network_util_test.cpp)

add_executable(txn_lazy_commit_test txn_lazy_commit_test.cpp)

message("Meta-service test dependencies: ${TEST_LINK_LIBS}")
#target_link_libraries(sync_point_test ${TEST_LINK_LIBS})

target_link_libraries(codec_test ${TEST_LINK_LIBS})

target_link_libraries(keys_test ${TEST_LINK_LIBS})

target_link_libraries(meta_service_test ${TEST_LINK_LIBS})

target_link_libraries(meta_server_test ${TEST_LINK_LIBS})

target_link_libraries(rate_limiter_test ${TEST_LINK_LIBS})

target_link_libraries(s3_rate_limiter_test ${TEST_LINK_LIBS})

target_link_libraries(encryption_test ${TEST_LINK_LIBS})

target_link_libraries(metric_test ${TEST_LINK_LIBS})

target_link_libraries(log_test ${TEST_LINK_LIBS})

target_link_libraries(resource_test ${TEST_LINK_LIBS})

target_link_libraries(http_encode_key_test ${TEST_LINK_LIBS})

target_link_libraries(s3_accessor_test ${TEST_LINK_LIBS})

target_link_libraries(hdfs_accessor_test ${TEST_LINK_LIBS})

target_link_libraries(stopwatch_test ${TEST_LINK_LIBS})

target_link_libraries(util_test ${TEST_LINK_LIBS})

target_link_libraries(network_util_test ${TEST_LINK_LIBS})

target_link_libraries(txn_lazy_commit_test ${TEST_LINK_LIBS})

# FDB related tests need to be linked with libfdb_c
set(FDB_LINKER_FLAGS "-lfdb_c -L${THIRDPARTY_DIR}/lib")

target_link_libraries(doris_txn_test
    ${FDB_LINKER_FLAGS}
    ${TEST_LINK_LIBS}
)

target_link_libraries(txn_kv_test
    ${FDB_LINKER_FLAGS}
    ${TEST_LINK_LIBS}
)

target_link_libraries(mem_txn_kv_test
    ${FDB_LINKER_FLAGS}
    ${TEST_LINK_LIBS}
)

target_link_libraries(recycler_test
    ${FDB_LINKER_FLAGS}
    ${TEST_LINK_LIBS}
)

target_link_libraries(fdb_injection_test
    ${FDB_LINKER_FLAGS}
    ${TEST_LINK_LIBS})

install(FILES
    ${BASE_DIR}/script/run_all_tests.sh
    ${BASE_DIR}/conf/fdb.cluster
    ${BASE_DIR}/conf/doris_cloud.conf
    ${BASE_DIR}/conf/lsan_suppression.conf
    ${BASE_DIR}/test/fdb_metric_example.json
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BUILD_DIR}/test)

install(FILES
    ${THIRDPARTY_DIR}/lib/libfdb_c.so
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BUILD_DIR}/test)

