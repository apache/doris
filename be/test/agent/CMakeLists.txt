# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/agent")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/agent")

# ADD_BE_TEST(agent_server_test) 
ADD_BE_TEST(cgroups_mgr_test)
ADD_BE_TEST(file_downloader_test)
#ADD_BE_TEST(heartbeat_server_test)
ADD_BE_TEST(pusher_test)
# ADD_BE_TEST(task_worker_pool_test)
ADD_BE_TEST(utils_test)
