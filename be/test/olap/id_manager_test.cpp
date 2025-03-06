#include "olap/id_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "olap/olap_common.h"

using namespace doris;

TEST(IdFileMapTest, BasicOperations) {
    IdFileMap id_file_map(1024);

    // Test adding a file mapping
    auto mapping1 =
            std::make_shared<FileMapping>(FileMapping {FileMappingType::DORIS_FORMAT, "file1"});
    uint32_t id1 = id_file_map.get_file_mapping_id(mapping1);
    EXPECT_EQ(id1, 0);

    auto mapping2 = std::make_shared<FileMapping>(FileMapping {FileMappingType::ORC, "file2"});
    uint32_t id2 = id_file_map.get_file_mapping_id(mapping2);
    EXPECT_EQ(id2, 1);

    // Test getting a file mapping
    auto retrieved_mapping1 = id_file_map.get_file_mapping(id1);
    EXPECT_EQ(retrieved_mapping1->type, FileMappingType::DORIS_FORMAT);
    EXPECT_EQ(retrieved_mapping1->value, "file1");

    auto retrieved_mapping2 = id_file_map.get_file_mapping(id2);
    EXPECT_EQ(retrieved_mapping2->type, FileMappingType::ORC);
    EXPECT_EQ(retrieved_mapping2->value, "file2");

    // Test getting a non-existent file mapping
    auto retrieved_mapping3 = id_file_map.get_file_mapping(999);
    EXPECT_EQ(retrieved_mapping3, nullptr);
}

TEST(IdFileMapTest, ConcurrentAddAndGet) {
    IdFileMap id_file_map(1024);
    std::vector<std::thread> threads;
    std::atomic<int> counter(0);

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 100; ++j) {
                auto mapping = std::make_shared<FileMapping>(FileMapping {
                        FileMappingType::DORIS_FORMAT, "file" + std::to_string(counter++)});
                uint32_t id = id_file_map.get_file_mapping_id(mapping);
                auto retrieved_mapping = id_file_map.get_file_mapping(id);
                EXPECT_EQ(retrieved_mapping->type, mapping->type);
                EXPECT_EQ(retrieved_mapping->value, mapping->value);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST(IdManagerTest, BasicOperations) {
    IdManager id_manager;

    // Test adding an IdFileMap
    UniqueId query_id1 = UniqueId::gen_uid();
    auto id_file_map1 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_NE(id_file_map1, nullptr);

    UniqueId query_id2 = UniqueId::gen_uid();
    auto id_file_map2 = id_manager.add_id_file_map(query_id2, 1024);
    EXPECT_NE(id_file_map2, nullptr);

    // Test getting an existing IdFileMap
    auto retrieved_id_file_map1 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_EQ(retrieved_id_file_map1, id_file_map1);

    // Test removing an IdFileMap
    id_manager.remove_id_file_map(query_id1);
    auto retrieved_id_file_map2 = id_manager.add_id_file_map(query_id1, 1024);
    EXPECT_NE(retrieved_id_file_map2, id_file_map1);
}

TEST(IdManagerTest, ConcurrentAddAndRemove) {
    IdManager id_manager;
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 10; ++j) {
                UniqueId query_id = UniqueId::gen_uid();
                auto id_file_map = id_manager.add_id_file_map(query_id, 1024);
                EXPECT_NE(id_file_map, nullptr);

                id_manager.remove_id_file_map(query_id);
                auto retrieved_id_file_map = id_manager.add_id_file_map(query_id, 1024);
                EXPECT_NE(retrieved_id_file_map, id_file_map);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}
