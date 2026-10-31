// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format/parquet_metadata_memory.h"

#include <gen_cpp/parquet_types.h>
#include <gtest/gtest.h>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_file_metadata.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "io/fs/file_meta_cache.h"
#include "runtime/exec_env.h"

namespace doris {
TEST(ParquetMetadataMemoryTest, ReservedVectorsStringsAndStatistics) {
    tparquet::FileMetaData metadata;
    const auto base = parquet_metadata_dynamic_size(metadata);
    metadata.row_groups.reserve(64);
    EXPECT_GE(parquet_metadata_dynamic_size(metadata) - base, 64 * sizeof(tparquet::RowGroup));
    metadata.row_groups.resize(1);
    auto& columns = metadata.row_groups[0].columns;
    columns.resize(1);
    auto& column = columns[0];
    const auto max_value_capacity_before = column.meta_data.statistics.max_value.capacity();
    auto before = parquet_metadata_dynamic_size(metadata);
    column.meta_data.statistics.max_value.assign(8192, 'm');
    // The empty string's inline capacity is already part of the baseline estimate.
    EXPECT_GE(parquet_metadata_dynamic_size(metadata) - before, 8192 - max_value_capacity_before);
    before = parquet_metadata_dynamic_size(metadata);
    column.meta_data.path_in_schema.emplace_back(4096, 'p');
    EXPECT_GE(parquet_metadata_dynamic_size(metadata) - before, 4096 + sizeof(std::string));
    before = parquet_metadata_dynamic_size(metadata);
    column.meta_data.key_value_metadata.resize(1);
    column.meta_data.key_value_metadata[0].value.assign(4096, 'v');
    column.meta_data.size_statistics.definition_level_histogram.reserve(100);
    EXPECT_GE(parquet_metadata_dynamic_size(metadata) - before,
              4096 + sizeof(tparquet::KeyValue) + 100 * sizeof(int64_t));
    const auto encryption_capacity_before =
            column.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY.key_metadata.capacity() +
            column.encrypted_column_metadata.capacity() +
            metadata.footer_signing_key_metadata.capacity();
    before = parquet_metadata_dynamic_size(metadata);
    column.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY.key_metadata.assign(4096, 'k');
    column.encrypted_column_metadata.assign(4096, 'e');
    metadata.footer_signing_key_metadata.assign(4096, 's');
    const auto encryption_capacity_after =
            column.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY.key_metadata.capacity() +
            column.encrypted_column_metadata.capacity() +
            metadata.footer_signing_key_metadata.capacity();
    // The baseline already includes the empty strings' inline capacities. Use
    // actual growth so the assertion is independent of the string implementation.
    EXPECT_EQ(parquet_metadata_dynamic_size(metadata) - before,
              encryption_capacity_after - encryption_capacity_before);
}

TEST(ParquetMetadataMemoryTest, SharedNestedDataTypesCountOnce) {
    auto child = make_nullable(std::make_shared<DataTypeInt32>());
    DataTypeStruct type({child, child}, {std::string(4096, 'a'), std::string(4096, 'b')});
    std::unordered_set<const IDataType*> seen;
    EXPECT_GE(parquet_type_memory_size(&type, &seen), 8192);
    EXPECT_EQ(parquet_type_memory_size(child.get(), &seen), 0);
    EXPECT_EQ(parquet_type_memory_size(&type, &seen), 0);
}

TEST(ParquetMetadataMemoryTest, BothSchemasIncludeNamesAndNativeTree) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name(std::string(1024, 'c'));
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    FieldDescriptor v1;
    format::parquet::NativeFieldDescriptor v2;
    ASSERT_TRUE(v1.parse_from_thrift({root, leaf}).ok());
    ASSERT_TRUE(v2.parse_from_thrift({root, leaf}).ok());
    // Field name, lowercase name, copied Thrift name and lookup-map key are retained separately.
    EXPECT_GE(v1.estimated_memory_size(), 4 * 1024);
    EXPECT_GE(v2.estimated_memory_size(), 4 * 1024);
}

TEST(ParquetMetadataMemoryTest, CacheTrackingTransfersAndReleasesOnce) {
    auto* tracker = ExecEnv::GetInstance()->parquet_meta_tracker().get();
    const int64_t before = tracker->consumption();
    FileMetaCache cache(1 << 20, LRUCacheType::SIZE, 1);
    tparquet::FileMetaData thrift;
    thrift.created_by.assign(4096, 'w');
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name(std::string(1024, 'c'));
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    thrift.schema = {root, leaf};
    auto v1 = std::make_unique<FileMetaData>(thrift);
    auto v2 = std::make_unique<format::parquet::NativeParquetMetadata>(thrift);
    ASSERT_TRUE(v1->init_schema(false, false).ok());
    ASSERT_TRUE(v2->init_schema(false, false).ok());
    const auto v1_size = v1->get_mem_size();
    const auto v2_size = v2->get_mem_size();
    EXPECT_GT(v1_size, 4096);
    EXPECT_GT(v2_size, 4096);
    EXPECT_EQ(tracker->consumption() - before, v1_size + v2_size);
    ObjLRUCache::CacheHandle first;
    ObjLRUCache::CacheHandle second;
    ASSERT_TRUE(cache.insert("v1", v1, &first, v1_size));
    first.data<FileMetaData>()->release_memory_tracking();
    ASSERT_TRUE(cache.insert("v2", v2, &second, v2_size));
    second.data<format::parquet::NativeParquetMetadata>()->release_memory_tracking();
    EXPECT_EQ(tracker->consumption(), before);
    EXPECT_GE(cache.cache().mem_consumption(), v1_size + v2_size);
    cache.cache().erase(ObjLRUCache::ObjKey {"v1"});
    EXPECT_EQ(first.data<FileMetaData>()->get_mem_size(), v1_size);
    first = {};
    second = {};
    cache.cache().prune_all(true);
    EXPECT_EQ(cache.cache().mem_consumption(), 0);
    EXPECT_EQ(tracker->consumption(), before);
}
} // namespace doris
