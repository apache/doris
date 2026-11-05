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

#include "format_v2/parquet/reader/decoded_column_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "io/file_factory.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/thread_context.h"

namespace doris::format::parquet {
namespace {

Field numeric_field(PrimitiveType type, double value) {
    switch (type) {
    case TYPE_TINYINT:
        return Field::create_field<TYPE_TINYINT>(static_cast<Int8>(value));
    case TYPE_SMALLINT:
        return Field::create_field<TYPE_SMALLINT>(static_cast<Int16>(value));
    case TYPE_INT:
        return Field::create_field<TYPE_INT>(static_cast<Int32>(value));
    case TYPE_BIGINT:
        return Field::create_field<TYPE_BIGINT>(static_cast<Int64>(value));
    case TYPE_FLOAT:
        return Field::create_field<TYPE_FLOAT>(static_cast<Float32>(value));
    case TYPE_DOUBLE:
        return Field::create_field<TYPE_DOUBLE>(value);
    default:
        DORIS_CHECK(false);
        return {};
    }
}

class DenseTestColumnReader final : public ParquetColumnReader {
public:
    DenseTestColumnReader(const ParquetColumnSchema& schema, ColumnPtr data, bool fail)
            : ParquetColumnReader(schema, schema.type), _data(std::move(data)), _fail(fail) {}
    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        if (_fail) {
            return Status::IOError("injected decode failure");
        }
        DORIS_CHECK(_offset + rows <= _data->size());
        column->insert_range_from(*_data, _offset, rows);
        _offset += rows;
        *rows_read = rows;
        return Status::OK();
    }
    Status skip(int64_t rows) override {
        _offset += rows;
        return Status::OK();
    }

private:
    ColumnPtr _data;
    bool _fail;
    size_t _offset = 0;
};

class ParquetDecodedColumnCacheTest : public testing::Test {
protected:
    ParquetColumnSchema schema;
    size_t admission = 0;
    int factory_calls = 0;

    void SetUp() override {
        schema.local_id = 0;
        schema.leaf_column_id = 0;
        schema.parquet_field_id = 1;
        schema.name = "value";
        schema.type = make_nullable(std::make_shared<DataTypeInt32>());
    }

    MutableColumnPtr values(size_t rows = 10) {
        auto column = schema.type->create_column();
        for (size_t row = 0; row < rows; ++row) {
            column->insert(Field::create_field<TYPE_INT>(static_cast<Int32>(row * 3)));
        }
        return column;
    }

    std::unique_ptr<ParquetColumnReader> reader(DecodedColumnCache& cache, const std::string& key,
                                                ColumnPtr data, bool fill = true, bool fail = false,
                                                bool* hit_out = nullptr) {
        std::unique_ptr<ParquetColumnReader> result;
        auto plan = cache.prepare_reader(key, schema, {});
        const bool hit = static_cast<bool>(plan.hit);
        EXPECT_TRUE(cache.create_reader(
                                 std::move(plan), schema, data->size(), fill, {}, &admission,
                                 [&](auto* native) {
                                     ++factory_calls;
                                     *native = std::make_unique<DenseTestColumnReader>(schema, data,
                                                                                       fail);
                                     return Status::OK();
                                 },
                                 &result)
                            .ok());
        if (hit_out != nullptr) {
            *hit_out = hit;
        }
        return result;
    }

    void expect_equal(const IColumn& actual, const IColumn& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t row = 0; row < actual.size(); ++row) {
            EXPECT_EQ(actual.compare_at(row, row, expected, 1), 0) << row;
        }
    }
};

TEST_F(ParquetDecodedColumnCacheTest, CompleteMultiBatchColumnSkipsNativeReaderOnHit) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto cold = reader(cache, "column", source->get_ptr());
    auto output = schema.type->create_column();
    int64_t read = 0;
    ASSERT_TRUE(cold->read(3, output, &read).ok());
    EXPECT_FALSE(cache.lookup_column("column"));
    ASSERT_TRUE(cold->read(7, output, &read).ok());
    expect_equal(*output, *source);
    ASSERT_TRUE(cache.lookup_column("column"));
    bool hit = false;
    auto warm = reader(cache, "column", source->get_ptr(), false, true, &hit);
    ASSERT_TRUE(hit);
    EXPECT_EQ(factory_calls, 1);
    output = schema.type->create_column();
    ASSERT_TRUE(warm->read(10, output, &read).ok());
    expect_equal(*output, *source);
}

TEST_F(ParquetDecodedColumnCacheTest, NumericTypesAndNullableMapsRoundTrip) {
    const DataTypes types = {
            std::make_shared<DataTypeInt8>(),    std::make_shared<DataTypeInt16>(),
            std::make_shared<DataTypeInt32>(),   std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat64>()};
    for (const auto& type : types) {
        for (bool nullable : {false, true}) {
            schema.type = nullable ? make_nullable(type) : type;
            DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
            auto source = schema.type->create_column();
            for (int row = -5; row < 5; ++row) {
                if (nullable && row % 3 == 0) {
                    source->insert(Field());
                } else if (type->get_primitive_type() == TYPE_FLOAT ||
                           type->get_primitive_type() == TYPE_DOUBLE) {
                    source->insert(numeric_field(
                            type->get_primitive_type(),
                            row == 1 ? std::numeric_limits<double>::quiet_NaN() : double(row) / 2));
                } else {
                    source->insert(numeric_field(type->get_primitive_type(), row));
                }
            }
            auto entry = cache.start_fill("typed", schema.type, source->size(), &admission);
            ASSERT_NE(entry, nullptr);
            entry->append_from(*source, 0, 4);
            entry->append_from(*source, 4, 6);
            cache.publish("typed", std::move(entry));
            auto cached = cache.lookup_column("typed");
            ASSERT_TRUE(cached);
            auto output = schema.type->create_column();
            cached->append_to(*output, 0, 10);
            expect_equal(*output, *source);
        }
    }
}

TEST_F(ParquetDecodedColumnCacheTest, HitPreservesPhysicalPositionsAcrossSkipAndSelection) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto cold = reader(cache, "column", source->get_ptr());
    auto output = schema.type->create_column();
    int64_t read = 0;
    ASSERT_TRUE(cold->read(10, output, &read).ok());
    auto warm = reader(cache, "column", source->get_ptr());
    ASSERT_TRUE(warm->skip(2).ok());
    SelectionVector selection(2);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    output = schema.type->create_column();
    ASSERT_TRUE(warm->select(selection, 2, 5, output).ok());
    ASSERT_TRUE(warm->read(3, output, &read).ok());
    auto expected = schema.type->create_column();
    for (size_t row : {2, 5, 7, 8, 9}) {
        expected->insert_from(*source, row);
    }
    expect_equal(*output, *expected);
}

TEST_F(ParquetDecodedColumnCacheTest, EarlyCloseAndDecodeFailureNeverPublishPartialColumns) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto output = schema.type->create_column();
    int64_t read = 0;
    auto partial = reader(cache, "partial", source->get_ptr());
    ASSERT_TRUE(partial->read(5, output, &read).ok());
    partial.reset();
    EXPECT_FALSE(cache.lookup_column("partial"));
    EXPECT_EQ(cache.live_bytes(), 0);
    auto failed = reader(cache, "failed", source->get_ptr(), true, true);
    EXPECT_FALSE(failed->read(10, output, &read).ok());
    EXPECT_FALSE(cache.lookup_column("failed"));
    EXPECT_EQ(cache.live_bytes(), 0);
}

TEST_F(ParquetDecodedColumnCacheTest, SkipOrSelectedMissAbandonsFill) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto skipped = reader(cache, "skip", source->get_ptr());
    ASSERT_TRUE(skipped->skip(1).ok());
    auto output = schema.type->create_column();
    int64_t read = 0;
    ASSERT_TRUE(skipped->read(9, output, &read).ok());
    EXPECT_FALSE(cache.lookup_column("skip"));
    auto selected = reader(cache, "select", source->get_ptr());
    SelectionVector selection(1);
    selection.set_index(0, 5);
    output = schema.type->create_column();
    ASSERT_TRUE(selected->select(selection, 1, 10, output).ok());
    EXPECT_FALSE(cache.lookup_column("select"));
    EXPECT_EQ(cache.live_bytes(), 0);
}

TEST_F(ParquetDecodedColumnCacheTest, FileIdentityRejectsMutableFilesAndSeparatesSources) {
    io::FileSystemProperties source;
    source.system_type = TFileType::FILE_S3;
    source.properties = {{"s3.endpoint", "https://one"}, {"s3.access_key", "user-one"}};
    io::FileDescription file;
    file.path = "s3://bucket/data.parquet";
    file.mtime = 1;
    EXPECT_TRUE(DecodedColumnCache::file_key(source, file, 100).empty());
    file.is_immutable = true;
    const auto original = DecodedColumnCache::file_key(source, file, 100);
    ASSERT_FALSE(original.empty());
    EXPECT_EQ(original, DecodedColumnCache::file_key(source, file, 100));
    EXPECT_NE(original, DecodedColumnCache::file_key(source, file, 101));
    source.properties["s3.endpoint"] = "https://two";
    EXPECT_NE(original, DecodedColumnCache::file_key(source, file, 100));
    source.properties["s3.endpoint"] = "https://one";
    source.properties["s3.access_key"] = "user-two";
    EXPECT_NE(original, DecodedColumnCache::file_key(source, file, 100));
    source.properties["s3.access_key"] = "user-one";
    file.mtime = 2;
    EXPECT_NE(original, DecodedColumnCache::file_key(source, file, 100));
    file.mtime = 1;
    file.path = "s3://bucket/rewritten.parquet";
    EXPECT_NE(original, DecodedColumnCache::file_key(source, file, 100));
}

TEST_F(ParquetDecodedColumnCacheTest, ColumnKeySeparatesProjectionTypeAndDecodeOptions) {
    schema.type = std::make_shared<DataTypeInt32>();
    const auto original = DecodedColumnCache::column_key("file", schema, 0, false);
    EXPECT_NE(original, DecodedColumnCache::column_key("file", schema, 1, false));
    EXPECT_NE(original, DecodedColumnCache::column_key("file", schema, 0, true));
    ++schema.parquet_field_id;
    EXPECT_NE(original, DecodedColumnCache::column_key("file", schema, 0, false));
    --schema.parquet_field_id;
    ++schema.leaf_column_id;
    EXPECT_NE(original, DecodedColumnCache::column_key("file", schema, 0, false));
    schema.type = make_nullable(schema.type);
    EXPECT_NE(original, DecodedColumnCache::column_key("file", schema, 0, false));
    schema.type = std::make_shared<DataTypeString>();
    EXPECT_TRUE(DecodedColumnCache::column_key("file", schema, 0, false).empty());
    schema.type = std::make_shared<DataTypeInt32>();
    schema.max_repetition_level = 1;
    EXPECT_TRUE(DecodedColumnCache::column_key("file", schema, 0, false).empty());
    EXPECT_TRUE(DecodedColumnCache::column_key("", schema, 0, false).empty());
}

TEST_F(ParquetDecodedColumnCacheTest, EntryAndFileAdmissionLimitsAreCheckedBeforeAllocation) {
    DecodedColumnCache cache(4096, 1024, 1024);
    EXPECT_FALSE(cache.start_fill("oversize", schema.type, 1024, &admission));
    EXPECT_EQ(cache.live_bytes(), 0);
    auto entry = cache.start_fill("fits", schema.type, 10, &admission);
    ASSERT_NE(entry, nullptr);
    EXPECT_GT(cache.live_bytes(), 0);
    EXPECT_FALSE(cache.start_fill("next", schema.type, 100, &admission));
    entry.reset();
    EXPECT_EQ(cache.live_bytes(), 0);
}

TEST_F(ParquetDecodedColumnCacheTest, PinnedAndFillingEntriesShareHardCapacity) {
    DecodedColumnCache cache(4096, 2048, 1 << 20);
    std::vector<DecodedColumnCache::Handle> pinned;
    auto source = values(200);
    for (size_t index = 0; index < 32; ++index) {
        const auto key = std::to_string(index);
        auto entry = cache.start_fill(key, schema.type, 200, &admission);
        if (!entry) {
            break;
        }
        entry->append_from(*source, 0, 200);
        cache.publish(key, std::move(entry));
        pinned.push_back(cache.lookup_column(key));
        EXPECT_LE(cache.live_bytes(), 4096);
    }
    ASSERT_FALSE(pinned.empty());
    EXPECT_FALSE(cache.start_fill("full", schema.type, 200, &admission));
    pinned.clear();
    auto replacement = cache.start_fill("evict", schema.type, 200, &admission);
    EXPECT_NE(replacement, nullptr);
    EXPECT_LE(cache.live_bytes(), 4096);
}

TEST_F(ParquetDecodedColumnCacheTest, ReplacedPinnedValueKeepsItsReservationAndContents) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto first = cache.start_fill("same", schema.type, 10, &admission);
    ASSERT_NE(first, nullptr);
    first->append_from(*source, 0, 10);
    cache.publish("same", std::move(first));
    auto pinned = cache.lookup_column("same");
    auto second = cache.start_fill("same", schema.type, 10, &admission);
    ASSERT_NE(second, nullptr);
    second->append_from(*source, 0, 10);
    const auto charge = second->charge;
    cache.publish("same", std::move(second));
    EXPECT_EQ(cache.live_bytes(), 2 * charge);
    auto output = schema.type->create_column();
    pinned->append_to(*output, 0, 10);
    expect_equal(*output, *source);
    pinned.reset();
    EXPECT_EQ(cache.live_bytes(), charge);
}

TEST_F(ParquetDecodedColumnCacheTest, LowQueryMemoryDeclinesOptionalFill) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "tiny", 1);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
    EXPECT_FALSE(cache.start_fill("limited", schema.type, 100, &admission));
    EXPECT_EQ(cache.live_bytes(), 0);
}

TEST_F(ParquetDecodedColumnCacheTest, PreparedHitRetainsTheResolvedValueAcrossReplacement) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto source = values();
    auto first = cache.start_fill("same", schema.type, source->size(), &admission);
    ASSERT_NE(first, nullptr);
    first->append_from(*source, 0, source->size());
    cache.publish("same", std::move(first));
    auto plan = cache.prepare_reader("same", schema, {});
    ASSERT_TRUE(plan.hit);

    auto replacement = schema.type->create_column();
    replacement->insert_many_defaults(source->size());
    auto second = cache.start_fill("same", schema.type, source->size(), &admission);
    ASSERT_NE(second, nullptr);
    second->append_from(*replacement, 0, replacement->size());
    cache.publish("same", std::move(second));

    std::unique_ptr<ParquetColumnReader> cached_reader;
    ASSERT_TRUE(cache.create_reader(
                             std::move(plan), schema, source->size(), false, {}, &admission,
                             [&](auto*) {
                                 ++factory_calls;
                                 return Status::InternalError("unexpected native reader");
                             },
                             &cached_reader)
                        .ok());
    EXPECT_EQ(factory_calls, 0);
    auto output = schema.type->create_column();
    int64_t read = 0;
    ASSERT_TRUE(cached_reader->read(source->size(), output, &read).ok());
    expect_equal(*output, *source);
}

TEST_F(ParquetDecodedColumnCacheTest, CacheAllocationPreservesTheQueryReservation) {
    DecodedColumnCache cache(1 << 20, 1 << 16, 1 << 18);
    auto query = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 1 << 20);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(query);
    auto* tracker = thread_context()->thread_mem_tracker_mgr.get();
    ASSERT_TRUE(tracker->try_reserve(4096).ok());
    DEFER_RELEASE_RESERVED();
    const auto query_reserved = tracker->reserved_mem();
    const auto process_reserved = GlobalMemoryArbitrator::process_reserved_memory();
    auto entry = cache.start_fill("reserved", schema.type, 100, &admission);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(tracker->limiter_mem_tracker(), query.get());
    EXPECT_EQ(tracker->reserved_mem(), query_reserved);
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), process_reserved);
    EXPECT_GT(cache.mem_tracker()->consumption(), 0);
    EXPECT_EQ(cache.mem_tracker()->reserved_consumption(), 0);
}

TEST_F(ParquetDecodedColumnCacheTest, ConcurrentFillsAndEvictionsRemainBounded) {
    DecodedColumnCache cache(8192, 2048, 1 << 20);
    auto source = values(100);
    std::atomic<int> failures {0};
    std::vector<std::thread> threads;
    for (int worker = 0; worker < 4; ++worker) {
        threads.emplace_back([&, worker] {
            SCOPED_INIT_THREAD_CONTEXT();
            size_t worker_admission = 0;
            for (int index = 0; index < 32; ++index) {
                const auto key = std::to_string(worker) + ":" + std::to_string(index);
                auto entry = cache.start_fill(key, schema.type, 100, &worker_admission);
                if (entry) {
                    entry->append_from(*source, 0, 100);
                    cache.publish(key, std::move(entry));
                }
                if (cache.live_bytes() > 8192) {
                    ++failures;
                }
                if (auto hit = cache.lookup_column(key); hit) {
                    auto output = schema.type->create_column();
                    hit->append_to(*output, 0, 100);
                    if (output->size() != 100) {
                        ++failures;
                    }
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(failures, 0);
    EXPECT_LE(cache.live_bytes(), 8192);
}

} // namespace
} // namespace doris::format::parquet
