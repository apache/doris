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
#include "storage/segment/column_reader.h"

#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {
namespace {
class TestColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t /* ord */) override { return Status::OK(); }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        dst->insert_many_defaults(*n);
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* /* rowids */, const size_t count,
                          MutableColumnPtr& dst) override {
        dst->insert_many_defaults(count);
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return 0; }

    void force_set_read_requirement(ReadRequirement requirement) {
        _read_requirement = requirement;
    }

    using ColumnIterator::AccessPathSplit;

    Result<AccessPathSplit> split_access_paths(const TColumnAccessPaths& access_paths) const {
        return _split_access_paths(access_paths);
    }

    Status check_and_set_meta_read_mode(ReadRequirement requirement_before_access_path,
                                        const TColumnAccessPaths& access_paths) {
        auto split = DORIS_TRY(_split_access_paths(access_paths));
        return _check_and_set_meta_read_mode(requirement_before_access_path, split);
    }

    void convert_to_place_holder_column(MutableColumnPtr& dst, size_t count) {
        _convert_to_place_holder_column(dst, count);
    }
};

TColumnAccessPath create_data_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    access_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_access_path;
    data_access_path.__set_path(std::move(path));
    access_path.__set_data_access_path(std::move(data_access_path));
    return access_path;
}

TColumnAccessPath create_legacy_data_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_access_path;
    data_access_path.__set_path(std::move(path));
    access_path.__set_data_access_path(std::move(data_access_path));
    return access_path;
}

TColumnAccessPath create_meta_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    access_path.__set_type(TAccessPathType::META);
    TMetaAccessPath meta_access_path;
    meta_access_path.__set_path(std::move(path));
    access_path.__set_meta_access_path(std::move(meta_access_path));
    return access_path;
}

std::shared_ptr<ColumnReader> create_test_reader(
        bool is_nullable = false, uint64_t num_rows = 0,
        FieldType field_type = FieldType::OLAP_FIELD_TYPE_INT) {
    auto reader = std::make_shared<ColumnReader>();
    reader->_meta_is_nullable = is_nullable;
    reader->_num_rows = num_rows;
    reader->_meta_type = field_type;
    return reader;
}

class TrackingColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t ord) override {
        seek_ordinals.emplace_back(ord);
        _current_ordinal = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        next_batch_sizes.emplace_back(*n);
        if (!need_to_read()) {
            _convert_to_place_holder_column(dst, *n);
            if (has_null != nullptr) {
                *has_null = false;
            }
            return Status::OK();
        }

        _recovery_from_place_holder_column(dst);
        dst->insert_many_defaults(*n);
        _current_ordinal += *n;
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        read_by_rowids_batches.emplace_back(rowids, rowids + count);
        if (!need_to_read()) {
            _convert_to_place_holder_column(dst, count);
            return Status::OK();
        }

        _recovery_from_place_holder_column(dst);
        dst->insert_many_defaults(count);
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override {
        routed_all_access_paths = all_access_paths;
        routed_predicate_access_paths = predicate_access_paths;
        return ColumnIterator::set_access_paths(all_access_paths, predicate_access_paths);
    }

    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override {
        record_collect_method(init_method);
        prefetchers[init_method].emplace_back(prefetcher());
    }

    SegmentPrefetcher* prefetcher() const {
        return reinterpret_cast<SegmentPrefetcher*>(const_cast<TrackingColumnIterator*>(this));
    }

    void clear_tracking() {
        seek_ordinals.clear();
        next_batch_sizes.clear();
        read_by_rowids_batches.clear();
        collect_methods.clear();
        routed_all_access_paths.clear();
        routed_predicate_access_paths.clear();
    }

    std::vector<ordinal_t> seek_ordinals;
    std::vector<size_t> next_batch_sizes;
    std::vector<std::vector<rowid_t>> read_by_rowids_batches;
    std::vector<PrefetcherInitMethod> collect_methods;
    TColumnAccessPaths routed_all_access_paths;
    TColumnAccessPaths routed_predicate_access_paths;

private:
    void record_collect_method(PrefetcherInitMethod init_method) {
        collect_methods.emplace_back(init_method);
    }

    ordinal_t _current_ordinal = 0;
};

class TrackingFileColumnIterator final : public FileColumnIterator {
public:
    explicit TrackingFileColumnIterator(std::shared_ptr<ColumnReader> reader)
            : FileColumnIterator(std::move(reader)) {}

    Status seek_to_ordinal(ordinal_t ord) override {
        seek_ordinals.emplace_back(ord);
        _current_ordinal = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        next_batch_sizes.emplace_back(*n);
        dst->insert_many_defaults(*n);
        _current_ordinal += *n;
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        read_by_rowids_batches.emplace_back(rowids, rowids + count);
        dst->insert_many_defaults(count);
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override {
        record_collect_method(init_method);
        prefetchers[init_method].emplace_back(prefetcher());
    }

    SegmentPrefetcher* prefetcher() const {
        return reinterpret_cast<SegmentPrefetcher*>(const_cast<TrackingFileColumnIterator*>(this));
    }

    std::vector<ordinal_t> seek_ordinals;
    std::vector<size_t> next_batch_sizes;
    std::vector<std::vector<rowid_t>> read_by_rowids_batches;
    std::vector<PrefetcherInitMethod> collect_methods;

private:
    void record_collect_method(PrefetcherInitMethod init_method) {
        collect_methods.emplace_back(init_method);
    }

    ordinal_t _current_ordinal = 0;
};

class RowidOffsetFileColumnIterator final : public FileColumnIterator {
public:
    RowidOffsetFileColumnIterator() : FileColumnIterator(create_test_reader(false, 10)) {}

    Status seek_to_ordinal(ordinal_t ord) override {
        _current_ordinal = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        auto& offsets = assert_cast<ColumnOffset64&, TypeCheckOnRelease::DISABLE>(*dst);
        for (size_t i = 0; i < *n; ++i) {
            offsets.insert_value(_current_ordinal + i);
        }
        _current_ordinal += *n;
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        auto& offsets = assert_cast<ColumnOffset64&, TypeCheckOnRelease::DISABLE>(*dst);
        for (size_t i = 0; i < count; ++i) {
            offsets.insert_value(rowids[i]);
        }
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

private:
    ordinal_t _current_ordinal = 0;
};

class NullMapOnlyFileColumnIterator final : public FileColumnIterator {
public:
    explicit NullMapOnlyFileColumnIterator(std::shared_ptr<ColumnReader> reader)
            : FileColumnIterator(std::move(reader)) {}

    void force_null_map_only() { _meta_read_mode = MetaReadMode::NULL_MAP_ONLY; }
};

MutableColumnPtr create_int_struct_column(size_t field_count) {
    Columns columns;
    for (size_t i = 0; i < field_count; ++i) {
        columns.emplace_back(ColumnInt32::create());
    }
    return ColumnStruct::create(std::move(columns));
}

MutableColumnPtr create_nullable_int_struct_column(size_t field_count) {
    return ColumnNullable::create(create_int_struct_column(field_count), ColumnUInt8::create());
}

MutableColumnPtr create_nullable_int_array_column() {
    return ColumnNullable::create(
            ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create()),
            ColumnUInt8::create());
}

MutableColumnPtr create_nullable_int_map_column() {
    return ColumnNullable::create(ColumnMap::create(ColumnInt32::create(), ColumnInt32::create(),
                                                    ColumnArray::ColumnOffsets::create()),
                                  ColumnUInt8::create());
}

struct TrackingOffsetIterator {
    OffsetFileColumnIteratorUPtr iterator;
    TrackingFileColumnIterator* tracker = nullptr;
};

TrackingOffsetIterator create_tracking_offset_iterator() {
    auto file_iterator = std::make_unique<TrackingFileColumnIterator>(create_test_reader());
    auto* tracker = file_iterator.get();
    return {std::make_unique<OffsetFileColumnIterator>(std::move(file_iterator)), tracker};
}
} // namespace

static const std::string COLUMN_READER_FILE_TEST_DIR = "./ut_dir/column_reader_test";

class ColumnReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _old_disable_storage_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = true;
        auto st = io::global_local_filesystem()->delete_directory(COLUMN_READER_FILE_TEST_DIR);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = io::global_local_filesystem()->create_directory(COLUMN_READER_FILE_TEST_DIR);
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    void TearDown() override {
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_directory(COLUMN_READER_FILE_TEST_DIR).ok());
        config::disable_storage_page_cache = _old_disable_storage_page_cache;
    }

private:
    bool _old_disable_storage_page_cache = false;
};

TEST_F(ColumnReaderTest, NullMapOnlyReadBySparseRowidsAcrossPages) {
    ColumnMetaPB meta;
    std::string fname = COLUMN_READER_FILE_TEST_DIR + "/null_map_only_sparse_rowids";
    auto fs = io::global_local_filesystem();

    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        ASSERT_TRUE(st.ok()) << st.to_string();

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(PLAIN_ENCODING);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.data_page_size = sizeof(int32_t) * 2;
        writer_opts.need_zone_map = false;

        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_INT);
        std::unique_ptr<ColumnWriter> writer;
        st = ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = writer->init();
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int32_t i = 0; i < 6; ++i) {
            st = writer->append(i == 2, &i);
            ASSERT_TRUE(st.ok()) << st.to_string();
        }

        st = writer->finish();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = writer->write_data();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = writer->write_ordinal_index();
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    io::FileReaderSPtr file_reader;
    auto st = fs->open_file(fname, &file_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ColumnReaderOptions reader_opts;
    std::shared_ptr<ColumnReader> reader;
    st = ColumnReader::create(reader_opts, meta, 6, file_reader, &reader);
    ASSERT_TRUE(st.ok()) << st.to_string();

    NullMapOnlyFileColumnIterator iter(reader);
    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    iter_opts.file_reader = file_reader.get();
    st = iter.init(iter_opts);
    ASSERT_TRUE(st.ok()) << st.to_string();
    iter.force_null_map_only();

    MutableColumnPtr dst = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    const rowid_t rowids[] = {0, 2};
    st = iter.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(2, dst->size());
    const auto& nullable_col = assert_cast<const ColumnNullable&>(*dst);
    const auto& null_map = nullable_col.get_null_map_data();
    ASSERT_EQ(2, null_map.size());
    EXPECT_EQ(0, null_map[0]);
    EXPECT_EQ(1, null_map[1]);
    EXPECT_EQ(2, nullable_col.get_nested_column().size());
}

TEST_F(ColumnReaderTest, StructAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto iterator = create_struct_iterator();
    auto st = iterator->set_access_paths(TColumnAccessPaths {}, TColumnAccessPaths {});

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_read_requirement, ColumnIterator::ReadRequirement::NORMAL);

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // empty paths leads to error
    ASSERT_FALSE(st.ok());

    // Only reading sub_col_1
    // sub_col_2 should be set to SKIP
    all_access_paths[0] = create_data_access_path({"self", "sub_col_1"});

    predicate_access_paths[0] = create_data_access_path({"self", "sub_col_1"});

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // invalid name leads to error
    ASSERT_FALSE(st.ok());

    iterator->set_column_name("self");
    // now column name is "self", should be ok
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::SKIP);

    // Reading all sub columns
    all_access_paths[0] = create_data_access_path({"self"});
    iterator = create_struct_iterator();
    iterator->set_column_name("self");
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
}

TEST_F(ColumnReaderTest, ReadPhaseMatrix) {
    TestColumnIterator iterator;

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::SKIP);
    iterator.set_read_phase(ColumnIterator::ReadPhase::NORMAL);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::NORMAL);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::NORMAL);
    EXPECT_FALSE(iterator.need_to_read());
    EXPECT_FALSE(iterator.need_to_read_meta_columns());

    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_TRUE(iterator.need_to_read());
    EXPECT_TRUE(iterator.need_to_read_meta_columns());
}

TEST_F(ColumnReaderTest, ReadRequirementPriorityAndLazyOutput) {
    TestColumnIterator iterator;

    iterator.set_read_requirement(ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(iterator.read_requirement(), ColumnIterator::ReadRequirement::SKIP);

    iterator.set_lazy_output_requirement();
    EXPECT_EQ(iterator.read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    iterator.set_read_requirement(ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(iterator.read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    iterator.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(iterator.read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);

    iterator.set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_EQ(iterator.read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, MetaReadModePrefersOffsetOverNull) {
    auto assert_meta_read_mode = [](TColumnAccessPaths access_paths, bool offset_only,
                                    bool null_map_only) {
        TestColumnIterator iterator;
        iterator.set_column_name("self");
        auto st = iterator.check_and_set_meta_read_mode(ColumnIterator::ReadRequirement::NORMAL,
                                                        access_paths);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(iterator.read_offset_only(), offset_only);
        EXPECT_EQ(iterator.read_null_map_only(), null_map_only);
    };

    assert_meta_read_mode(TColumnAccessPaths {}, false, false);
    assert_meta_read_mode(
            TColumnAccessPaths {create_meta_access_path({"self", ColumnIterator::ACCESS_OFFSET})},
            true, false);
    assert_meta_read_mode(
            TColumnAccessPaths {create_meta_access_path({"self", ColumnIterator::ACCESS_NULL})},
            false, true);
    assert_meta_read_mode(
            TColumnAccessPaths {create_meta_access_path({"self", ColumnIterator::ACCESS_OFFSET}),
                                create_meta_access_path({"self", ColumnIterator::ACCESS_NULL})},
            true, false);
    assert_meta_read_mode(TColumnAccessPaths {create_data_access_path({"self", "child"})}, false,
                          false);
    assert_meta_read_mode(
            TColumnAccessPaths {create_data_access_path({"self", ColumnIterator::ACCESS_OFFSET})},
            false, false);
    assert_meta_read_mode(
            TColumnAccessPaths {create_data_access_path({"self", ColumnIterator::ACCESS_NULL})},
            false, false);
    assert_meta_read_mode(
            TColumnAccessPaths {create_meta_access_path({"self", ColumnIterator::ACCESS_OFFSET}),
                                create_data_access_path({"self", "child"})},
            false, false);

    {
        TestColumnIterator iterator;
        iterator.set_column_name("self");
        auto st = iterator.check_and_set_meta_read_mode(
                ColumnIterator::ReadRequirement::LAZY_OUTPUT,
                TColumnAccessPaths {
                        create_meta_access_path({"self", ColumnIterator::ACCESS_NULL})});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_FALSE(iterator.read_null_map_only());
        EXPECT_FALSE(iterator.read_offset_only());
    }
}

TEST_F(ColumnReaderTest, TypedAccessPathRequiresMatchingPayload) {
    FileColumnIterator iterator(create_test_reader(true));
    iterator.set_column_name("c");

    TColumnAccessPath unknown_type_path;
    unknown_type_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    TDataAccessPath data_access_path;
    data_access_path.__set_path({"c"});
    unknown_type_path.__set_data_access_path(data_access_path);
    auto st = iterator.set_access_paths({unknown_type_path}, {});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), ::testing::HasSubstr("Invalid access path type"));

    TColumnAccessPath missing_meta_payload;
    missing_meta_payload.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    missing_meta_payload.__set_type(TAccessPathType::META);
    missing_meta_payload.__set_data_access_path(data_access_path);
    st = iterator.set_access_paths({missing_meta_payload}, {});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), ::testing::HasSubstr("meta_access_path payload is not set"));

    TColumnAccessPath missing_data_payload;
    missing_data_payload.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    missing_data_payload.__set_type(TAccessPathType::DATA);
    TMetaAccessPath meta_access_path;
    meta_access_path.__set_path({"c", ColumnIterator::ACCESS_NULL});
    missing_data_payload.__set_meta_access_path(meta_access_path);
    st = iterator.set_access_paths({missing_data_payload}, {});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), ::testing::HasSubstr("data_access_path payload is not set"));

    TColumnAccessPath missing_legacy_payload;
    missing_legacy_payload.__set_type(TAccessPathType::DATA);
    st = iterator.set_access_paths({missing_legacy_payload}, {});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(),
                ::testing::HasSubstr(
                        "Invalid legacy access path: data_access_path payload is not set"));

    for (const bool explicit_legacy_version : {false, true}) {
        TColumnAccessPath invalid_legacy_type;
        invalid_legacy_type.__set_type(TAccessPathType::META);
        invalid_legacy_type.__set_data_access_path(data_access_path);
        invalid_legacy_type.__set_meta_access_path(meta_access_path);
        if (explicit_legacy_version) {
            invalid_legacy_type.__set_version(
                    g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
        }
        st = iterator.set_access_paths({invalid_legacy_type}, {});
        EXPECT_FALSE(st.ok());
        EXPECT_THAT(st.to_string(), ::testing::HasSubstr("Invalid legacy access path type"));
    }

    FileColumnIterator data_precedence_iterator(create_test_reader(true));
    data_precedence_iterator.set_column_name("c");
    auto compatible_data_path = create_data_access_path({"c"});
    compatible_data_path.__set_meta_access_path(meta_access_path);
    st = data_precedence_iterator.set_access_paths({compatible_data_path}, {});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(data_precedence_iterator.read_null_map_only());
    EXPECT_FALSE(data_precedence_iterator.read_offset_only());

    auto compatible_meta_path = create_meta_access_path({"c", ColumnIterator::ACCESS_NULL});
    compatible_meta_path.__set_data_access_path(data_access_path);
    st = iterator.set_access_paths({compatible_meta_path}, {compatible_meta_path});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(iterator.read_null_map_only());

    FileColumnIterator wrong_root_iterator(create_test_reader(true));
    wrong_root_iterator.set_column_name("c");
    auto wrong_root_path = create_meta_access_path({"other", ColumnIterator::ACCESS_NULL});
    st = wrong_root_iterator.set_access_paths({}, {wrong_root_path});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), ::testing::HasSubstr("expected name \"c\", got \"other\""));

    auto unsupported_version_path = create_data_access_path({"c"});
    unsupported_version_path.__set_version(
            g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED + 1);
    st = iterator.set_access_paths({unsupported_version_path}, {});
    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), ::testing::HasSubstr("Unsupported access path version"));
}

TEST_F(ColumnReaderTest, AccessPathVersionControlsLegacyMetaEncoding) {
    for (const bool explicit_legacy_version : {false, true}) {
        SCOPED_TRACE(explicit_legacy_version ? "explicit-version-0" : "missing-version");
        FileColumnIterator iterator(create_test_reader(true));
        iterator.set_column_name("c");
        auto legacy_null_path = create_legacy_data_access_path({"c", ColumnIterator::ACCESS_NULL});
        if (explicit_legacy_version) {
            legacy_null_path.__set_version(
                    g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
        }
        auto st = iterator.set_access_paths({legacy_null_path}, {legacy_null_path});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(iterator.read_null_map_only());
    }

    {
        FileColumnIterator iterator(create_test_reader(true));
        iterator.set_column_name("c");
        auto typed_data_path = create_data_access_path({"c", ColumnIterator::ACCESS_NULL});
        auto st = iterator.set_access_paths({typed_data_path}, {typed_data_path});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_FALSE(iterator.read_null_map_only());
        EXPECT_FALSE(iterator.read_offset_only());
    }
}

TEST_F(ColumnReaderTest, LegacyDataSpecialComponentsRemainDataSelectors) {
    auto make_map_iterator = []() {
        auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(create_test_reader()));
        auto key_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        auto value_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                create_test_reader(), nullptr, std::move(offsets_iterator), std::move(key_iterator),
                std::move(value_iterator));
        map_iterator->set_column_name("m");
        return map_iterator;
    };

    struct SelectorCase {
        const char* component;
        bool explicit_legacy_version;
        ColumnIterator::ReadRequirement key_requirement;
        ColumnIterator::ReadRequirement value_requirement;
    };
    for (const auto& test_case : {SelectorCase {ColumnIterator::ACCESS_MAP_KEYS, false,
                                                ColumnIterator::ReadRequirement::LAZY_OUTPUT,
                                                ColumnIterator::ReadRequirement::SKIP},
                                  SelectorCase {ColumnIterator::ACCESS_MAP_VALUES, true,
                                                ColumnIterator::ReadRequirement::SKIP,
                                                ColumnIterator::ReadRequirement::LAZY_OUTPUT}}) {
        SCOPED_TRACE(test_case.component);
        auto map_iterator = make_map_iterator();
        auto path = create_legacy_data_access_path({"m", test_case.component});
        if (test_case.explicit_legacy_version) {
            path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
        }

        auto st = map_iterator->set_access_paths({path}, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_FALSE(map_iterator->read_null_map_only());
        EXPECT_FALSE(map_iterator->read_offset_only());
        EXPECT_EQ(map_iterator->_key_iterator->read_requirement(), test_case.key_requirement);
        EXPECT_EQ(map_iterator->_val_iterator->read_requirement(), test_case.value_requirement);
    }

    // `*` is also a legacy DATA selector. It keeps keys readable while OFFSET is promoted only
    // after the remaining path reaches the value iterator.
    auto map_iterator = make_map_iterator();
    auto offset_path = create_legacy_data_access_path(
            {"m", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET});
    offset_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    auto st = map_iterator->set_access_paths({offset_path}, {});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(map_iterator->_key_iterator->read_requirement(),
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_TRUE(map_iterator->_val_iterator->read_offset_only());
}

TEST_F(ColumnReaderTest, TypedMetaPathsRouteThroughScalarAndComplexIterators) {
    {
        FileColumnIterator scalar_iterator(create_test_reader(true));
        scalar_iterator.set_column_name("i");
        TColumnAccessPaths null_path {create_meta_access_path({"i", ColumnIterator::ACCESS_NULL})};
        auto st = scalar_iterator.set_access_paths(null_path, null_path);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(scalar_iterator.read_null_map_only());
    }

    {
        auto item_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        item_iterator->set_column_name("item");
        auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(create_test_reader()));
        ArrayFileColumnIterator array_iterator(create_test_reader(), std::move(offsets_iterator),
                                               std::move(item_iterator), nullptr);
        array_iterator.set_column_name("a");

        TColumnAccessPaths offset_path {
                create_meta_access_path({"a", ColumnIterator::ACCESS_OFFSET})};
        auto st = array_iterator.set_access_paths(offset_path, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(array_iterator.read_offset_only());
        EXPECT_EQ(array_iterator._item_iterator->read_requirement(),
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(create_test_reader()));
        auto key_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        auto value_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        MapFileColumnIterator map_iterator(create_test_reader(), nullptr,
                                           std::move(offsets_iterator), std::move(key_iterator),
                                           std::move(value_iterator));
        map_iterator.set_column_name("m");

        TColumnAccessPaths offset_path {
                create_meta_access_path({"m", ColumnIterator::ACCESS_OFFSET})};
        auto st = map_iterator.set_access_paths(offset_path, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(map_iterator.read_offset_only());
        EXPECT_EQ(map_iterator._key_iterator->read_requirement(),
                  ColumnIterator::ReadRequirement::SKIP);
        EXPECT_EQ(map_iterator._val_iterator->read_requirement(),
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        std::vector<ColumnIteratorUPtr> sub_iterators;
        auto string_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        string_iterator->set_column_name("text");
        sub_iterators.emplace_back(std::move(string_iterator));
        StructFileColumnIterator struct_iterator(create_test_reader(), nullptr,
                                                 std::move(sub_iterators));
        struct_iterator.set_column_name("s");

        TColumnAccessPaths offset_path {
                create_meta_access_path({"s", "text", ColumnIterator::ACCESS_OFFSET})};
        auto st = struct_iterator.set_access_paths(offset_path, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        auto* text_iterator = static_cast<StringFileColumnIterator*>(
                struct_iterator._sub_column_iterators[0].get());
        EXPECT_TRUE(text_iterator->read_offset_only());
    }

    {
        auto item_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        item_iterator->set_column_name("item");
        auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(create_test_reader()));
        ArrayFileColumnIterator array_iterator(create_test_reader(), std::move(offsets_iterator),
                                               std::move(item_iterator), nullptr);
        array_iterator.set_column_name("a");

        TColumnAccessPaths offset_path {create_meta_access_path(
                {"a", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET})};
        auto st = array_iterator.set_access_paths(offset_path, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        auto* typed_item_iterator =
                static_cast<StringFileColumnIterator*>(array_iterator._item_iterator.get());
        EXPECT_TRUE(typed_item_iterator->read_offset_only());
    }

    {
        auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(create_test_reader()));
        auto key_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        auto value_iterator = std::make_unique<StringFileColumnIterator>(create_test_reader());
        MapFileColumnIterator map_iterator(create_test_reader(), nullptr,
                                           std::move(offsets_iterator), std::move(key_iterator),
                                           std::move(value_iterator));
        map_iterator.set_column_name("m");

        TColumnAccessPaths offset_path {create_meta_access_path(
                {"m", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET})};
        auto st = map_iterator.set_access_paths(offset_path, {});
        ASSERT_TRUE(st.ok()) << st.to_string();
        auto* typed_key_iterator =
                static_cast<StringFileColumnIterator*>(map_iterator._key_iterator.get());
        auto* typed_value_iterator =
                static_cast<StringFileColumnIterator*>(map_iterator._val_iterator.get());
        EXPECT_FALSE(typed_key_iterator->read_offset_only());
        EXPECT_EQ(typed_key_iterator->read_requirement(),
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_TRUE(typed_value_iterator->read_offset_only());
    }
}

TEST_F(ColumnReaderTest, TypedDataFieldsNamedMetaComponentsAreNotTreatedAsMetaPaths) {
    auto make_struct_iterator = [](const std::string& field_name) {
        std::vector<ColumnIteratorUPtr> sub_iterators;
        auto field_iterator = std::make_unique<FileColumnIterator>(create_test_reader());
        field_iterator->set_column_name(field_name);
        sub_iterators.emplace_back(std::move(field_iterator));
        auto struct_iterator = std::make_unique<StructFileColumnIterator>(
                create_test_reader(), nullptr, std::move(sub_iterators));
        struct_iterator->set_column_name("s");
        return struct_iterator;
    };

    for (const std::string field_name :
         {ColumnIterator::ACCESS_NULL, ColumnIterator::ACCESS_OFFSET}) {
        SCOPED_TRACE(field_name);
        auto struct_iterator = make_struct_iterator(field_name);
        TColumnAccessPaths data_path {create_data_access_path({"s", field_name})};
        auto st = struct_iterator->set_access_paths(data_path, data_path);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_FALSE(struct_iterator->read_null_map_only());
        EXPECT_FALSE(struct_iterator->read_offset_only());
        ASSERT_EQ(struct_iterator->_sub_column_iterators.size(), 1);
        EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->read_requirement(),
                  ColumnIterator::ReadRequirement::PREDICATE);
    }

    auto struct_iterator = make_struct_iterator(ColumnIterator::ACCESS_NULL);
    TColumnAccessPaths meta_path {create_meta_access_path({"s", ColumnIterator::ACCESS_NULL})};
    auto st = struct_iterator->set_access_paths(meta_path, meta_path);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(struct_iterator->read_null_map_only());
    EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->read_requirement(),
              ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, LegacyStructMetaComponentsRemainSentinels) {
    auto null_iterator = std::make_unique<FileColumnIterator>(create_test_reader());
    std::vector<ColumnIteratorUPtr> sub_iterators;
    auto field_iterator = std::make_unique<FileColumnIterator>(create_test_reader());
    field_iterator->set_column_name(ColumnIterator::ACCESS_NULL);
    sub_iterators.emplace_back(std::move(field_iterator));
    StructFileColumnIterator struct_iterator(create_test_reader(), std::move(null_iterator),
                                             std::move(sub_iterators));
    struct_iterator.set_column_name("s");

    auto legacy_path = create_legacy_data_access_path({"s", ColumnIterator::ACCESS_NULL});
    legacy_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    auto st = struct_iterator.set_access_paths({legacy_path}, {legacy_path});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(struct_iterator.read_null_map_only());
    EXPECT_EQ(struct_iterator._sub_column_iterators[0]->read_requirement(),
              ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, PlaceHolderLifecycleInLazyMode) {
    TestColumnIterator iterator;
    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnInt32::create();
    iterator.convert_to_place_holder_column(dst, 3);

    EXPECT_EQ(3, dst->size());
    EXPECT_TRUE(iterator._has_place_holder_column);

    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    iterator.finalize_lazy_phase(dst);
    EXPECT_EQ(0, dst->size());
    EXPECT_FALSE(iterator._has_place_holder_column);

    MutableColumnPtr lazy_dst = ColumnInt32::create();
    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    iterator.convert_to_place_holder_column(lazy_dst, 4);
    EXPECT_EQ(0, lazy_dst->size());
}

TEST_F(ColumnReaderTest, PlaceHolderRecoveryAfterColumnReplacement) {
    TestColumnIterator iterator;
    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnInt32::create();
    iterator.convert_to_place_holder_column(dst, 3);
    EXPECT_TRUE(iterator._has_place_holder_column);

    IColumn::Filter filter;
    filter.resize(3);
    filter[0] = 1;
    filter[1] = 0;
    filter[2] = 1;
    dst = IColumn::mutate(dst->filter(filter, 2));
    EXPECT_EQ(2, dst->size());

    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    iterator.finalize_lazy_phase(dst);
    EXPECT_EQ(0, dst->size());
    EXPECT_FALSE(iterator._has_place_holder_column);

    dst->insert_many_defaults(2);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    iterator.finalize_lazy_phase(dst);
    EXPECT_EQ(2, dst->size());
}

namespace {
void check_default_value_lazy_output(bool read_by_rowids) {
    SCOPED_TRACE(read_by_rowids ? "read_by_rowids" : "next_batch");

    DefaultValueColumnIterator iterator(true, "7", false, FieldType::OLAP_FIELD_TYPE_INT, 0, 0,
                                        sizeof(int32_t));
    ColumnIteratorOptions iter_opts;
    ASSERT_TRUE(iterator.init(iter_opts).ok());
    iterator.set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnInt32::create();
    size_t rows = 3;
    bool has_null = false;
    ASSERT_TRUE(iterator.next_batch(&rows, dst, &has_null).ok());
    EXPECT_TRUE(iterator._has_place_holder_column);

    IColumn::Filter filter;
    filter.resize(3);
    filter[0] = 1;
    filter[1] = 0;
    filter[2] = 1;
    dst = IColumn::mutate(dst->filter(filter, 2));
    ASSERT_EQ(2, dst->size());

    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    if (read_by_rowids) {
        const rowid_t rowids[] = {2, 8};
        ASSERT_TRUE(iterator.read_by_rowids(rowids, std::size(rowids), dst).ok());
    } else {
        rows = 2;
        ASSERT_TRUE(iterator.next_batch(&rows, dst, &has_null).ok());
    }

    ASSERT_EQ(2, dst->size());
    EXPECT_FALSE(iterator._has_place_holder_column);
    const auto& result = assert_cast<const ColumnInt32&>(*dst);
    EXPECT_EQ(7, result.get_element(0));
    EXPECT_EQ(7, result.get_element(1));
}

void check_default_value_predicate_not_read_again(bool read_by_rowids) {
    SCOPED_TRACE(read_by_rowids ? "read_by_rowids" : "next_batch");

    DefaultValueColumnIterator iterator(true, "7", false, FieldType::OLAP_FIELD_TYPE_INT, 0, 0,
                                        sizeof(int32_t));
    ColumnIteratorOptions iter_opts;
    ASSERT_TRUE(iterator.init(iter_opts).ok());
    iterator.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnInt32::create();
    size_t rows = 3;
    bool has_null = true;
    ASSERT_TRUE(iterator.next_batch(&rows, dst, &has_null).ok());
    EXPECT_FALSE(has_null);
    EXPECT_FALSE(iterator._has_place_holder_column);

    IColumn::Filter filter;
    filter.resize(3);
    filter[0] = 1;
    filter[1] = 0;
    filter[2] = 1;
    dst = IColumn::mutate(dst->filter(filter, 2));
    ASSERT_EQ(2, dst->size());

    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    if (read_by_rowids) {
        const rowid_t rowids[] = {2, 8};
        ASSERT_TRUE(iterator.read_by_rowids(rowids, std::size(rowids), dst).ok());
    } else {
        rows = 2;
        ASSERT_TRUE(iterator.next_batch(&rows, dst, &has_null).ok());
    }

    ASSERT_EQ(2, dst->size());
    const auto& result = assert_cast<const ColumnInt32&>(*dst);
    EXPECT_EQ(7, result.get_element(0));
    EXPECT_EQ(7, result.get_element(1));
}
} // namespace

TEST_F(ColumnReaderTest, DefaultValueLazyOutputRecoversFilteredPlaceholder) {
    check_default_value_lazy_output(false);
    check_default_value_lazy_output(true);
}

TEST_F(ColumnReaderTest, DefaultValueLazyOutputFinalizesEmptySelection) {
    DefaultValueColumnIterator iterator(true, "7", false, FieldType::OLAP_FIELD_TYPE_INT, 0, 0,
                                        sizeof(int32_t));
    ColumnIteratorOptions iter_opts;
    ASSERT_TRUE(iterator.init(iter_opts).ok());
    iterator.set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnInt32::create();
    size_t rows = 3;
    bool has_null = false;
    ASSERT_TRUE(iterator.next_batch(&rows, dst, &has_null).ok());
    EXPECT_TRUE(iterator._has_place_holder_column);

    IColumn::Filter filter;
    filter.resize(3);
    filter[0] = 0;
    filter[1] = 0;
    filter[2] = 0;
    dst = IColumn::mutate(dst->filter(filter, 0));
    ASSERT_EQ(0, dst->size());

    iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    iterator.finalize_lazy_phase(dst);
    EXPECT_EQ(0, dst->size());
    EXPECT_FALSE(iterator._has_place_holder_column);
}

TEST_F(ColumnReaderTest, DefaultValuePredicateIsNotReadAgainInLazyPhase) {
    check_default_value_predicate_not_read_again(false);
    check_default_value_predicate_not_read_again(true);
}

TEST_F(ColumnReaderTest, SetReadRequirementPropagatesToNestedIterators) {
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;

    auto sub_col = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_col->set_column_name("sub_col");
    struct_sub_iters.emplace_back(std::move(sub_col));

    auto array_item = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    array_item->set_column_name("item");
    auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_iter = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets), std::move(array_item),
            std::move(array_null));
    array_iter->set_column_name("arr");
    struct_sub_iters.emplace_back(std::move(array_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(), std::move(null_iter),
                                         std::move(struct_sub_iters));
    struct_iter.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);

    EXPECT_EQ(struct_iter.read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iter._sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto* nested_array =
            static_cast<ArrayFileColumnIterator*>(struct_iter._sub_column_iterators[1].get());
    EXPECT_EQ(nested_array->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(nested_array->_item_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iter(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                   std::move(map_offsets_iter), std::move(map_key_iter),
                                   std::move(map_val_iter));
    map_iter.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iter._key_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iter._val_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, SetReadRequirementSelfKeepsNestedIteratorRequirements) {
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_iter->set_column_name("sub_col");
    struct_sub_iters.emplace_back(std::move(sub_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(), std::move(null_iter),
                                         std::move(struct_sub_iters));
    struct_iter.set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);

    EXPECT_EQ(struct_iter.read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iter._sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::NORMAL);
}

TEST_F(ColumnReaderTest, RemovePrunedSubIterators) {
    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_keep = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_keep->set_column_name("keep");
    auto sub_prune = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_prune->set_column_name("prune");
    sub_prune->set_read_requirement(ColumnIterator::ReadRequirement::SKIP);
    struct_sub_iters.emplace_back(std::move(sub_keep));
    struct_sub_iters.emplace_back(std::move(sub_prune));

    auto array_item_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> item_struct_sub_iters;
    auto item_keep = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    item_keep->set_column_name("keep");
    auto item_prune = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    item_prune->set_column_name("prune");
    item_prune->set_read_requirement(ColumnIterator::ReadRequirement::SKIP);
    item_struct_sub_iters.emplace_back(std::move(item_keep));
    item_struct_sub_iters.emplace_back(std::move(item_prune));
    auto item_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                  std::move(array_item_null),
                                                                  std::move(item_struct_sub_iters));

    auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_iter = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets), std::move(item_struct),
            std::move(array_null));
    struct_sub_iters.emplace_back(std::move(array_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(),
                                         std::move(struct_null_iter), std::move(struct_sub_iters));
    ASSERT_EQ(3, struct_iter._sub_column_iterators.size());
    struct_iter.remove_pruned_sub_iterators();
    ASSERT_EQ(2, struct_iter._sub_column_iterators.size());

    auto* nested_array =
            static_cast<ArrayFileColumnIterator*>(struct_iter._sub_column_iterators[1].get());
    auto* nested_struct =
            static_cast<StructFileColumnIterator*>(nested_array->_item_iterator.get());
    ASSERT_EQ(1, nested_struct->_sub_column_iterators.size());
    EXPECT_EQ(nested_struct->_sub_column_iterators[0]->column_name(), "keep");
}

TEST_F(ColumnReaderTest, FinalizeLazyModeOnNestedStruct) {
    auto sub_iter = std::make_unique<TestColumnIterator>();
    auto* sub_iter_ptr = sub_iter.get();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> sub_iters;
    sub_iters.emplace_back(std::move(sub_iter));

    StructFileColumnIterator struct_iter(std::make_shared<ColumnReader>(), std::move(null_iter),
                                         std::move(sub_iters));
    sub_iter_ptr->set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    struct_iter.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    sub_iter_ptr->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr nested_column = ColumnInt32::create();
    MutableColumnPtr nested_mut = IColumn::mutate(std::move(nested_column));
    sub_iter_ptr->convert_to_place_holder_column(nested_mut, 5);
    EXPECT_EQ(5, nested_mut->size());

    Columns struct_columns;
    struct_columns.emplace_back(std::move(nested_mut));
    auto struct_column = ColumnStruct::create(struct_columns);
    MutableColumnPtr struct_mut = std::move(struct_column);
    struct_iter.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    sub_iter_ptr->set_read_phase(ColumnIterator::ReadPhase::LAZY);
    struct_iter.finalize_lazy_phase(struct_mut);

    auto& column_struct = assert_cast<ColumnStruct&, TypeCheckOnRelease::DISABLE>(*struct_mut);
    auto nested_after = column_struct.get_column_ptr(0);
    EXPECT_EQ(0, nested_after->size());
}

TEST_F(ColumnReaderTest, SplitAccessPathsClassifiesCurrentAndDescendantPaths) {
    TestColumnIterator iterator;
    iterator.set_column_name("self");
    iterator.force_set_read_requirement(ColumnIterator::ReadRequirement::NORMAL);

    {
        auto split = TEST_TRY(iterator.split_access_paths(
                TColumnAccessPaths {create_data_access_path({"self"})}));
        EXPECT_TRUE(split.reads_current_data);
        EXPECT_EQ(split.current_meta_mode, ColumnIterator::MetaReadMode::DEFAULT);
        EXPECT_TRUE(split.descendant_paths.empty());
    }

    {
        auto split = TEST_TRY(iterator.split_access_paths(TColumnAccessPaths {
                create_meta_access_path({"self", ColumnIterator::ACCESS_NULL}),
                create_meta_access_path({"self", ColumnIterator::ACCESS_OFFSET})}));
        EXPECT_FALSE(split.reads_current_data);
        EXPECT_EQ(split.current_meta_mode, ColumnIterator::MetaReadMode::OFFSET_ONLY);
        EXPECT_TRUE(split.descendant_paths.empty());
    }

    {
        auto split = TEST_TRY(iterator.split_access_paths(TColumnAccessPaths {
                create_data_access_path({"self", "child"}),
                create_meta_access_path({"self", "child", ColumnIterator::ACCESS_NULL})}));
        EXPECT_FALSE(split.reads_current_data);
        EXPECT_EQ(split.current_meta_mode, ColumnIterator::MetaReadMode::DEFAULT);
        ASSERT_EQ(split.descendant_paths.size(), 2);
        EXPECT_EQ(split.descendant_paths[0].type, TAccessPathType::DATA);
        EXPECT_EQ(split.descendant_paths[0].data_access_path.path,
                  (std::vector<std::string> {"child"}));
        EXPECT_EQ(split.descendant_paths[1].type, TAccessPathType::META);
        EXPECT_EQ(split.descendant_paths[1].meta_access_path.path,
                  (std::vector<std::string> {"child", ColumnIterator::ACCESS_NULL}));
    }

    {
        auto legacy_null = create_legacy_data_access_path({"self", ColumnIterator::ACCESS_NULL});
        auto legacy_offset =
                create_legacy_data_access_path({"self", ColumnIterator::ACCESS_OFFSET});
        legacy_offset.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
        auto split = TEST_TRY(iterator.split_access_paths(
                TColumnAccessPaths {std::move(legacy_null), std::move(legacy_offset)}));
        EXPECT_FALSE(split.reads_current_data);
        EXPECT_EQ(split.current_meta_mode, ColumnIterator::MetaReadMode::OFFSET_ONLY);
        EXPECT_TRUE(split.descendant_paths.empty());
    }

    {
        auto legacy_keys =
                create_legacy_data_access_path({"self", ColumnIterator::ACCESS_MAP_KEYS});
        auto split = TEST_TRY(iterator.split_access_paths(
                TColumnAccessPaths {create_data_access_path({"self", ColumnIterator::ACCESS_NULL}),
                                    std::move(legacy_keys)}));
        EXPECT_FALSE(split.reads_current_data);
        EXPECT_EQ(split.current_meta_mode, ColumnIterator::MetaReadMode::DEFAULT);
        ASSERT_EQ(split.descendant_paths.size(), 2);
        EXPECT_EQ(split.descendant_paths[0].data_access_path.path,
                  (std::vector<std::string> {ColumnIterator::ACCESS_NULL}));
        EXPECT_EQ(split.descendant_paths[1].data_access_path.path,
                  (std::vector<std::string> {ColumnIterator::ACCESS_MAP_KEYS}));
    }

    // Splitting is a pure classification step. Callers explicitly apply lazy/predicate state.
    EXPECT_EQ(iterator._read_requirement, ColumnIterator::ReadRequirement::NORMAL);
}

TEST_F(ColumnReaderTest, MapChildPathRoutingUsesLogicalSelectorsAndPreservesVersion) {
    auto key_iterator = std::make_unique<TrackingColumnIterator>();
    key_iterator->set_column_name("physical_key");
    auto* key = key_iterator.get();
    auto value_iterator = std::make_unique<TrackingColumnIterator>();
    value_iterator->set_column_name("physical_value");
    auto* value = value_iterator.get();
    auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(create_test_reader()));
    MapFileColumnIterator map_iterator(create_test_reader(), nullptr, std::move(offsets_iterator),
                                       std::move(key_iterator), std::move(value_iterator));
    map_iterator.set_column_name("m");
    EXPECT_EQ(key->column_name(), ColumnIterator::ACCESS_MAP_KEYS);
    EXPECT_EQ(value->column_name(), ColumnIterator::ACCESS_MAP_VALUES);

    auto path = create_meta_access_path(
            {"m", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET});
    TDataAccessPath unselected_data_payload;
    unselected_data_payload.__set_path(path.meta_access_path.path);
    path.__set_data_access_path(std::move(unselected_data_payload));
    auto legacy_path = create_data_access_path(
            {"m", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET});
    legacy_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);

    auto st = map_iterator.set_access_paths({path}, {legacy_path});
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(key->routed_all_access_paths.size(), 1);
    const auto& key_path = key->routed_all_access_paths[0];
    EXPECT_EQ(key_path.type, TAccessPathType::DATA);
    ASSERT_TRUE(key_path.__isset.version);
    EXPECT_EQ(key_path.version, g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    EXPECT_EQ(key_path.data_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_KEYS}));
    ASSERT_EQ(key->routed_predicate_access_paths.size(), 1);
    const auto& legacy_key_path = key->routed_predicate_access_paths[0];
    EXPECT_EQ(legacy_key_path.type, TAccessPathType::DATA);
    ASSERT_TRUE(legacy_key_path.__isset.version);
    EXPECT_EQ(legacy_key_path.version, g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    EXPECT_EQ(legacy_key_path.data_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_KEYS}));

    ASSERT_EQ(value->routed_all_access_paths.size(), 1);
    const auto& value_path = value->routed_all_access_paths[0];
    EXPECT_EQ(value_path.type, TAccessPathType::META);
    ASSERT_TRUE(value_path.__isset.version);
    EXPECT_EQ(value_path.version, g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    EXPECT_EQ(value_path.meta_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_VALUES,
                                         ColumnIterator::ACCESS_OFFSET}));
    EXPECT_EQ(value_path.data_access_path.path,
              (std::vector<std::string> {"m", ColumnIterator::ACCESS_ALL,
                                         ColumnIterator::ACCESS_OFFSET}));
    ASSERT_EQ(value->routed_predicate_access_paths.size(), 1);
    const auto& legacy_value_path = value->routed_predicate_access_paths[0];
    EXPECT_EQ(legacy_value_path.type, TAccessPathType::DATA);
    ASSERT_TRUE(legacy_value_path.__isset.version);
    EXPECT_EQ(legacy_value_path.version,
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    EXPECT_EQ(legacy_value_path.data_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_VALUES,
                                         ColumnIterator::ACCESS_OFFSET}));
}

TEST_F(ColumnReaderTest, NestedMapWildcardRoutingUsesLogicalSelectorsWithoutPhysicalNames) {
    auto inner_key_iterator = std::make_unique<TrackingColumnIterator>();
    auto* inner_key = inner_key_iterator.get();
    auto inner_value_iterator = std::make_unique<TrackingColumnIterator>();
    auto* inner_value = inner_value_iterator.get();
    auto inner_offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(create_test_reader()));
    auto inner_map_iterator = std::make_unique<MapFileColumnIterator>(
            create_test_reader(), nullptr, std::move(inner_offsets_iterator),
            std::move(inner_key_iterator), std::move(inner_value_iterator));

    auto outer_key_iterator = std::make_unique<TrackingColumnIterator>();
    auto* outer_key = outer_key_iterator.get();
    auto outer_offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(create_test_reader()));
    MapFileColumnIterator outer_map_iterator(
            create_test_reader(), nullptr, std::move(outer_offsets_iterator),
            std::move(outer_key_iterator), std::move(inner_map_iterator));
    outer_map_iterator.set_column_name("m");

    auto path =
            create_meta_access_path({"m", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_ALL,
                                     ColumnIterator::ACCESS_OFFSET});
    auto st = outer_map_iterator.set_access_paths({path}, {});
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(outer_key->routed_all_access_paths.size(), 1);
    EXPECT_EQ(outer_key->routed_all_access_paths[0].data_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_KEYS}));
    ASSERT_EQ(inner_key->routed_all_access_paths.size(), 1);
    EXPECT_EQ(inner_key->routed_all_access_paths[0].data_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_KEYS}));
    ASSERT_EQ(inner_value->routed_all_access_paths.size(), 1);
    EXPECT_EQ(inner_value->routed_all_access_paths[0].meta_access_path.path,
              (std::vector<std::string> {ColumnIterator::ACCESS_MAP_VALUES,
                                         ColumnIterator::ACCESS_OFFSET}));
}

TEST_F(ColumnReaderTest, ArrayItemPathRoutingRewritesOnlySelectedPayload) {
    auto item_iterator = std::make_unique<TrackingColumnIterator>();
    item_iterator->set_column_name("item");
    auto* item = item_iterator.get();
    auto offsets_iterator = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(create_test_reader()));
    ArrayFileColumnIterator array_iterator(create_test_reader(), std::move(offsets_iterator),
                                           std::move(item_iterator), nullptr);
    array_iterator.set_column_name("a");

    auto path = create_meta_access_path(
            {"a", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET});
    TDataAccessPath unselected_data_payload;
    unselected_data_payload.__set_path(path.meta_access_path.path);
    path.__set_data_access_path(std::move(unselected_data_payload));

    auto st = array_iterator.set_access_paths({path}, {});
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(item->routed_all_access_paths.size(), 1);
    const auto& item_path = item->routed_all_access_paths[0];
    EXPECT_EQ(item_path.type, TAccessPathType::META);
    ASSERT_TRUE(item_path.__isset.version);
    EXPECT_EQ(item_path.version, g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    EXPECT_EQ(item_path.meta_access_path.path,
              (std::vector<std::string> {"item", ColumnIterator::ACCESS_OFFSET}));
    EXPECT_EQ(item_path.data_access_path.path,
              (std::vector<std::string> {"a", ColumnIterator::ACCESS_ALL,
                                         ColumnIterator::ACCESS_OFFSET}));
}

TEST_F(ColumnReaderTest, StructCurrentMetaDoesNotRouteToDataFieldWithSameName) {
    std::vector<ColumnIteratorUPtr> sub_iterators;
    auto null_field_iterator = std::make_unique<TrackingColumnIterator>();
    null_field_iterator->set_column_name(ColumnIterator::ACCESS_NULL);
    auto* null_field = null_field_iterator.get();
    sub_iterators.emplace_back(std::move(null_field_iterator));
    auto data_field_iterator = std::make_unique<TrackingColumnIterator>();
    data_field_iterator->set_column_name("data");
    auto* data_field = data_field_iterator.get();
    sub_iterators.emplace_back(std::move(data_field_iterator));

    StructFileColumnIterator struct_iterator(create_test_reader(), nullptr,
                                             std::move(sub_iterators));
    struct_iterator.set_column_name("s");
    TColumnAccessPaths all_access_paths {
            create_meta_access_path({"s", ColumnIterator::ACCESS_NULL}),
            create_data_access_path({"s", "data"})};
    TColumnAccessPaths predicate_access_paths {create_data_access_path({"s", "data"})};

    auto st = struct_iterator.set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(null_field->routed_all_access_paths.empty());
    EXPECT_TRUE(null_field->routed_predicate_access_paths.empty());
    EXPECT_EQ(null_field->read_requirement(), ColumnIterator::ReadRequirement::SKIP);
    ASSERT_EQ(data_field->routed_all_access_paths.size(), 1);
    ASSERT_EQ(data_field->routed_predicate_access_paths.size(), 1);
    EXPECT_EQ(data_field->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, NestedIteratorsPropagateReadPhase) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto struct_iterator = std::make_unique<StructFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(struct_null_iterator),
            std::move(struct_sub_iters));

    struct_iterator->set_read_phase(ColumnIterator::ReadPhase::LAZY);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_read_phase,
              ColumnIterator::ReadPhase::LAZY);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_read_phase,
              ColumnIterator::ReadPhase::LAZY);

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    EXPECT_EQ(array_iterator._item_iterator->_read_phase, ColumnIterator::ReadPhase::PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    EXPECT_EQ(map_iterator._key_iterator->_read_phase, ColumnIterator::ReadPhase::LAZY);
    EXPECT_EQ(map_iterator._val_iterator->_read_phase, ColumnIterator::ReadPhase::LAZY);
}

TEST_F(ColumnReaderTest, AccessPathsPropagatePredicateToChildren) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto struct_iterator = std::make_unique<StructFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(struct_null_iterator),
            std::move(struct_sub_iters));
    struct_iterator->set_column_name("s");

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0] = create_data_access_path({"s"});
    TColumnAccessPaths predicate_access_paths = all_access_paths;

    auto st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();
    EXPECT_EQ(struct_iterator->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_column_name("a");
    TColumnAccessPaths array_access_paths;
    array_access_paths.emplace_back();
    array_access_paths[0] = create_data_access_path({"a"});
    TColumnAccessPaths array_predicate_paths = array_access_paths;
    st = array_iterator.set_access_paths(array_access_paths, array_predicate_paths);
    ASSERT_TRUE(st.ok()) << "failed to set array access paths: " << st.to_string();
    EXPECT_EQ(array_iterator._read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(array_iterator._item_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_column_name("m");
    TColumnAccessPaths map_access_paths;
    map_access_paths.emplace_back();
    map_access_paths[0] = create_data_access_path({"m"});
    TColumnAccessPaths map_predicate_paths = map_access_paths;
    st = map_iterator.set_access_paths(map_access_paths, map_predicate_paths);
    ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();
    EXPECT_EQ(map_iterator._read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iterator._key_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iterator._val_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, StructPredicateOnlyChildPathStillRoutesToChild) {
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> sub_iters;
    auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_a->set_column_name("a");
    auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_b->set_column_name("b");
    sub_iters.emplace_back(std::move(sub_a));
    sub_iters.emplace_back(std::move(sub_b));

    StructFileColumnIterator struct_iterator(std::make_shared<ColumnReader>(), std::move(null_iter),
                                             std::move(sub_iters));
    struct_iterator.set_column_name("s");

    TColumnAccessPaths all_access_paths {create_data_access_path({"s", "a"})};
    TColumnAccessPaths predicate_access_paths {create_data_access_path({"s", "b"})};

    auto st = struct_iterator.set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();

    EXPECT_EQ(struct_iterator._read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iterator._sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_EQ(struct_iterator._sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    struct_iterator.remove_pruned_sub_iterators();
    ASSERT_EQ(struct_iterator._sub_column_iterators.size(), 2);
    EXPECT_EQ(struct_iterator._sub_column_iterators[0]->column_name(), "a");
    EXPECT_EQ(struct_iterator._sub_column_iterators[1]->column_name(), "b");
}

TEST_F(ColumnReaderTest, LegacyCurrentLevelPredicateNullPathUsesMetaOnlyMode) {
    auto make_struct_iterator = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_a->set_column_name("a");
        auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_b->set_column_name("b");
        sub_iters.emplace_back(std::move(sub_a));
        sub_iters.emplace_back(std::move(sub_b));

        auto struct_iterator = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        struct_iterator->set_column_name("s");
        return struct_iterator;
    };

    {
        auto struct_iterator = make_struct_iterator();
        TColumnAccessPaths all_access_paths {
                create_legacy_data_access_path({"s", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_legacy_data_access_path({"s", ColumnIterator::ACCESS_NULL})};

        auto st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();

        EXPECT_TRUE(struct_iterator->read_null_map_only());
        EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
        EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);

        struct_iterator->remove_pruned_sub_iterators();
        EXPECT_TRUE(struct_iterator->_sub_column_iterators.empty());
    }

    {
        auto struct_iterator = make_struct_iterator();
        TColumnAccessPaths all_access_paths {
                create_legacy_data_access_path({"s"}),
                create_legacy_data_access_path({"s", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_legacy_data_access_path({"s", ColumnIterator::ACCESS_NULL})};

        auto st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();

        EXPECT_FALSE(struct_iterator->read_null_map_only());
        EXPECT_EQ(struct_iterator->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    }

    {
        auto array_item_iterator =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        ArrayFileColumnIterator array_iterator(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(array_item_iterator), std::move(array_null_iter));
        array_iterator.set_column_name("a");

        TColumnAccessPaths all_access_paths {
                create_legacy_data_access_path({"a", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_legacy_data_access_path({"a", ColumnIterator::ACCESS_NULL})};

        auto st = array_iterator.set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set array access paths: " << st.to_string();
        EXPECT_TRUE(array_iterator.read_null_map_only());
        EXPECT_EQ(array_iterator._item_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(),
                                           std::move(map_null_iter), std::move(map_offsets_iter),
                                           std::move(map_key_iter), std::move(map_val_iter));
        map_iterator.set_column_name("m");

        TColumnAccessPaths all_access_paths {
                create_legacy_data_access_path({"m", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_legacy_data_access_path({"m", ColumnIterator::ACCESS_NULL})};

        auto st = map_iterator.set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();
        EXPECT_TRUE(map_iterator.read_null_map_only());
        EXPECT_EQ(map_iterator._key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
        EXPECT_EQ(map_iterator._val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }
}

TEST_F(ColumnReaderTest, StructPredicateMetaPathDoesNotOverrideExistingDataNeed) {
    auto make_struct_iterator = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto city_iter =
                std::make_unique<StringFileColumnIterator>(std::make_shared<ColumnReader>());
        city_iter->set_column_name("city");
        auto data_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        data_iter->set_column_name("data");
        sub_iters.emplace_back(std::move(city_iter));
        sub_iters.emplace_back(std::move(data_iter));
        auto struct_iterator = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        struct_iterator->set_column_name("s");
        return struct_iterator;
    };

    auto struct_iterator = make_struct_iterator();
    TColumnAccessPaths all_access_paths {
            create_data_access_path({"s"}),
            create_meta_access_path({"s", "city", ColumnIterator::ACCESS_NULL})};
    TColumnAccessPaths predicate_access_paths {
            create_meta_access_path({"s", "city", ColumnIterator::ACCESS_NULL})};

    auto st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();

    auto* city_iter =
            static_cast<StringFileColumnIterator*>(struct_iterator->_sub_column_iterators[0].get());
    EXPECT_FALSE(city_iter->read_null_map_only());
    EXPECT_EQ(city_iter->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->read_requirement(),
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    struct_iterator = make_struct_iterator();
    all_access_paths = {create_meta_access_path({"s", "city", ColumnIterator::ACCESS_NULL})};
    predicate_access_paths = all_access_paths;
    st = struct_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set predicate-only struct access paths: " << st.to_string();
    city_iter =
            static_cast<StringFileColumnIterator*>(struct_iterator->_sub_column_iterators[0].get());
    EXPECT_TRUE(city_iter->read_null_map_only());
    EXPECT_EQ(struct_iterator->_sub_column_iterators[1]->read_requirement(),
              ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, StructSiblingDataPathDoesNotDisablePredicateMetaOnlyRead) {
    auto city_iterator = std::make_unique<StringFileColumnIterator>(
            create_test_reader(true, 0, FieldType::OLAP_FIELD_TYPE_STRING));
    city_iterator->set_column_name("city");
    auto* city_iterator_ptr = city_iterator.get();

    auto data_iterator = std::make_unique<FileColumnIterator>(create_test_reader());
    data_iterator->set_column_name("data");
    auto* data_iterator_ptr = data_iterator.get();

    std::vector<ColumnIteratorUPtr> sub_iterators;
    sub_iterators.emplace_back(std::move(city_iterator));
    sub_iterators.emplace_back(std::move(data_iterator));
    StructFileColumnIterator struct_iterator(
            create_test_reader(false, 0, FieldType::OLAP_FIELD_TYPE_STRUCT), nullptr,
            std::move(sub_iterators));
    struct_iterator.set_column_name("s");

    TColumnAccessPaths all_access_paths {
            create_data_access_path({"s", "data"}),
            create_meta_access_path({"s", "city", ColumnIterator::ACCESS_NULL})};
    TColumnAccessPaths predicate_access_paths {
            create_meta_access_path({"s", "city", ColumnIterator::ACCESS_NULL})};

    auto st = struct_iterator.set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(struct_iterator.read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(data_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_TRUE(city_iterator_ptr->read_null_map_only());
    EXPECT_EQ(city_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, ArrayPredicateMetaPathDoesNotOverrideExistingDataNeed) {
    auto make_array_iterator = []() {
        auto item_iter =
                std::make_unique<StringFileColumnIterator>(std::make_shared<ColumnReader>());
        item_iter->set_column_name("item");
        auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(offsets_iter), std::move(item_iter),
                std::move(null_iter));
        array_iterator->set_column_name("a");
        return array_iterator;
    };

    auto array_iterator = make_array_iterator();
    TColumnAccessPaths all_access_paths {create_data_access_path({"a"}),
                                         create_meta_access_path({"a", ColumnIterator::ACCESS_ALL,
                                                                  ColumnIterator::ACCESS_NULL})};
    TColumnAccessPaths predicate_access_paths {create_meta_access_path(
            {"a", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_NULL})};

    auto st = array_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set array access paths: " << st.to_string();
    auto* item_iter = static_cast<StringFileColumnIterator*>(array_iterator->_item_iterator.get());
    EXPECT_FALSE(item_iter->read_null_map_only());
    EXPECT_EQ(item_iter->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);

    array_iterator = make_array_iterator();
    all_access_paths = {create_meta_access_path(
            {"a", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_NULL})};
    predicate_access_paths = all_access_paths;
    st = array_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set predicate-only array access paths: " << st.to_string();
    item_iter = static_cast<StringFileColumnIterator*>(array_iterator->_item_iterator.get());
    EXPECT_TRUE(item_iter->read_null_map_only());
}

TEST_F(ColumnReaderTest, MapPredicateMetaPathDoesNotOverrideExistingDataNeed) {
    auto make_map_iterator = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto value_iter =
                std::make_unique<StringFileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(offsets_iter),
                std::move(key_iter), std::move(value_iter));
        map_iterator->set_column_name("m");
        return map_iterator;
    };

    auto map_iterator = make_map_iterator();
    TColumnAccessPaths all_access_paths {
            create_data_access_path({"m"}),
            create_meta_access_path(
                    {"m", ColumnIterator::ACCESS_MAP_VALUES, ColumnIterator::ACCESS_NULL})};
    TColumnAccessPaths predicate_access_paths {create_meta_access_path(
            {"m", ColumnIterator::ACCESS_MAP_VALUES, ColumnIterator::ACCESS_NULL})};

    auto st = map_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();
    auto* value_iter = static_cast<StringFileColumnIterator*>(map_iterator->_val_iterator.get());
    EXPECT_FALSE(value_iter->read_null_map_only());
    EXPECT_EQ(map_iterator->_key_iterator->read_requirement(),
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_EQ(value_iter->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);

    map_iterator = make_map_iterator();
    all_access_paths = {create_meta_access_path(
            {"m", ColumnIterator::ACCESS_MAP_VALUES, ColumnIterator::ACCESS_NULL})};
    predicate_access_paths = all_access_paths;
    st = map_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set predicate-only map access paths: " << st.to_string();
    value_iter = static_cast<StringFileColumnIterator*>(map_iterator->_val_iterator.get());
    EXPECT_TRUE(value_iter->read_null_map_only());
    EXPECT_EQ(map_iterator->_key_iterator->read_requirement(),
              ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, MapFullProjectionStillRoutesPredicateSubPaths) {
    auto make_value_struct = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_a->set_column_name("a");
        auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_b->set_column_name("b");
        sub_iters.emplace_back(std::move(sub_a));
        sub_iters.emplace_back(std::move(sub_b));

        auto value_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        return value_struct;
    };

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(map_null_iter), std::move(map_offsets_iter),
            std::move(map_key_iter), make_value_struct());
    map_iterator->set_column_name("m");

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0] = create_data_access_path({"m"});

    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();
    predicate_access_paths[0] = create_data_access_path({"m", "KEYS"});
    predicate_access_paths.emplace_back();
    predicate_access_paths[1] = create_data_access_path({"m", "VALUES", "a"});

    auto st = map_iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();

    EXPECT_EQ(map_iterator->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iterator->_key_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(map_iterator->_val_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto* value_struct = static_cast<StructFileColumnIterator*>(map_iterator->_val_iterator.get());
    EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
}

TEST_F(ColumnReaderTest, MetaOnlyAllPathsStillRoutePredicateSubPaths) {
    {
        auto struct_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto selected_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        selected_iter->set_column_name("selected");
        auto skipped_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        skipped_iter->set_column_name("skipped");
        sub_iters.emplace_back(std::move(selected_iter));
        sub_iters.emplace_back(std::move(skipped_iter));
        StructFileColumnIterator struct_iterator(std::make_shared<ColumnReader>(),
                                                 std::move(struct_null_iter), std::move(sub_iters));
        struct_iterator.set_column_name("s");

        TColumnAccessPaths all_access_paths {
                create_meta_access_path({"s", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {create_data_access_path({"s", "selected"})};

        auto st = struct_iterator.set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set struct access paths: " << st.to_string();
        EXPECT_FALSE(struct_iterator.read_null_map_only());
        EXPECT_EQ(struct_iterator._sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(struct_iterator._sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto array_item_iterator =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        ArrayFileColumnIterator array_iterator(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(array_item_iterator), std::move(array_null_iter));
        array_iterator.set_column_name("a");

        TColumnAccessPaths all_access_paths {
                create_meta_access_path({"a", ColumnIterator::ACCESS_OFFSET}),
                create_meta_access_path({"a", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_data_access_path({"a", ColumnIterator::ACCESS_ALL})};

        auto st = array_iterator.set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set array access paths: " << st.to_string();
        EXPECT_FALSE(array_iterator.read_offset_only());
        EXPECT_EQ(array_iterator._item_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
    }

    {
        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(),
                                           std::move(map_null_iter), std::move(map_offsets_iter),
                                           std::move(map_key_iter), std::move(map_val_iter));
        map_iterator.set_column_name("m");

        TColumnAccessPaths all_access_paths {
                create_meta_access_path({"m", ColumnIterator::ACCESS_OFFSET}),
                create_meta_access_path({"m", ColumnIterator::ACCESS_NULL})};
        TColumnAccessPaths predicate_access_paths {
                create_data_access_path({"m", ColumnIterator::ACCESS_MAP_KEYS})};

        auto st = map_iterator.set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set map access paths: " << st.to_string();
        EXPECT_FALSE(map_iterator.read_offset_only());
        EXPECT_EQ(map_iterator._key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(map_iterator._val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }
}

TEST_F(ColumnReaderTest, NestedStructArrayMapStructAccessPaths) {
    auto make_value_struct = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_a->set_column_name("a");
        auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_b->set_column_name("b");
        sub_iters.emplace_back(std::move(sub_a));
        sub_iters.emplace_back(std::move(sub_b));

        auto value_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        return value_struct;
    };

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = make_value_struct();
    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(map_null_iter), std::move(map_offsets_iter),
            std::move(map_key_iter), std::move(map_val_iter));
    map_iterator->set_column_name("item");

    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(map_iterator), std::move(array_null_iter));
    array_iterator->set_column_name("col2");

    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_col1->set_column_name("col1");
    struct_sub_iters.emplace_back(std::move(sub_col1));
    struct_sub_iters.emplace_back(std::move(array_iterator));
    auto top_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                 std::move(struct_null_iter),
                                                                 std::move(struct_sub_iters));
    top_struct->set_column_name("root");

    TColumnAccessPaths access_paths;
    access_paths.emplace_back();
    access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES", "a"});
    TColumnAccessPaths predicate_access_paths = access_paths;

    auto st = top_struct->set_access_paths(access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set nested access paths: " << st.to_string();

    EXPECT_EQ(top_struct->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(top_struct->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(top_struct->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
    auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
    EXPECT_EQ(map_iter->_key_iterator->_read_requirement, ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
    EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, NestedStructArrayMapStructAccessPathsVariants) {
    auto build_nested_iterator = []() {
        auto make_value_struct = []() {
            auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            std::vector<ColumnIteratorUPtr> sub_iters;
            auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_a->set_column_name("a");
            auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_b->set_column_name("b");
            sub_iters.emplace_back(std::move(sub_a));
            sub_iters.emplace_back(std::move(sub_b));

            auto value_struct = std::make_unique<StructFileColumnIterator>(
                    std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
            return value_struct;
        };

        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_val_iter = make_value_struct();
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(map_null_iter),
                std::move(map_offsets_iter), std::move(map_key_iter), std::move(map_val_iter));
        map_iterator->set_column_name("item");

        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(map_iterator), std::move(array_null_iter));
        array_iterator->set_column_name("col2");

        auto struct_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> struct_sub_iters;
        auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_col1->set_column_name("col1");
        struct_sub_iters.emplace_back(std::move(sub_col1));
        struct_sub_iters.emplace_back(std::move(array_iterator));
        auto top_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(struct_null_iter),
                std::move(struct_sub_iters));
        top_struct->set_column_name("root");
        return top_struct;
    };

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col1"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_EQ(top_struct->_read_requirement, ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(top_struct->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "KEYS"});
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES", "b"});

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2"});
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_EQ(top_struct->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES", "a"});

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    }
    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "KEYS"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES", "a"});
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        EXPECT_EQ(top_struct->_read_requirement, ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(top_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);

        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        EXPECT_EQ(value_struct->_sub_column_iterators[0]->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
        EXPECT_EQ(value_struct->_sub_column_iterators[1]->_read_requirement,
                  ColumnIterator::ReadRequirement::SKIP);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*"});
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES"});

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();

        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        EXPECT_EQ(map_iter->_key_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::LAZY_OUTPUT);
        EXPECT_EQ(map_iter->_val_iterator->_read_requirement,
                  ColumnIterator::ReadRequirement::PREDICATE);
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"wrong_root", "col2"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "wrong_item"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(ColumnReaderTest, DeepNestedAccessPathsFiveLevels) {
    auto make_item_struct = []() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto sub_p = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_p->set_column_name("p");
        auto sub_q = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_q->set_column_name("q");
        sub_iters.emplace_back(std::move(sub_p));
        sub_iters.emplace_back(std::move(sub_q));

        auto item_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        item_struct->set_column_name("item");
        return item_struct;
    };

    auto make_value_struct = [make_item_struct]() {
        auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> sub_iters;
        auto array_offsets = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_null = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_iter = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets), make_item_struct(),
                std::move(array_null));
        array_iter->set_column_name("arr");
        sub_iters.emplace_back(std::move(array_iter));

        auto sub_z = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_z->set_column_name("z");
        sub_iters.emplace_back(std::move(sub_z));

        auto value_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
        return value_struct;
    };

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = make_value_struct();
    auto map_iter = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(), std::move(map_null_iter), std::move(map_offsets_iter),
            std::move(map_key_iter), std::move(map_val_iter));
    map_iter->set_column_name("m");

    auto struct_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    auto sub_x = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    sub_x->set_column_name("x");
    struct_sub_iters.emplace_back(std::move(sub_x));
    struct_sub_iters.emplace_back(std::move(map_iter));
    auto top_struct = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                 std::move(struct_null_iter),
                                                                 std::move(struct_sub_iters));
    top_struct->set_column_name("root");

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0] = create_data_access_path({"root", "m", "VALUES", "arr", "*"});
    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();
    predicate_access_paths[0] = create_data_access_path({"root", "m", "VALUES", "arr", "*", "q"});

    auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set deep access paths: " << st.to_string();

    auto* map_ptr = static_cast<MapFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
    EXPECT_EQ(map_ptr->_key_iterator->_read_requirement, ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(map_ptr->_val_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);

    auto* value_struct = static_cast<StructFileColumnIterator*>(map_ptr->_val_iterator.get());
    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(value_struct->_sub_column_iterators[0].get());
    auto* item_struct = static_cast<StructFileColumnIterator*>(array_iter->_item_iterator.get());
    EXPECT_EQ(item_struct->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_EQ(item_struct->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::PREDICATE);
}

TEST_F(ColumnReaderTest, NestedLazyOutputInLazyPredicatePhase) {
    auto struct_null_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    std::vector<ColumnIteratorUPtr> struct_sub_iters;
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    struct_sub_iters.emplace_back(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    StructFileColumnIterator struct_iterator(std::make_shared<ColumnReader>(),
                                             std::move(struct_null_iterator),
                                             std::move(struct_sub_iters));
    struct_iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    struct_iterator.set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_FALSE(struct_iterator.has_lazy_read_target());
    EXPECT_FALSE(struct_iterator.need_to_read());
    struct_iterator._sub_column_iterators[0]->set_lazy_output_requirement();
    EXPECT_TRUE(struct_iterator.has_lazy_read_target());
    EXPECT_TRUE(struct_iterator.need_to_read());

    auto array_item_iterator =
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto array_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    ArrayFileColumnIterator array_iterator(
            std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
            std::move(array_item_iterator), std::move(array_null_iter));
    array_iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    array_iterator.set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_FALSE(array_iterator.has_lazy_read_target());
    EXPECT_FALSE(array_iterator.need_to_read());
    array_iterator._item_iterator->set_lazy_output_requirement();
    EXPECT_TRUE(array_iterator.has_lazy_read_target());
    EXPECT_TRUE(array_iterator.need_to_read());

    auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto map_val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    MapFileColumnIterator map_iterator(std::make_shared<ColumnReader>(), std::move(map_null_iter),
                                       std::move(map_offsets_iter), std::move(map_key_iter),
                                       std::move(map_val_iter));
    map_iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    map_iterator.set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_FALSE(map_iterator.has_lazy_read_target());
    EXPECT_FALSE(map_iterator.need_to_read());
    map_iterator._val_iterator->set_lazy_output_requirement();
    EXPECT_TRUE(map_iterator.has_lazy_read_target());
    EXPECT_TRUE(map_iterator.need_to_read());
}

TEST_F(ColumnReaderTest, NestedReadPhaseLazyOutputMatrix) {
    auto build_nested_iterator = []() {
        auto make_value_struct = []() {
            auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            std::vector<ColumnIteratorUPtr> sub_iters;
            auto sub_a = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_a->set_column_name("a");
            auto sub_b = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
            sub_b->set_column_name("b");
            sub_iters.emplace_back(std::move(sub_a));
            sub_iters.emplace_back(std::move(sub_b));

            auto value_struct = std::make_unique<StructFileColumnIterator>(
                    std::make_shared<ColumnReader>(), std::move(null_iter), std::move(sub_iters));
            return value_struct;
        };

        auto map_null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto map_key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto map_val_iter = make_value_struct();
        auto map_iterator = std::make_unique<MapFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(map_null_iter),
                std::move(map_offsets_iter), std::move(map_key_iter), std::move(map_val_iter));
        map_iterator->set_column_name("item");

        auto array_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        auto array_offsets_iter = std::make_unique<OffsetFileColumnIterator>(
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
        auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(array_offsets_iter),
                std::move(map_iterator), std::move(array_null_iter));
        array_iterator->set_column_name("col2");

        auto struct_null_iter =
                std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        std::vector<ColumnIteratorUPtr> struct_sub_iters;
        auto sub_col1 = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
        sub_col1->set_column_name("col1");
        struct_sub_iters.emplace_back(std::move(sub_col1));
        struct_sub_iters.emplace_back(std::move(array_iterator));
        auto top_struct = std::make_unique<StructFileColumnIterator>(
                std::make_shared<ColumnReader>(), std::move(struct_null_iter),
                std::move(struct_sub_iters));
        top_struct->set_column_name("root");
        return top_struct;
    };

    auto assert_need_to_read = [](StructFileColumnIterator* top_struct) {
        auto* array_iter =
                static_cast<ArrayFileColumnIterator*>(top_struct->_sub_column_iterators[1].get());
        auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
        auto* value_struct = static_cast<StructFileColumnIterator*>(map_iter->_val_iterator.get());
        auto expect_scalar = [](ColumnIterator::ReadRequirement requirement,
                                ColumnIterator::ReadPhase mode) {
            switch (mode) {
            case ColumnIterator::ReadPhase::NORMAL:
                return requirement != ColumnIterator::ReadRequirement::SKIP;
            case ColumnIterator::ReadPhase::PREDICATE:
                return requirement == ColumnIterator::ReadRequirement::PREDICATE;
            case ColumnIterator::ReadPhase::LAZY:
                return requirement == ColumnIterator::ReadRequirement::LAZY_OUTPUT;
            default:
                return false;
            }
        };
        auto expect_nested = [](ColumnIterator::ReadRequirement requirement,
                                ColumnIterator::ReadPhase mode) {
            switch (mode) {
            case ColumnIterator::ReadPhase::NORMAL:
                return requirement != ColumnIterator::ReadRequirement::SKIP;
            case ColumnIterator::ReadPhase::PREDICATE:
                return requirement == ColumnIterator::ReadRequirement::PREDICATE;
            default:
                return false;
            }
        };

        top_struct->set_read_phase(ColumnIterator::ReadPhase::NORMAL);
        EXPECT_EQ(expect_nested(top_struct->read_requirement(), ColumnIterator::ReadPhase::NORMAL),
                  top_struct->need_to_read());
        EXPECT_EQ(expect_nested(array_iter->read_requirement(), ColumnIterator::ReadPhase::NORMAL),
                  array_iter->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->read_requirement(), ColumnIterator::ReadPhase::NORMAL),
                  map_iter->need_to_read());
        EXPECT_EQ(
                expect_nested(value_struct->read_requirement(), ColumnIterator::ReadPhase::NORMAL),
                value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->read_requirement(),
                                ColumnIterator::ReadPhase::NORMAL),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->_val_iterator->read_requirement(),
                                ColumnIterator::ReadPhase::NORMAL),
                  map_iter->_val_iterator->need_to_read());

        top_struct->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
        EXPECT_EQ(
                expect_nested(top_struct->read_requirement(), ColumnIterator::ReadPhase::PREDICATE),
                top_struct->need_to_read());
        EXPECT_EQ(
                expect_nested(array_iter->read_requirement(), ColumnIterator::ReadPhase::PREDICATE),
                array_iter->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->read_requirement(), ColumnIterator::ReadPhase::PREDICATE),
                  map_iter->need_to_read());
        EXPECT_EQ(expect_nested(value_struct->read_requirement(),
                                ColumnIterator::ReadPhase::PREDICATE),
                  value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->read_requirement(),
                                ColumnIterator::ReadPhase::PREDICATE),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(expect_nested(map_iter->_val_iterator->read_requirement(),
                                ColumnIterator::ReadPhase::PREDICATE),
                  map_iter->_val_iterator->need_to_read());

        top_struct->set_read_phase(ColumnIterator::ReadPhase::LAZY);
        EXPECT_EQ(top_struct->has_lazy_read_target(), top_struct->need_to_read());
        EXPECT_EQ(array_iter->has_lazy_read_target(), array_iter->need_to_read());
        EXPECT_EQ(map_iter->has_lazy_read_target(), map_iter->need_to_read());
        EXPECT_EQ(value_struct->has_lazy_read_target(), value_struct->need_to_read());
        EXPECT_EQ(expect_scalar(map_iter->_key_iterator->read_requirement(),
                                ColumnIterator::ReadPhase::LAZY),
                  map_iter->_key_iterator->need_to_read());
        EXPECT_EQ(map_iter->_val_iterator->has_lazy_read_target(),
                  map_iter->_val_iterator->need_to_read());
    };

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES", "a"});
        TColumnAccessPaths predicate_access_paths = all_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "KEYS"});
        TColumnAccessPaths predicate_access_paths;

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*"});
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES"});

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
        assert_need_to_read(top_struct.get());
    }

    {
        auto top_struct = build_nested_iterator();
        TColumnAccessPaths all_access_paths;
        all_access_paths.emplace_back();
        all_access_paths[0] = create_data_access_path({"root", "col2", "*", "VALUES"});
        TColumnAccessPaths predicate_access_paths;
        predicate_access_paths.emplace_back();
        predicate_access_paths[0] = create_data_access_path({"root", "col2", "*", "KEYS"});

        auto st = top_struct->set_access_paths(all_access_paths, predicate_access_paths);
        EXPECT_TRUE(st.ok());
    }
}

TEST_F(ColumnReaderTest, MultiAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto create_struct_iterator2 = [](ColumnIteratorUPtr&& nested_iterator) {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(nested_iterator));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto struct_iterator = create_struct_iterator();
    struct_iterator->set_column_name("struct");

    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()), // null iterator
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()),
            std::move(struct_iterator));

    auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::move(map_iterator),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));

    // here create:
    // struct<
    //      sub_col_1,
    //      sub_col_2: array<
    //          map<
    //              key,
    //              value: struct<
    //                  sub_col_1,
    //                  sub_col_2
    //              >
    //          >
    //      >
    //  >
    array_iterator->set_column_name("sub_col_2");
    auto iterator = create_struct_iterator2(std::move(array_iterator));
    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    // all access paths:
    // self.sub_col_2.*.KEYS
    // predicates paths empty
    all_access_paths[0] = create_data_access_path({"self", "sub_col_2", "*", "KEYS"});

    TColumnAccessPaths predicate_access_paths;

    iterator->set_column_name("self");
    auto st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_read_requirement, ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_read_requirement,
              ColumnIterator::ReadRequirement::SKIP);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(iterator->_sub_column_iterators[1].get());
    ASSERT_EQ(array_iter->_item_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
    ASSERT_EQ(map_iter->_key_iterator->_read_requirement,
              ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    ASSERT_EQ(map_iter->_val_iterator->_read_requirement, ColumnIterator::ReadRequirement::SKIP);
}

TEST_F(ColumnReaderTest, StructNextBatchAndReadByRowidsUseSequentialChildReads) {
    std::vector<ColumnIteratorUPtr> sub_column_iterators;
    auto first_child = std::make_unique<TrackingColumnIterator>();
    auto* first_child_ptr = first_child.get();
    auto second_child = std::make_unique<TrackingColumnIterator>();
    auto* second_child_ptr = second_child.get();
    sub_column_iterators.emplace_back(std::move(first_child));
    sub_column_iterators.emplace_back(std::move(second_child));

    StructFileColumnIterator struct_iterator(create_test_reader(), nullptr,
                                             std::move(sub_column_iterators));

    MutableColumnPtr dst = create_int_struct_column(2);
    size_t rows = 3;
    bool has_null = false;
    auto st = struct_iterator.next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << "struct next_batch failed: " << st.to_string();
    EXPECT_EQ(3, rows);
    EXPECT_EQ(3, dst->size());
    EXPECT_THAT(first_child_ptr->next_batch_sizes, ::testing::ElementsAre(3));
    EXPECT_THAT(second_child_ptr->next_batch_sizes, ::testing::ElementsAre(3));

    first_child_ptr->clear_tracking();
    second_child_ptr->clear_tracking();

    const rowid_t rowids[] = {0, 1, 4, 5, 6};
    st = struct_iterator.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << "struct read_by_rowids failed: " << st.to_string();
    EXPECT_EQ(8, dst->size());
    EXPECT_THAT(first_child_ptr->seek_ordinals, ::testing::ElementsAre(0, 4));
    EXPECT_THAT(second_child_ptr->seek_ordinals, ::testing::ElementsAre(0, 4));
    EXPECT_THAT(first_child_ptr->next_batch_sizes, ::testing::ElementsAre(2, 3));
    EXPECT_THAT(second_child_ptr->next_batch_sizes, ::testing::ElementsAre(2, 3));
    EXPECT_TRUE(first_child_ptr->read_by_rowids_batches.empty());
    EXPECT_TRUE(second_child_ptr->read_by_rowids_batches.empty());
}

TEST_F(ColumnReaderTest, StructNullMapOnlyNextBatchSkipsSubColumns) {
    auto null_iterator = std::make_unique<TrackingColumnIterator>();
    auto* null_iterator_ptr = null_iterator.get();
    std::vector<ColumnIteratorUPtr> sub_column_iterators;
    auto child_iterator = std::make_unique<TrackingColumnIterator>();
    auto* child_iterator_ptr = child_iterator.get();
    child_iterator->set_column_name("field");
    sub_column_iterators.emplace_back(std::move(child_iterator));

    StructFileColumnIterator struct_iterator(create_test_reader(true), std::move(null_iterator),
                                             std::move(sub_column_iterators));
    struct_iterator.set_column_name("s");

    TColumnAccessPaths null_path {create_meta_access_path({"s", ColumnIterator::ACCESS_NULL})};
    auto st = struct_iterator.set_access_paths(null_path, null_path);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();
    EXPECT_TRUE(struct_iterator.read_null_map_only());
    EXPECT_EQ(child_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::SKIP);

    MutableColumnPtr dst = create_nullable_int_struct_column(1);
    size_t rows = 2;
    bool has_null = false;
    st = struct_iterator.next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << "struct null-map-only next_batch failed: " << st.to_string();
    EXPECT_TRUE(has_null);
    EXPECT_EQ(2, dst->size());
    EXPECT_THAT(null_iterator_ptr->next_batch_sizes, ::testing::ElementsAre(2));
    EXPECT_TRUE(child_iterator_ptr->next_batch_sizes.empty());

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*dst);
    EXPECT_EQ(2, nullable_column.get_null_map_column().size());
    const auto& nested_struct = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(
            nullable_column.get_nested_column());
    EXPECT_EQ(2, nested_struct.get_column(0).size());
}

TEST_F(ColumnReaderTest, ArrayNullMapOnlyNextBatchAndReadByRowidsSkipItems) {
    auto null_iterator = std::make_unique<TrackingColumnIterator>();
    auto* null_iterator_ptr = null_iterator.get();
    auto item_iterator = std::make_unique<TrackingColumnIterator>();
    auto* item_iterator_ptr = item_iterator.get();
    auto offset_iterator = create_tracking_offset_iterator();

    ArrayFileColumnIterator array_iterator(create_test_reader(true),
                                           std::move(offset_iterator.iterator),
                                           std::move(item_iterator), std::move(null_iterator));
    array_iterator.set_column_name("a");

    TColumnAccessPaths null_path {create_meta_access_path({"a", ColumnIterator::ACCESS_NULL})};
    auto st = array_iterator.set_access_paths(null_path, null_path);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();
    EXPECT_TRUE(array_iterator.read_null_map_only());
    EXPECT_EQ(item_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::SKIP);

    MutableColumnPtr dst = create_nullable_int_array_column();
    size_t rows = 3;
    bool has_null = false;
    st = array_iterator.next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << "array null-map-only next_batch failed: " << st.to_string();
    EXPECT_TRUE(has_null);
    EXPECT_EQ(3, dst->size());
    EXPECT_THAT(null_iterator_ptr->next_batch_sizes, ::testing::ElementsAre(3));
    EXPECT_TRUE(item_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(offset_iterator.tracker->next_batch_sizes.empty());

    null_iterator_ptr->clear_tracking();
    item_iterator_ptr->clear_tracking();

    const rowid_t rowids[] = {1, 3};
    st = array_iterator.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << "array null-map-only read_by_rowids failed: " << st.to_string();
    EXPECT_EQ(5, dst->size());
    EXPECT_THAT(null_iterator_ptr->seek_ordinals, ::testing::ElementsAre(1, 3));
    EXPECT_THAT(null_iterator_ptr->next_batch_sizes, ::testing::ElementsAre(1, 1));
    EXPECT_TRUE(item_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(offset_iterator.tracker->next_batch_sizes.empty());
}

TEST_F(ColumnReaderTest, MapNullMapOnlyNextBatchAndReadByRowidsSkipKeysAndValues) {
    auto null_iterator = std::make_unique<TrackingColumnIterator>();
    auto* null_iterator_ptr = null_iterator.get();
    auto key_iterator = std::make_unique<TrackingColumnIterator>();
    auto* key_iterator_ptr = key_iterator.get();
    auto value_iterator = std::make_unique<TrackingColumnIterator>();
    auto* value_iterator_ptr = value_iterator.get();
    auto offset_iterator = create_tracking_offset_iterator();

    MapFileColumnIterator map_iterator(create_test_reader(true, 4), std::move(null_iterator),
                                       std::move(offset_iterator.iterator), std::move(key_iterator),
                                       std::move(value_iterator));
    map_iterator.set_column_name("m");

    TColumnAccessPaths null_path {create_meta_access_path({"m", ColumnIterator::ACCESS_NULL})};
    auto st = map_iterator.set_access_paths(null_path, null_path);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();
    EXPECT_TRUE(map_iterator.read_null_map_only());
    EXPECT_EQ(key_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::SKIP);
    EXPECT_EQ(value_iterator_ptr->read_requirement(), ColumnIterator::ReadRequirement::SKIP);

    MutableColumnPtr dst = create_nullable_int_map_column();
    size_t rows = 3;
    bool has_null = false;
    st = map_iterator.next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << "map null-map-only next_batch failed: " << st.to_string();
    EXPECT_TRUE(has_null);
    EXPECT_EQ(3, dst->size());
    EXPECT_THAT(null_iterator_ptr->next_batch_sizes, ::testing::ElementsAre(3));
    EXPECT_TRUE(key_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(value_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(offset_iterator.tracker->next_batch_sizes.empty());

    null_iterator_ptr->clear_tracking();
    key_iterator_ptr->clear_tracking();
    value_iterator_ptr->clear_tracking();

    const rowid_t rowids[] = {1, 3};
    st = map_iterator.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << "map null-map-only read_by_rowids failed: " << st.to_string();
    EXPECT_EQ(5, dst->size());
    ASSERT_EQ(1, null_iterator_ptr->read_by_rowids_batches.size());
    EXPECT_THAT(null_iterator_ptr->read_by_rowids_batches[0], ::testing::ElementsAre(1, 3));
    EXPECT_TRUE(key_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(value_iterator_ptr->next_batch_sizes.empty());
    EXPECT_TRUE(offset_iterator.tracker->next_batch_sizes.empty());
}

TEST_F(ColumnReaderTest, CollectPrefetchersHonorsNestedReadRequirements) {
    auto null_iterator = std::make_unique<TrackingColumnIterator>();
    auto* null_iterator_ptr = null_iterator.get();
    std::vector<ColumnIteratorUPtr> sub_column_iterators;
    auto predicate_child = std::make_unique<TrackingColumnIterator>();
    auto* predicate_child_ptr = predicate_child.get();
    auto lazy_child = std::make_unique<TrackingColumnIterator>();
    auto* lazy_child_ptr = lazy_child.get();
    sub_column_iterators.emplace_back(std::move(predicate_child));
    sub_column_iterators.emplace_back(std::move(lazy_child));

    StructFileColumnIterator struct_iterator(create_test_reader(true), std::move(null_iterator),
                                             std::move(sub_column_iterators));
    struct_iterator.set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);
    predicate_child_ptr->set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
    lazy_child_ptr->set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    struct_iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>> prefetchers;
    struct_iterator.collect_prefetchers(prefetchers, PrefetcherInitMethod::FROM_ROWIDS);

    EXPECT_THAT(null_iterator_ptr->collect_methods,
                ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
    EXPECT_THAT(predicate_child_ptr->collect_methods,
                ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
    EXPECT_TRUE(lazy_child_ptr->collect_methods.empty());
    EXPECT_THAT(prefetchers[PrefetcherInitMethod::FROM_ROWIDS],
                ::testing::ElementsAre(null_iterator_ptr->prefetcher(),
                                       predicate_child_ptr->prefetcher()));

    null_iterator_ptr->clear_tracking();
    predicate_child_ptr->clear_tracking();
    lazy_child_ptr->clear_tracking();
    prefetchers.clear();

    struct_iterator.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    struct_iterator.collect_prefetchers(prefetchers, PrefetcherInitMethod::FROM_ROWIDS);

    EXPECT_THAT(null_iterator_ptr->collect_methods,
                ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
    EXPECT_TRUE(predicate_child_ptr->collect_methods.empty());
    EXPECT_THAT(lazy_child_ptr->collect_methods,
                ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
}

TEST_F(ColumnReaderTest, ArrayAndMapCollectPrefetchersUseAllDataBlocksForNestedData) {
    {
        auto null_iterator = std::make_unique<TrackingColumnIterator>();
        auto* null_iterator_ptr = null_iterator.get();
        auto item_iterator = std::make_unique<TrackingColumnIterator>();
        auto* item_iterator_ptr = item_iterator.get();
        auto offset_iterator = create_tracking_offset_iterator();
        auto* offset_iterator_ptr = offset_iterator.tracker;

        ArrayFileColumnIterator array_iterator(create_test_reader(true),
                                               std::move(offset_iterator.iterator),
                                               std::move(item_iterator), std::move(null_iterator));
        array_iterator.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
        array_iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>> prefetchers;
        array_iterator.collect_prefetchers(prefetchers, PrefetcherInitMethod::FROM_ROWIDS);

        EXPECT_THAT(offset_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
        EXPECT_THAT(null_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
        EXPECT_THAT(item_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::ALL_DATA_BLOCKS));
        EXPECT_THAT(prefetchers[PrefetcherInitMethod::ALL_DATA_BLOCKS],
                    ::testing::ElementsAre(item_iterator_ptr->prefetcher()));
    }

    {
        auto null_iterator = std::make_unique<TrackingColumnIterator>();
        auto* null_iterator_ptr = null_iterator.get();
        auto key_iterator = std::make_unique<TrackingColumnIterator>();
        auto* key_iterator_ptr = key_iterator.get();
        auto value_iterator = std::make_unique<TrackingColumnIterator>();
        auto* value_iterator_ptr = value_iterator.get();
        auto offset_iterator = create_tracking_offset_iterator();
        auto* offset_iterator_ptr = offset_iterator.tracker;

        MapFileColumnIterator map_iterator(create_test_reader(true), std::move(null_iterator),
                                           std::move(offset_iterator.iterator),
                                           std::move(key_iterator), std::move(value_iterator));
        map_iterator.set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
        map_iterator.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>> prefetchers;
        map_iterator.collect_prefetchers(prefetchers, PrefetcherInitMethod::FROM_ROWIDS);

        EXPECT_THAT(offset_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
        EXPECT_THAT(null_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::FROM_ROWIDS));
        EXPECT_THAT(key_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::ALL_DATA_BLOCKS));
        EXPECT_THAT(value_iterator_ptr->collect_methods,
                    ::testing::ElementsAre(PrefetcherInitMethod::ALL_DATA_BLOCKS));
        EXPECT_THAT(prefetchers[PrefetcherInitMethod::ALL_DATA_BLOCKS],
                    ::testing::ElementsAre(key_iterator_ptr->prefetcher(),
                                           value_iterator_ptr->prefetcher()));
    }
}

TEST_F(ColumnReaderTest, MapPredicateAccessAllWithOffsetKeepsKeysReadable) {
    auto map_reader = create_test_reader(false, 0, FieldType::OLAP_FIELD_TYPE_MAP);
    auto key_iter = std::make_unique<StringFileColumnIterator>(
            create_test_reader(false, 0, FieldType::OLAP_FIELD_TYPE_STRING));
    auto* key_ptr = key_iter.get();
    auto val_iter = std::make_unique<StringFileColumnIterator>(
            create_test_reader(false, 0, FieldType::OLAP_FIELD_TYPE_STRING));
    auto* val_ptr = val_iter.get();
    auto offset_iterator = create_tracking_offset_iterator();

    MapFileColumnIterator map_iter(map_reader, nullptr, std::move(offset_iterator.iterator),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");

    TColumnAccessPaths access_paths {create_meta_access_path(
            {"map_col", ColumnIterator::ACCESS_ALL, ColumnIterator::ACCESS_OFFSET})};
    auto st = map_iter.set_access_paths(access_paths, access_paths);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();

    EXPECT_EQ(key_ptr->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_FALSE(key_ptr->read_offset_only());
    EXPECT_EQ(val_ptr->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_TRUE(val_ptr->read_offset_only());
}

TEST_F(ColumnReaderTest, OffsetPeekUsesPageSentinelWhenNoRemaining) {
    // create a bare FileColumnIterator with a dummy ColumnReader
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate a page that has no remaining offsets in decoder but has a valid
    // next_array_item_ordinal recorded in footer
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 12345;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));
    ordinal_t offset = 0;
    auto st = offset_iter._peek_one_offset(&offset);

    ASSERT_TRUE(st.ok()) << "peek one offset failed: " << st.to_string();
    ASSERT_EQ(static_cast<ordinal_t>(12345), offset);
}

TEST_F(ColumnReaderTest, OffsetCalculateOffsetsUsesPageSentinelForLastOffset) {
    // create offset iterator with a page whose sentinel offset is set in footer
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate page with no remaining values, but a valid next_array_item_ordinal
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 15;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));

    // prepare in-memory column offsets:
    // offsets_data = [first_column_offset, first_storage_offset, next_storage_offset_placeholder]
    // first_column_offset = 100
    // first_storage_offset = 10
    // placeholder real next_storage_offset will be fetched from page sentinel (15)
    ColumnArray::ColumnOffsets column_offsets;
    auto& data = column_offsets.get_data();
    data.push_back(100); // index 0: first_column_offset
    data.push_back(10);  // index 1: first_storage_offset
    data.push_back(12);  // index 2: placeholder storage offset for middle element

    auto st = offset_iter._calculate_offsets(1, column_offsets);
    ASSERT_TRUE(st.ok()) << "calculate offsets failed: " << st.to_string();

    // after calculation:
    // data[1] = 100 + (12 - 10) = 102
    // data[2] = 100 + (15 - 10) = 105 (using page sentinel as next_storage_offset)
    ASSERT_EQ(static_cast<ordinal_t>(100), data[0]);
    ASSERT_EQ(static_cast<ordinal_t>(102), data[1]);
    ASSERT_EQ(static_cast<ordinal_t>(105), data[2]);
}

TEST_F(ColumnReaderTest, MapReadByRowidsSkipReadingResizesDestination) {
    // create a basic map iterator with dummy readers/iterators
    auto map_reader = std::make_shared<ColumnReader>();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());

    MapFileColumnIterator map_iter(map_reader, std::move(null_iter), std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");
    map_iter.set_read_requirement(ColumnIterator::ReadRequirement::SKIP);

    // prepare an empty ColumnMap as destination
    auto keys = ColumnInt32::create();
    auto values = ColumnInt32::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    auto column_map = ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    MutableColumnPtr dst = std::move(column_map);

    const rowid_t rowids[] = {1, 5, 7};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    auto st = map_iter.read_by_rowids(rowids, count, dst);

    ASSERT_TRUE(st.ok()) << "read_by_rowids failed: " << st.to_string();
    ASSERT_EQ(count, dst->size());
}

TEST_F(ColumnReaderTest, MapLazyReadByRowidsFillsSkippedKeysForValuesOnlyPath) {
    auto map_reader = create_test_reader(false, 10);
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<RowidOffsetFileColumnIterator>());
    auto key_iter = std::make_unique<TrackingColumnIterator>();
    auto* key_ptr = key_iter.get();
    auto val_iter = std::make_unique<TrackingColumnIterator>();
    auto* val_ptr = val_iter.get();

    MapFileColumnIterator map_iter(map_reader, nullptr, std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");

    TColumnAccessPaths all_access_paths {
            create_data_access_path({"map_col", ColumnIterator::ACCESS_MAP_VALUES})};
    auto st = map_iter.set_access_paths(all_access_paths, {});
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();
    ASSERT_EQ(key_ptr->read_requirement(), ColumnIterator::ReadRequirement::SKIP);
    ASSERT_EQ(val_ptr->read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    // Simulate the top-level struct reader marking this map as a lazy output branch.
    map_iter.set_read_requirement_self(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    map_iter.set_read_phase(ColumnIterator::ReadPhase::LAZY);

    MutableColumnPtr dst = ColumnMap::create(ColumnInt32::create(), ColumnInt32::create(),
                                             ColumnArray::ColumnOffsets::create());
    const rowid_t rowids[] = {1};
    st = map_iter.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << "lazy map read_by_rowids failed: " << st.to_string();

    const auto& column_map = assert_cast<const ColumnMap&, TypeCheckOnRelease::DISABLE>(*dst);
    EXPECT_EQ(1, dst->size());
    EXPECT_EQ(1, column_map.get_keys().size());
    EXPECT_EQ(1, column_map.get_values().size());
    EXPECT_TRUE(key_ptr->next_batch_sizes.empty());
    EXPECT_THAT(val_ptr->seek_ordinals, ::testing::ElementsAre(1));
    EXPECT_THAT(val_ptr->next_batch_sizes, ::testing::ElementsAre(1));
}

TEST_F(ColumnReaderTest, MapLazyReadByRowidsPreservesPredicateKeysForValuesOnlyPath) {
    auto map_reader = create_test_reader(false, 10);
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<RowidOffsetFileColumnIterator>());
    auto key_iter = std::make_unique<TrackingColumnIterator>();
    auto* key_ptr = key_iter.get();
    auto val_iter = std::make_unique<TrackingColumnIterator>();
    auto* val_ptr = val_iter.get();

    MapFileColumnIterator map_iter(map_reader, nullptr, std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");

    TColumnAccessPaths all_access_paths {
            create_data_access_path({"map_col", ColumnIterator::ACCESS_MAP_VALUES})};
    TColumnAccessPaths predicate_access_paths {
            create_data_access_path({"map_col", ColumnIterator::ACCESS_MAP_KEYS})};
    auto st = map_iter.set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();
    ASSERT_EQ(key_ptr->read_requirement(), ColumnIterator::ReadRequirement::PREDICATE);
    ASSERT_EQ(val_ptr->read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);

    map_iter.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    ASSERT_TRUE(map_iter.need_to_read());
    ASSERT_FALSE(map_iter.need_to_read_meta_columns());

    MutableColumnPtr dst = ColumnMap::create(ColumnInt32::create(), ColumnInt32::create(),
                                             ColumnArray::ColumnOffsets::create());
    auto& column_map = assert_cast<ColumnMap&, TypeCheckOnRelease::DISABLE>(*dst);
    auto keys_ptr = IColumn::mutate(std::move(column_map.get_keys_ptr()));
    keys_ptr->insert_many_defaults(1);
    column_map.get_keys_ptr() = std::move(keys_ptr);
    column_map.get_offsets().push_back(1);

    const rowid_t rowids[] = {1};
    st = map_iter.read_by_rowids(rowids, std::size(rowids), dst);
    ASSERT_TRUE(st.ok()) << "lazy map read_by_rowids failed: " << st.to_string();

    EXPECT_EQ(1, dst->size());
    EXPECT_EQ(1, column_map.get_keys().size());
    EXPECT_EQ(1, column_map.get_values().size());
    EXPECT_TRUE(key_ptr->seek_ordinals.empty());
    EXPECT_TRUE(key_ptr->next_batch_sizes.empty());
    EXPECT_THAT(val_ptr->seek_ordinals, ::testing::ElementsAre(1));
    EXPECT_THAT(val_ptr->next_batch_sizes, ::testing::ElementsAre(1));
}
TEST_F(ColumnReaderTest, MapAccessAllWithOffsetDoesNotPropagateOffsetToKey) {
    // Regression test: when the access path is [map_col, *, OFFSET]
    // (e.g. length(map_col['some_key'])), the key column must be fully read
    // so that element_at() can match the key. Only the value column should
    // enter OFFSET_ONLY mode.
    auto map_reader = std::make_shared<ColumnReader>();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto key_iter = std::make_unique<StringFileColumnIterator>(std::make_shared<ColumnReader>());
    auto val_iter = std::make_unique<StringFileColumnIterator>(std::make_shared<ColumnReader>());

    MapFileColumnIterator map_iter(map_reader, std::move(null_iter), std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");

    // path: [map_col, *, OFFSET]  — simulates length(map_col['c_phone'])
    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();
    all_access_paths[0] = create_meta_access_path({"map_col", "*", "OFFSET"});
    TColumnAccessPaths predicate_access_paths;

    auto st = map_iter.set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "set_access_paths failed: " << st.to_string();

    // Key must be fully readable (LAZY_OUTPUT), NOT in OFFSET_ONLY mode.
    auto* key_ptr = static_cast<StringFileColumnIterator*>(map_iter._key_iterator.get());
    ASSERT_EQ(key_ptr->_read_requirement, ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    ASSERT_FALSE(key_ptr->read_offset_only());

    // Value should be in OFFSET_ONLY mode since we only need string lengths.
    auto* val_ptr = static_cast<StringFileColumnIterator*>(map_iter._val_iterator.get());
    ASSERT_EQ(val_ptr->_read_requirement, ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    ASSERT_TRUE(val_ptr->read_offset_only());
}

} // namespace doris::segment_v2
