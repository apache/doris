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

#include "storage/index/index_disk_usage.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {

namespace {

const IndexDiskUsageRecord* find_record(const std::vector<IndexDiskUsageRecord>& records,
                                        int64_t index_id, IndexDiskUsageStructure structure) {
    for (const auto& record : records) {
        if (record.index_id == index_id && record.structure == structure) {
            return &record;
        }
    }
    return nullptr;
}

int64_t sum_total(const std::vector<IndexDiskUsageRecord>& records) {
    int64_t total = 0;
    for (const auto& record : records) {
        total += record.total_bytes;
    }
    return total;
}

} // namespace

TEST(IndexDiskUsageClassifyTest, ClassifiesCluceneFiles) {
    IndexDiskUsageRecord record;
    classify_clucene_file("_0.tis", 10, &record);
    classify_clucene_file("_0.tii", 1, &record);
    classify_clucene_file("_0.frq", 20, &record);
    classify_clucene_file("_0.prx", 30, &record);
    classify_clucene_file("_0.nrm", 4, &record);
    classify_clucene_file("_0.fnm", 5, &record);
    classify_clucene_file("segments_3", 6, &record);
    classify_clucene_file("segments.gen", 7, &record);
    classify_clucene_file("null_bitmap", 8, &record);
    EXPECT_EQ(IndexDiskUsageStructure::kTerm, record.structure);
    EXPECT_EQ(11, record.dict_bytes);
    EXPECT_EQ(20, record.posting_bytes);
    EXPECT_EQ(30, record.position_bytes);
    EXPECT_EQ(4, record.stats_bytes);
    EXPECT_EQ(26, record.other_bytes);
    EXPECT_EQ(91, record.total_bytes);
}

TEST(IndexDiskUsageClassifyTest, BkdFilesMarkBkdStructure) {
    IndexDiskUsageRecord record;
    classify_clucene_file("bkd", 100, &record);
    classify_clucene_file("bkd_meta", 10, &record);
    classify_clucene_file("bkd_index", 20, &record);
    classify_clucene_file("null_bitmap", 8, &record);
    EXPECT_EQ(IndexDiskUsageStructure::kBkd, record.structure);
    EXPECT_EQ(138, record.total_bytes);
}

class IndexDiskUsageCollectorTest : public ::testing::Test {
protected:
    struct IndexSpec {
        TabletIndex index;
        int column_index;
        std::function<void(IndexColumnWriter*)> feed;
    };

    const std::string kTestDir = "./ut_dir/index_disk_usage_collector_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    static TabletSchemaSPtr create_schema() {
        auto schema = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_compress_kind(COMPRESS_NONE);
        schema_pb.set_next_column_unique_id(2);
        schema->init_from_pb(schema_pb);

        TabletColumn key;
        key.set_name("c1");
        key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key.set_length(4);
        key.set_index_length(4);
        key.set_is_key(true);
        key.set_is_nullable(true);
        schema->append_column(key);

        TabletColumn text;
        text.set_name("c2");
        text.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        text.set_length(65535);
        text.set_is_key(false);
        text.set_is_nullable(false);
        schema->append_column(text);
        return schema;
    }

    static TabletIndex text_index(int64_t index_id, bool support_phrase) {
        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(index_id);
        index_pb.set_index_name("idx_text");
        index_pb.add_col_unique_id(1);
        (*index_pb.mutable_properties())["parser"] = "english";
        if (support_phrase) {
            (*index_pb.mutable_properties())["support_phrase"] = "true";
        }
        TabletIndex index;
        index.init_from_pb(index_pb);
        return index;
    }

    static TabletIndex numeric_index(int64_t index_id) {
        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(index_id);
        index_pb.set_index_name("idx_num");
        index_pb.add_col_unique_id(0);
        (*index_pb.mutable_properties())["type"] = "bkd";
        TabletIndex index;
        index.init_from_pb(index_pb);
        return index;
    }

    static void feed_text(IndexColumnWriter* writer) {
        std::vector<Slice> values = {Slice("hello world"), Slice("quick brown fox"),
                                     Slice("hello quick fox")};
        ASSERT_TRUE(writer->add_values("c2", values.data(), values.size()).ok());
    }

    static void feed_numbers(IndexColumnWriter* writer) {
        std::vector<int32_t> values = {42, 100, 42, 200};
        ASSERT_TRUE(writer->add_values("c1", values.data(), values.size()).ok());
    }

    // Writes the given indexes of one segment and returns the index path prefix.
    std::string write_segment(InvertedIndexStorageFormatPB format, const std::string& rowset_id,
                              const TabletSchemaSPtr& schema, std::vector<IndexSpec>* specs) {
        std::string prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                fmt::format("{}/{}_0.dat", kTestDir, rowset_id))};
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        if (format != InvertedIndexStorageFormatPB::V1) {
            io::FileWriterOptions opts;
            EXPECT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                        &file_writer, &opts)
                                .ok());
        }
        IndexFileWriter index_file_writer(fs, prefix, rowset_id, 0, format, std::move(file_writer));
        std::vector<std::unique_ptr<IndexColumnWriter>> writers;
        for (auto& spec : *specs) {
            std::unique_ptr<IndexColumnWriter> writer;
            const TabletColumn& column = schema->column(spec.column_index);
            EXPECT_TRUE(IndexColumnWriter::create(&column, &writer, &index_file_writer, &spec.index)
                                .ok());
            spec.feed(writer.get());
            EXPECT_TRUE(writer->finish().ok());
            writers.push_back(std::move(writer));
        }
        EXPECT_TRUE(index_file_writer.begin_close().ok());
        EXPECT_TRUE(index_file_writer.finish_close().ok());
        return prefix;
    }

    static int64_t container_file_size(const std::string& prefix) {
        int64_t size = 0;
        EXPECT_TRUE(
                io::global_local_filesystem()
                        ->file_size(InvertedIndexDescriptor::get_index_file_path_v2(prefix), &size)
                        .ok());
        return size;
    }

    void check_text_index_with_positions(InvertedIndexStorageFormatPB format) {
        auto schema = create_schema();
        std::vector<IndexSpec> specs {
                {.index = text_index(1, true), .column_index = 1, .feed = feed_text}};
        const std::string prefix = write_segment(format, "rs_text", schema, &specs);

        IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema, format,
                                          /*tablet_id=*/1001);
        std::vector<IndexDiskUsageRecord> records;
        const Status st = collector.collect(IndexDiskUsageOptions {}, &records);
        ASSERT_TRUE(st.ok()) << st;

        const IndexDiskUsageRecord* text = find_record(records, 1, IndexDiskUsageStructure::kTerm);
        ASSERT_NE(text, nullptr);
        EXPECT_GT(text->dict_bytes, 0);
        EXPECT_GT(text->posting_bytes, 0);
        EXPECT_GT(text->position_bytes, 0);
        EXPECT_EQ(text->total_bytes, text->dict_bytes + text->posting_bytes + text->position_bytes +
                                             text->stats_bytes + text->other_bytes);
        const IndexDiskUsageRecord* container =
                find_record(records, -1, IndexDiskUsageStructure::kContainer);
        ASSERT_NE(container, nullptr);
        EXPECT_GE(container->total_bytes, 0);
        EXPECT_EQ(container_file_size(prefix), sum_total(records));
    }

    // Enough rows that common terms get non-inline SNII postings with positions.
    static void feed_many_text(IndexColumnWriter* writer) {
        const std::vector<std::string> sentences = {"hello world", "quick brown fox",
                                                    "hello quick fox"};
        std::vector<std::string> storage;
        storage.reserve(1200);
        for (int i = 0; i < 1200; ++i) {
            storage.push_back(sentences[i % sentences.size()] + " doc" + std::to_string(i));
        }
        std::vector<Slice> values(storage.begin(), storage.end());
        ASSERT_TRUE(writer->add_values("c2", values.data(), values.size()).ok());
    }

    static std::vector<IndexDiskUsageRecord> collect_snii(const std::string& prefix,
                                                          const TabletSchemaSPtr& schema,
                                                          bool position_detail) {
        IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                          InvertedIndexStorageFormatPB::SNII, 1001);
        IndexDiskUsageOptions options;
        options.position_detail = position_detail;
        std::vector<IndexDiskUsageRecord> records;
        const Status st = collector.collect(options, &records);
        EXPECT_TRUE(st.ok()) << st;
        return records;
    }

    // Independently sums the position bytes of every pod_ref dictionary entry.
    static int64_t sum_pod_ref_prx_bytes(const std::string& prefix, const TabletIndex& index) {
        IndexFileReader reader(io::global_local_filesystem(), prefix,
                               InvertedIndexStorageFormatPB::SNII);
        const Status init_status = reader.init();
        if (!init_status.ok()) {
            ADD_FAILURE() << init_status;
            return -1;
        }
        auto logical = reader.open_snii_index(&index, nullptr,
                                              snii::reader::LogicalIndexOpenMode::kCompaction);
        if (!logical.has_value()) {
            ADD_FAILURE() << logical.error();
            return -1;
        }
        int64_t bytes = 0;
        std::vector<snii::format::DictEntry> entries;
        for (uint32_t block = 0; block < logical.value()->n_dict_blocks(); ++block) {
            uint64_t frq_base = 0;
            uint64_t prx_base = 0;
            const Status st =
                    logical.value()->decode_dict_block(block, &entries, &frq_base, &prx_base);
            if (!st.ok()) {
                ADD_FAILURE() << st;
                return -1;
            }
            for (const auto& entry : entries) {
                if (entry.kind == snii::format::DictEntryKind::kPodRef) {
                    bytes += static_cast<int64_t>(entry.prx_len);
                }
            }
        }
        return bytes;
    }
};

TEST_F(IndexDiskUsageCollectorTest, CollectV2TextIndexWithPositions) {
    check_text_index_with_positions(InvertedIndexStorageFormatPB::V2);
}

TEST_F(IndexDiskUsageCollectorTest, CollectV3TextIndexWithPositions) {
    check_text_index_with_positions(InvertedIndexStorageFormatPB::V3);
}

TEST_F(IndexDiskUsageCollectorTest, CollectV2DocsOnlyHasNoPositions) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = text_index(1, false), .column_index = 1, .feed = feed_text}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::V2, "rs_docs", schema, &specs);

    IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                      InvertedIndexStorageFormatPB::V2, 1001);
    std::vector<IndexDiskUsageRecord> records;
    const Status st = collector.collect(IndexDiskUsageOptions {}, &records);
    ASSERT_TRUE(st.ok()) << st;
    const IndexDiskUsageRecord* text = find_record(records, 1, IndexDiskUsageStructure::kTerm);
    ASSERT_NE(text, nullptr);
    EXPECT_EQ(0, text->position_bytes);
    EXPECT_GT(text->posting_bytes, 0);
}

TEST_F(IndexDiskUsageCollectorTest, CollectV2NumericIndexIsBkd) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = numeric_index(2), .column_index = 0, .feed = feed_numbers}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::V2, "rs_num", schema, &specs);

    IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                      InvertedIndexStorageFormatPB::V2, 1001);
    std::vector<IndexDiskUsageRecord> records;
    const Status st = collector.collect(IndexDiskUsageOptions {}, &records);
    ASSERT_TRUE(st.ok()) << st;
    const IndexDiskUsageRecord* bkd = find_record(records, 2, IndexDiskUsageStructure::kBkd);
    ASSERT_NE(bkd, nullptr);
    EXPECT_GT(bkd->total_bytes, 0);
    EXPECT_EQ(container_file_size(prefix), sum_total(records));
}

TEST_F(IndexDiskUsageCollectorTest, CollectFiltersIndexIds) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = text_index(1, true), .column_index = 1, .feed = feed_text},
            {.index = numeric_index(2), .column_index = 0, .feed = feed_numbers}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::V2, "rs_filter", schema, &specs);

    IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                      InvertedIndexStorageFormatPB::V2, 1001);
    IndexDiskUsageOptions options;
    options.index_ids = {2};
    std::vector<IndexDiskUsageRecord> records;
    const Status st = collector.collect(options, &records);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1U, records.size());
    EXPECT_EQ(2, records[0].index_id);
    EXPECT_EQ(IndexDiskUsageStructure::kBkd, records[0].structure);
}

TEST_F(IndexDiskUsageCollectorTest, CollectMissingFileFails) {
    auto schema = create_schema();
    const std::string prefix = kTestDir + "/missing_0";
    IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                      InvertedIndexStorageFormatPB::V2, 1001);
    std::vector<IndexDiskUsageRecord> records;
    const Status st = collector.collect(IndexDiskUsageOptions {}, &records);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("missing_0"), std::string::npos) << st;
}

TEST_F(IndexDiskUsageCollectorTest, CollectV1TextIndex) {
    auto schema = create_schema();
    schema->append_index(text_index(1, true));
    std::vector<IndexSpec> specs {
            {.index = text_index(1, true), .column_index = 1, .feed = feed_text}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::V1, "rs_v1", schema, &specs);

    IndexDiskUsageCollector collector(io::global_local_filesystem(), prefix, schema,
                                      InvertedIndexStorageFormatPB::V1, 1001);
    std::vector<IndexDiskUsageRecord> records;
    const Status st = collector.collect(IndexDiskUsageOptions {}, &records);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1U, records.size());
    EXPECT_EQ(1, records[0].index_id);
    EXPECT_EQ(IndexDiskUsageStructure::kTerm, records[0].structure);
    EXPECT_GT(records[0].dict_bytes, 0);
    EXPECT_GT(records[0].position_bytes, 0);
    int64_t file_size = 0;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->file_size(InvertedIndexDescriptor::get_index_file_path_v1(prefix, 1, ""),
                                    &file_size)
                        .ok());
    EXPECT_EQ(file_size, records[0].total_bytes);
}

TEST_F(IndexDiskUsageCollectorTest, CollectSniiTextIndex) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = text_index(1, true), .column_index = 1, .feed = feed_many_text}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::SNII, "rs_snii", schema, &specs);

    auto records = collect_snii(prefix, schema, /*position_detail=*/false);
    const IndexDiskUsageRecord* text = find_record(records, 1, IndexDiskUsageStructure::kTerm);
    ASSERT_NE(text, nullptr);
    EXPECT_GT(text->dict_bytes, 0);
    EXPECT_GT(text->posting_bytes, 0);
    EXPECT_GT(text->stats_bytes, 0);
    EXPECT_EQ(-1, text->position_bytes);
    const IndexDiskUsageRecord* container =
            find_record(records, -1, IndexDiskUsageStructure::kContainer);
    ASSERT_NE(container, nullptr);
    EXPECT_GE(container->total_bytes, 0);
    EXPECT_EQ(container_file_size(prefix), sum_total(records));
}

TEST_F(IndexDiskUsageCollectorTest, CollectSniiDocsOnlyHasNoPositions) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = text_index(1, false), .column_index = 1, .feed = feed_many_text}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::SNII, "rs_snii_docs", schema, &specs);

    auto records = collect_snii(prefix, schema, /*position_detail=*/false);
    const IndexDiskUsageRecord* text = find_record(records, 1, IndexDiskUsageStructure::kTerm);
    ASSERT_NE(text, nullptr);
    EXPECT_EQ(0, text->position_bytes);
    EXPECT_GT(text->posting_bytes, 0);
}

TEST_F(IndexDiskUsageCollectorTest, CollectSniiPositionDetail) {
    auto schema = create_schema();
    TabletIndex index = text_index(1, true);
    std::vector<IndexSpec> specs {
            {.index = text_index(1, true), .column_index = 1, .feed = feed_many_text}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::SNII, "rs_snii_detail", schema, &specs);

    auto coarse = collect_snii(prefix, schema, /*position_detail=*/false);
    auto detailed = collect_snii(prefix, schema, /*position_detail=*/true);
    const IndexDiskUsageRecord* c = find_record(coarse, 1, IndexDiskUsageStructure::kTerm);
    const IndexDiskUsageRecord* d = find_record(detailed, 1, IndexDiskUsageStructure::kTerm);
    ASSERT_NE(c, nullptr);
    ASSERT_NE(d, nullptr);
    const int64_t expected_positions = sum_pod_ref_prx_bytes(prefix, index);
    EXPECT_GT(expected_positions, 0);
    EXPECT_EQ(expected_positions, d->position_bytes);
    EXPECT_EQ(c->posting_bytes, d->posting_bytes + d->position_bytes);
    EXPECT_EQ(c->total_bytes, d->total_bytes);
}

TEST_F(IndexDiskUsageCollectorTest, CollectSniiBkdIndex) {
    auto schema = create_schema();
    std::vector<IndexSpec> specs {
            {.index = numeric_index(2), .column_index = 0, .feed = feed_numbers}};
    const std::string prefix =
            write_segment(InvertedIndexStorageFormatPB::SNII, "rs_snii_bkd", schema, &specs);

    auto records = collect_snii(prefix, schema, /*position_detail=*/false);
    const IndexDiskUsageRecord* bkd = find_record(records, 2, IndexDiskUsageStructure::kBkd);
    ASSERT_NE(bkd, nullptr);
    EXPECT_GT(bkd->total_bytes, 0);
    EXPECT_EQ(container_file_size(prefix), sum_total(records));
}

} // namespace doris::segment_v2
