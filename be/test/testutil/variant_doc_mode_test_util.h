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

#pragma once

#include <string>
#include <vector>

#include "common/config.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exec/common/variant_util.h"
#include "gtest/gtest.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/olap_define.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"

namespace doris::variant_doc_mode_test {

// ============================================================
// JSON generation helpers
// ============================================================

// Generate JSON with num_keys flat keys + a nested object with obj_child_count children:
// {"k0":base_val, ..., "k{N-1}":base_val+N-1, "obj":{"f0":base_val, ..., "f{M-1}":base_val+M-1}}
// Total unique paths = num_keys + obj_child_count
inline std::string generate_json(int num_keys, int base_val, int obj_child_count = 3) {
    std::string json = "{";
    for (int i = 0; i < num_keys; ++i) {
        if (i > 0) json += ",";
        json += "\"k" + std::to_string(i) + "\":" + std::to_string(base_val + i);
    }
    if (obj_child_count > 0) {
        if (num_keys > 0) json += ",";
        json += "\"obj\":{";
        for (int f = 0; f < obj_child_count; ++f) {
            if (f > 0) json += ",";
            json += "\"f" + std::to_string(f) + "\":" + std::to_string(base_val + f);
        }
        json += "}";
    }
    json += "}";
    return json;
}

// ============================================================
// ColumnVariant creation helpers
// ============================================================

// Parse JSON strings into a doc_value mode ColumnVariant.
inline MutableColumnPtr create_doc_value_variant(const std::vector<std::string>& jsons,
                                                 bool enable_doc_mode = true) {
    auto col = ColumnVariant::create(0, enable_doc_mode);
    ParseConfig config;
    config.deprecated_enable_flatten_nested = false;
    config.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    for (const auto& json : jsons) {
        variant_util::parse_json_to_variant(*col, StringRef(json.data(), json.size()), nullptr,
                                            config);
    }
    col->finalize();
    return col;
}

// ============================================================
// Schema construction
// ============================================================

inline void construct_variant_column_pb(ColumnPB* column_pb, int32_t col_unique_id,
                                        int32_t max_subcolumns_count = 10,
                                        bool enable_doc_mode = true,
                                        int64_t doc_materialization_min_rows = 0,
                                        int doc_hash_shard_count = 1, bool is_nullable = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name("V1");
    column_pb->set_type("VARIANT");
    column_pb->set_is_key(false);
    column_pb->set_is_nullable(is_nullable);
    column_pb->set_variant_max_subcolumns_count(max_subcolumns_count);
    column_pb->set_variant_max_sparse_column_statistics_size(10000);
    column_pb->set_variant_sparse_hash_shard_count(0);
    column_pb->set_variant_enable_doc_mode(enable_doc_mode);
    column_pb->set_variant_doc_materialization_min_rows(doc_materialization_min_rows);
    if (doc_hash_shard_count > 0) {
        column_pb->set_variant_doc_hash_shard_count(doc_hash_shard_count);
    }
    column_pb->set_variant_enable_nested_group(false);
}

inline TabletSchemaSPtr create_doc_mode_schema(int32_t max_subcolumns_count = 10,
                                               int64_t doc_materialization_min_rows = 0,
                                               int doc_hash_shard_count = 1,
                                               bool is_nullable = false) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_variant_column_pb(schema_pb.add_column(), /*col_unique_id=*/1, max_subcolumns_count,
                                /*enable_doc_mode=*/true, doc_materialization_min_rows,
                                doc_hash_shard_count, is_nullable);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

inline TabletSchemaSPtr create_doc_mode_schema_with_index(int32_t max_subcolumns_count = 10,
                                                          int64_t doc_materialization_min_rows = 0,
                                                          int doc_hash_shard_count = 1,
                                                          int64_t index_id = 10000,
                                                          const std::string& field_pattern = "",
                                                          bool is_nullable = false) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_variant_column_pb(schema_pb.add_column(), /*col_unique_id=*/1, max_subcolumns_count,
                                /*enable_doc_mode=*/true, doc_materialization_min_rows,
                                doc_hash_shard_count, is_nullable);

    auto* idx = schema_pb.add_index();
    idx->set_index_id(index_id);
    idx->set_index_name("idx_v1");
    idx->set_index_type(IndexType::INVERTED);
    idx->add_col_unique_id(1); // variant column unique_id
    if (!field_pattern.empty()) {
        (*idx->mutable_properties())["field_pattern"] = field_pattern;
    }

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

// ============================================================
// Segment footer structure inspection
// ============================================================

struct SegmentStructure {
    int root_column_count = 0;
    int subcolumn_count = 0;
    int doc_value_bucket_count = 0;
    int sparse_column_count = 0;
    int indexed_subcolumn_count = 0; // subcolumns with indexes_size() > 0

    bool has_doc_value() const { return doc_value_bucket_count > 0; }
    bool has_subcolumns() const { return subcolumn_count > 0; }
};

inline SegmentStructure inspect_footer(const segment_v2::SegmentFooterPB& footer) {
    SegmentStructure result;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col_meta = footer.columns(i);
        if (!col_meta.has_column_path_info()) {
            result.root_column_count++;
            continue;
        }
        PathInData path;
        path.from_protobuf(col_meta.column_path_info());
        auto rel_path = path.copy_pop_front().get_path();
        if (rel_path.empty()) {
            result.root_column_count++;
        } else if (rel_path.find("__DORIS_VARIANT_DOC_VALUE__") != std::string::npos) {
            result.doc_value_bucket_count++;
        } else if (rel_path.find("__DORIS_VARIANT_SPARSE__") != std::string::npos) {
            result.sparse_column_count++;
        } else {
            result.subcolumn_count++;
            if (col_meta.indexes_size() > 0) {
                result.indexed_subcolumn_count++;
            }
        }
    }
    return result;
}

// ============================================================
// Segment type assertions
// ============================================================

// Segment A: pure doc_value, no materialized subcolumns
inline void assert_segment_type_A(const segment_v2::SegmentFooterPB& footer) {
    auto s = inspect_footer(footer);
    EXPECT_GT(s.doc_value_bucket_count, 0) << "Segment A should have doc_value buckets";
    EXPECT_EQ(s.subcolumn_count, 0) << "Segment A should have no subcolumns";
}

// Segment B: downgraded, pure subcolumns, no doc_value
inline void assert_segment_type_B(const segment_v2::SegmentFooterPB& footer) {
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.doc_value_bucket_count, 0) << "Segment B should have no doc_value buckets";
    EXPECT_GT(s.subcolumn_count, 0) << "Segment B should have subcolumns";
}

// Segment C: materialized subcolumns + doc_value
inline void assert_segment_type_C(const segment_v2::SegmentFooterPB& footer) {
    auto s = inspect_footer(footer);
    EXPECT_GT(s.doc_value_bucket_count, 0) << "Segment C should have doc_value buckets";
    EXPECT_GT(s.subcolumn_count, 0) << "Segment C should have subcolumns";
}

// ============================================================
// MockColumnReaderCache (for path-based column reading)
// ============================================================

class MockColumnReaderCache : public segment_v2::ColumnReaderCache {
public:
    MockColumnReaderCache(const segment_v2::SegmentFooterPB& footer,
                          const io::FileReaderSPtr& file_reader,
                          const TabletSchemaSPtr& tablet_schema)
            : ColumnReaderCache(nullptr, nullptr, nullptr, 0,
                                [](std::shared_ptr<segment_v2::SegmentFooterPB>&,
                                   OlapReaderStatistics*) { return Status::OK(); }),
              _footer(footer),
              _file_reader(file_reader),
              _tablet_schema(tablet_schema) {}

    Status get_path_column_reader(
            int32_t col_uid, PathInData relative_path,
            std::shared_ptr<segment_v2::ColumnReader>* column_reader, OlapReaderStatistics* stats,
            const SubcolumnColumnMetaInfo::Node* node_hint = nullptr) override {
        DCHECK(node_hint != nullptr);
        int32_t footer_ordinal = node_hint->data.footer_ordinal;
        if (footer_ordinal < 0 || footer_ordinal >= _footer.columns_size()) {
            *column_reader = nullptr;
            return Status::OK();
        }
        segment_v2::ColumnReaderOptions opts;
        opts.kept_in_memory = false;
        opts.be_exec_version = BeExecVersionManager::get_newest_version();
        opts.tablet_schema = _tablet_schema;
        return segment_v2::ColumnReader::create(opts, _footer.columns(footer_ordinal),
                                                _footer.num_rows(), _file_reader, column_reader);
    }

private:
    const segment_v2::SegmentFooterPB& _footer;
    const io::FileReaderSPtr& _file_reader;
    const TabletSchemaSPtr& _tablet_schema;
};

// ============================================================
// Segment write helper
// ============================================================

// Write a ColumnVariant into a segment file via ColumnWriter.
// Returns the footer and file_path for subsequent reads.
inline Status write_variant_segment(const TabletSchemaSPtr& schema, const ColumnPtr& variant_column,
                                    size_t num_rows, const std::string& tablet_path,
                                    const std::string& rowset_id, int segment_id,
                                    segment_v2::SegmentFooterPB* footer,
                                    std::string* out_file_path) {
    auto file_path = local_segment_path(tablet_path, rowset_id, segment_id);
    *out_file_path = file_path;

    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file_path, &file_writer));

    segment_v2::ColumnWriterOptions opts;
    opts.meta = footer->add_columns();
    opts.compression_type = segment_v2::CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = schema;

    TabletColumn parent_column = schema->column(0);
    _init_column_meta(opts.meta, 0, parent_column, segment_v2::CompressionTypePB::LZ4);

    std::unique_ptr<segment_v2::ColumnWriter> writer;
    RETURN_IF_ERROR(
            segment_v2::ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer));
    RETURN_IF_ERROR(writer->init());

    Block block;
    block.insert(
            {variant_column,
             std::make_shared<DataTypeVariant>(parent_column.variant_max_subcolumns_count(), false),
             "V1"});

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    converter->add_column_data_convertor(parent_column);
    converter->set_source_content(&block, 0, num_rows);
    auto [result, accessor] = converter->convert_column_data(0);
    RETURN_IF_ERROR(result);

    RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), num_rows));
    RETURN_IF_ERROR(writer->finish());
    RETURN_IF_ERROR(writer->write_data());
    RETURN_IF_ERROR(writer->write_ordinal_index());
    RETURN_IF_ERROR(writer->write_zone_map());
    RETURN_IF_ERROR(file_writer->close());

    footer->set_num_rows(static_cast<uint32_t>(num_rows));
    return Status::OK();
}

// ============================================================
// Segment read helpers
// ============================================================

// Create a VariantColumnReader from footer + file.
inline Status create_variant_reader(const segment_v2::SegmentFooterPB& footer,
                                    const io::FileReaderSPtr& file_reader,
                                    const TabletSchemaSPtr& schema,
                                    std::shared_ptr<segment_v2::ColumnReader>* out) {
    segment_v2::ColumnMetaAccessor accessor;
    RETURN_IF_ERROR(accessor.init(footer, file_reader));

    segment_v2::ColumnReaderOptions opts;
    opts.kept_in_memory = false;
    opts.be_exec_version = BeExecVersionManager::get_newest_version();
    opts.tablet_schema = schema;

    auto variant_reader = std::make_shared<segment_v2::VariantColumnReader>();
    int32_t root_uid = schema->column(0).unique_id();
    auto footer_sp = std::make_shared<segment_v2::SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    RETURN_IF_ERROR(variant_reader->init(opts, &accessor, footer_sp, root_uid, footer.num_rows(),
                                         file_reader));
    *out = std::move(variant_reader);
    return Status::OK();
}

// SELECT * — read all rows via RootColumnIterator, return JSON strings.
inline Status read_all_rows(const segment_v2::SegmentFooterPB& footer, const std::string& file_path,
                            const TabletSchemaSPtr& schema, std::vector<std::string>* out_rows) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    RETURN_IF_ERROR(create_variant_reader(footer, file_reader, schema, &column_reader));

    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    MockColumnReaderCache cache(footer, file_reader, schema);

    TabletColumn parent_column = schema->column(0);
    StorageReadOptions read_opts;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;

    segment_v2::ColumnIteratorUPtr iterator;
    RETURN_IF_ERROR(variant_reader->new_iterator(&iterator, &parent_column, &read_opts, &cache));

    segment_v2::ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = file_reader.get();
    RETURN_IF_ERROR(iterator->init(iter_opts));

    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = footer.num_rows();
    RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
    RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));

    out_rows->clear();
    out_rows->reserve(nrows);
    DataTypeSerDe::FormatOptions fmt_opts;
    for (size_t i = 0; i < nrows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, fmt_opts);
        out_rows->push_back(std::move(value));
    }
    return Status::OK();
}

// SELECT v['path'] — read a specific subcolumn path, return JSON strings per row.
// Returns null string for rows where the path doesn't exist.
inline Status read_subcolumn(const segment_v2::SegmentFooterPB& footer,
                             const std::string& file_path, const TabletSchemaSPtr& schema,
                             const std::string& subcolumn_path,
                             std::vector<std::string>* out_values) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    RETURN_IF_ERROR(create_variant_reader(footer, file_reader, schema, &column_reader));

    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    MockColumnReaderCache cache(footer, file_reader, schema);

    // Build a TabletColumn for the subcolumn path
    TabletColumn parent_column = schema->column(0);
    TabletColumn sub_column = parent_column;
    sub_column.set_path_info(PathInData("V1." + subcolumn_path));

    StorageReadOptions read_opts;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;

    segment_v2::ColumnIteratorUPtr iterator;
    RETURN_IF_ERROR(variant_reader->new_iterator(&iterator, &sub_column, &read_opts, &cache));

    segment_v2::ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = file_reader.get();
    RETURN_IF_ERROR(iterator->init(iter_opts));

    // For LEAF reads (scalar paths like k0), the iterator reads typed data directly.
    // For HIERARCHICAL reads (obj, root), the iterator reads into ColumnVariant.
    // Try ColumnVariant first; fall back to a typed column looked up from the footer.
    size_t nrows = footer.num_rows();
    MutableColumnPtr dst;
    DataTypePtr dst_type;
    bool used_variant = true;
    try {
        dst = ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));
    } catch (const doris::Exception&) {
        // LEAF path: find the leaf column type from footer, re-create iterator, and read.
        used_variant = false;
        nrows = footer.num_rows();

        // Look up the leaf column's type from footer metadata
        std::string full_path = "v1." + subcolumn_path;
        for (int i = 0; i < footer.columns_size(); ++i) {
            const auto& col_meta = footer.columns(i);
            if (!col_meta.has_column_path_info()) continue;
            PathInData p;
            p.from_protobuf(col_meta.column_path_info());
            if (p.get_path() == full_path) {
                segment_v2::ColumnReaderOptions cr_opts;
                cr_opts.kept_in_memory = false;
                cr_opts.be_exec_version = BeExecVersionManager::get_newest_version();
                cr_opts.tablet_schema = schema;
                std::shared_ptr<segment_v2::ColumnReader> leaf_reader;
                RETURN_IF_ERROR(segment_v2::ColumnReader::create(
                        cr_opts, col_meta, footer.num_rows(), file_reader, &leaf_reader));
                dst_type = leaf_reader->get_vec_data_type();
                break;
            }
        }
        if (!dst_type) {
            std::string all_paths;
            for (int j = 0; j < footer.columns_size(); ++j) {
                if (footer.columns(j).has_column_path_info()) {
                    PathInData pp;
                    pp.from_protobuf(footer.columns(j).column_path_info());
                    all_paths += pp.get_path() + ", ";
                }
            }
            return Status::NotFound("Leaf column not found for path: {} in [{}]", full_path,
                                    all_paths);
        }

        segment_v2::ColumnIteratorUPtr iter2;
        RETURN_IF_ERROR(variant_reader->new_iterator(&iter2, &sub_column, &read_opts, &cache));
        segment_v2::ColumnIteratorOptions iter_opts2;
        iter_opts2.stats = &stats;
        iter_opts2.file_reader = file_reader.get();
        RETURN_IF_ERROR(iter2->init(iter_opts2));

        dst = dst_type->create_column();
        RETURN_IF_ERROR(iter2->seek_to_ordinal(0));
        RETURN_IF_ERROR(iter2->next_batch(&nrows, dst));
    }

    out_values->clear();
    out_values->reserve(nrows);
    DataTypeSerDe::FormatOptions fmt_opts;
    if (used_variant) {
        for (size_t i = 0; i < nrows; ++i) {
            std::string value;
            assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value,
                                                                                fmt_opts);
            out_values->push_back(std::move(value));
        }
    } else {
        for (size_t i = 0; i < nrows; ++i) {
            DataTypeSerDe::FormatOptions fmt;
            out_values->push_back(dst_type->to_string(*dst, i, fmt));
        }
    }
    return Status::OK();
}

} // namespace doris::variant_doc_mode_test
