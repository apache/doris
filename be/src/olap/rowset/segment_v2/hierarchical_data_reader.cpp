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

#include "olap/rowset/segment_v2/hierarchical_data_reader.h"

#include "io/io_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type.h"

namespace doris {
namespace segment_v2 {

void HierarchicalDataReader::reset() {
    for (auto& leave : _substream_cache) {
        // TODO reuse
        if (leave->data.column) {
            leave->data.column = nullptr;
        }
    }
    _is_finalized.assign(_is_finalized.size(), false);
}

void HierarchicalDataReader::clear() {
    for (auto& leave : _substream_cache) {
        if (leave->data.column) {
            leave->data.column->assume_mutable()->clear();
        }
    }
    _is_finalized.assign(_is_finalized.size(), false);
}

const SubstreamCache::Node* HierarchicalDataReader::find_leaf(const vectorized::PathInData& path) {
    return _substream_cache.find_leaf(path);
}

bool HierarchicalDataReader::add(const vectorized::PathInData& path, StreamReader&& sub_reader) {
    return _substream_cache.add(path, std::move(sub_reader));
}

Status HierarchicalDataReader::finalize(const std::vector<ColumnId>& column_ids,
                                        vectorized::MutableColumns& result) {
    if (_substream_cache.empty()) {
        return Status::OK();
    }
    for (int cid : column_ids) {
        const Field* desc = _schema.column(cid);
        if (!desc->path().empty() && !_is_finalized[cid]) {
            const auto* node = _substream_cache.find_exact(desc->path());
            // No such node and cid is an materialized column but not exist in file, read and parse from sparse column
            auto root_path = vectorized::PathInData({desc->path().get_parts()[0]});
            auto root_node = _substream_cache.find_leaf(root_path);
            HierarchicalDataReader::ReadType type = HierarchicalDataReader::ReadType::NONE;
            auto it = _column_read_type_map.find(desc->path());
            if (it != _column_read_type_map.end()) {
                type = it->second;
            }
            if (type == HierarchicalDataReader::ReadType::DIRECT_READ_MATERIALIZED ||
                type == HierarchicalDataReader::ReadType::NONE) {
                // scalar variant is default finalized
                if (result[cid]->empty() && node != nullptr) {
                    result[cid] = node->data.column->assume_mutable();
                }
                VLOG_DEBUG << fmt::format("skip read materialized column, path {}",
                                          desc->path().get_path());
            } else if (type == HierarchicalDataReader::ReadType::EXTRACT_FROM_ROOT) {
                // Root may not exist when segment does not contain this column
                CHECK(root_node->data.column->is_variant());
                vectorized::MutableColumnPtr extracted_column;
                const auto& root =
                        assert_cast<const vectorized::ColumnObject&>(*root_node->data.column);
                // extract root value with path, we can't modify the original root column
                // since some other column may depend on it.
                RETURN_IF_ERROR(
                        assert_cast<const vectorized::ColumnObject&>(*root_node->data.column)
                                .extract_root( // trim the root name, eg. v.a.b -> a.b
                                        desc->path().pop_front(), extracted_column));
                vectorized::MutableColumnPtr res = result[cid]->clone_empty();
                auto expected_type = Schema::get_data_type_ptr(*desc);
                if (res->is_variant()) {
                    assert_cast<vectorized::ColumnObject&>(*res).create_root(
                            root.get_root_type(), std::move(extracted_column));
                } else {
                    res = std::move(extracted_column);
                    if (!vectorized::WhichDataType(expected_type).is_json()) {
                        auto json_type =
                                res->is_nullable()
                                        ? make_nullable(
                                                  std::make_shared<vectorized::DataTypeJsonb>())
                                        : std::make_shared<vectorized::DataTypeJsonb>();
                        vectorized::ColumnPtr dst;
                        RETURN_IF_ERROR(vectorized::schema_util::cast_column(
                                {res->get_ptr(), json_type, ""}, expected_type, &dst));
                        res = dst->assume_mutable();
                    }
                    // res->insert_many_defaults(root_node->data.column->size());
                }
                VLOG_DEBUG << fmt::format("extract from root, path {}, num_rows {}, type {}",
                                          desc->path().get_path(), res->size(),
                                          expected_type->get_name());
                result[cid] = std::move(res);
                CHECK_EQ(root_node->data.column->size(), result[cid]->size());
            } else {
                // None scalar or has children columns, need merge subcolumns into a variant column
                auto variant_ptr =
                        vectorized::ColumnObject::create(true /*nullable*/, false /*without root*/);
                auto& variant = assert_cast<vectorized::ColumnObject&>(*variant_ptr);
                std::vector<const SubstreamCache::Node*> leaves;
                vectorized::PathsInData leaves_paths;
                SubstreamCache::get_leaves_of_node(node, leaves, leaves_paths);
                auto root_path = vectorized::PathInData({desc->path().get_parts()[0]});

                int size = 0;
                auto add_subcolum = [&](const vectorized::PathInData& path,
                                        const SubstreamCache::Node* node) {
                    if (!variant.has_subcolumn(path)) {
                        variant.add_sub_column(path, 0);
                    }
                    CHECK(node->data.column) << "path: " << node->path.get_path();
                    vectorized::MutableColumnPtr column = node->data.column->assume_mutable();
                    auto vnode = variant.get_subcolumn(path);
                    size = column->size();
                    vnode->insertRangeFrom(
                            vectorized::ColumnObject::Subcolumn {std::move(column), node->data.type,
                                                                 column->is_nullable(), false},
                            0, size);
                };
                for (size_t i = 0; i < leaves_paths.size(); ++i) {
                    auto node = leaves[i];
                    CHECK(node->data.column) << "path: " << node->path.get_path();
                    if (node->path == root_path) {
                        // Directly extract from root and add to variant
                        CHECK(node->data.column->is_variant());
                        const auto& root =
                                assert_cast<const vectorized::ColumnObject&>(*node->data.column);
                        variant.add_sub_column(desc->path().pop_front(),
                                               root.get_root()->assume_mutable(),
                                               root.get_root_type());
                        continue;
                    }
                    // trim the root name, eg. v.a.b -> a.b
                    add_subcolum(node->path.pop_front(), node);
                }
                // TODO select v:b -> v.b / v.b.c but v.d maybe in v
                // if (node->path == root_path) {
                //     // Extract from root and add to variant
                //     // extract root value with path
                //     CHECK(node->data.column->is_variant());
                //     vectorized::MutableColumnPtr extracted_column;
                //     const auto& root =
                //             assert_cast<const vectorized::ColumnObject&>(*node->data.column);
                //     RETURN_IF_ERROR(
                //             root.extract_root(desc->path().pop_front(), extracted_column));
                //     variant.add_sub_column(desc->path().pop_front(),
                //                            std::move(extracted_column), root.get_root_type());
                //     continue;
                // }
                variant.set_num_rows(size);
                DCHECK_EQ(variant.size(), size);
                result[cid] = std::move(variant_ptr);
            }
            if (result[cid]->is_variant()) {
                result[cid]->finalize();
            }
            _is_finalized[cid] = true;
        }
    }
    return Status::OK();
}

// In this scenario,  select v, v:A from tbl where v:A > 10.
// However, this specific column 'v:A' is not materialized in this segment and may reside in a sparse column.
// To ensure data integrity, we need to filter the data for later processing,
// otherwise, the row numbers will not match as expected.
// Here we just filter none predicate column, predicate column is filtered in `_output_column_by_sel_idx`
Status HierarchicalDataReader::filter(uint16_t* sel_rowid_idx, uint16_t selected_size,
                                      const std::vector<ColumnId>& filtered_cids,
                                      vectorized::Block* block) {
    if (_substream_cache.empty()) {
        return Status::OK();
    }
    std::set<ColumnId> filtered(filtered_cids.begin(), filtered_cids.end());
    for (auto& leave : _substream_cache) {
        if (leave->data.column && leave->data.column->size() > 0 &&
            // filter more columns
            leave->data.column->size() != selected_size) {
            VLOG_DEBUG << fmt::format("filter, rows: {}, path:{}, selected_size:{}",
                                      leave->data.column->size(), leave->path.get_path(),
                                      selected_size);
            vectorized::ColumnPtr res_ptr = leave->data.type->create_column();
            RETURN_IF_ERROR(leave->data.column->assume_mutable()->filter_by_selector(
                    sel_rowid_idx, selected_size, res_ptr->assume_mutable().get()));
            // leave->data.column.swap(res_ptr);
            // if (leave->data.cid >= 0) {
            //     _current_return_columns[leave->data.cid] = leave->data.column->assume_mutable();
            // }
            // TODO avoid copy
            leave->data.column->assume_mutable()->clear();
            leave->data.column->assume_mutable()->insert_range_from(*res_ptr, 0, res_ptr->size());
            VLOG_DEBUG << fmt::format("filter, rows: {}, path:{}, selected_size:{}",
                                      leave->data.column->size(), leave->path.get_path(),
                                      selected_size);
        }
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
