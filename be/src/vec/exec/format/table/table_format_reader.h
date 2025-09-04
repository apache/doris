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

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "exec/olap_common.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris {
class TFileRangeDesc;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class TableFormatReader : public GenericReader {
public:
    TableFormatReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeState* state,
                      RuntimeProfile* profile, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx, FileMetaCache* meta_cache)
            : _file_format_reader(std::move(file_format_reader)),
              _state(state),
              _profile(profile),
              _params(params),
              _range(range),
              _io_ctx(io_ctx) {
        _meta_cache = meta_cache;
        if (range.table_format_params.__isset.table_level_row_count) {
            _table_level_row_count = range.table_format_params.table_level_row_count;
        } else {
            _table_level_row_count = -1;
        }
    }
    ~TableFormatReader() override = default;
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) final {
        if (_push_down_agg_type == TPushAggOp::type::COUNT && _table_level_row_count >= 0) {
            auto rows =
                    std::min(_table_level_row_count, (int64_t)_state->query_options().batch_size);
            _table_level_row_count -= rows;
            auto mutate_columns = block->mutate_columns();
            for (auto& col : mutate_columns) {
                col->resize(rows);
            }
            block->set_columns(std::move(mutate_columns));
            *read_rows = rows;
            if (_table_level_row_count == 0) {
                *eof = true;
            }

            return Status::OK();
        }
        return get_next_block_inner(block, read_rows, eof);
    }

    virtual Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) = 0;

    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) final {
        return _file_format_reader->get_columns(name_to_type, missing_cols);
    }

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override {
        return _file_format_reader->get_parsed_schema(col_names, col_types);
    }

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) final {
        return _file_format_reader->set_fill_columns(partition_columns, missing_columns);
    }

    bool fill_all_columns() const override { return _file_format_reader->fill_all_columns(); }

    virtual Status init_row_filters() = 0;

    bool count_read_rows() override { return _file_format_reader->count_read_rows(); }

protected:
    std::string _table_format;                          // hudi, iceberg, paimon
    std::unique_ptr<GenericReader> _file_format_reader; // parquet, orc
    RuntimeState* _state = nullptr;                     // for query options
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    io::IOContext* _io_ctx = nullptr;
    int64_t _table_level_row_count = -1; // for optimization of count(*) push down
    void _collect_profile_before_close() override {
        if (_file_format_reader != nullptr) {
            _file_format_reader->collect_profile_before_close();
        }
    }
};

class TableSchemaChangeHelper {
public:
    ~TableSchemaChangeHelper() = default;

public:
    class Node {
    public:
        virtual ~Node() = default;
        virtual std::shared_ptr<Node> get_children_node(std::string table_column_name) const {
            throw std::logic_error("get_children_node should not be called on base TableInfoNode");
        };

        virtual std::string children_file_column_name(std::string table_column_name) const {
            throw std::logic_error(
                    "children_file_column_name should not be called on base TableInfoNode");
        }

        virtual bool children_column_exists(std::string table_column_name) const {
            throw std::logic_error(
                    "children_column_exists should not be called on base TableInfoNode");
        }

        virtual std::shared_ptr<Node> get_element_node() const {
            throw std::logic_error("get_element_node should not be called on base TableInfoNode");
        }

        virtual std::shared_ptr<Node> get_key_node() const {
            throw std::logic_error("get_key_node should not be called on base TableInfoNode");
        }
        virtual std::shared_ptr<Node> get_value_node() const {
            throw std::logic_error("get_value_node should not be called on base TableInfoNode");
        }

        virtual void add_not_exist_children(std::string table_column_name) {
            throw std::logic_error(
                    "add_not_exist_children should not be called on base TableInfoNode");
        };

        virtual void add_children(std::string table_column_name, std::string file_column_name,
                                  std::shared_ptr<Node> children_node) {
            throw std::logic_error("add_children should not be called on base TableInfoNode");
        }
    };

    class ScalarNode : public Node {};

    class StructNode : public Node {
        using ChildrenType = std::tuple<std::shared_ptr<Node>, std::string, bool>;

        // table column name -> { node, file_column_name, exists_in_file}
        std::map<std::string, ChildrenType> children;

    public:
        std::shared_ptr<Node> get_children_node(std::string table_column_name) const override {
            DCHECK(children.contains(table_column_name));
            DCHECK(children_column_exists(table_column_name));
            return std::get<0>(children.at(table_column_name));
        }

        std::string children_file_column_name(std::string table_column_name) const override {
            DCHECK(children.contains(table_column_name));
            DCHECK(children_column_exists(table_column_name));
            return std::get<1>(children.at(table_column_name));
        }

        bool children_column_exists(std::string table_column_name) const override {
            DCHECK(children.contains(table_column_name));
            return std::get<2>(children.at(table_column_name));
        }

        void add_not_exist_children(std::string table_column_name) override {
            children.emplace(table_column_name, std::make_tuple(nullptr, "", false));
        }

        void add_children(std::string table_column_name, std::string file_column_name,
                          std::shared_ptr<Node> children_node) override {
            children.emplace(table_column_name,
                             std::make_tuple(children_node, file_column_name, true));
        }

        const std::map<std::string, ChildrenType>& get_childrens() const { return children; }
    };

    class ArrayNode : public Node {
        std::shared_ptr<Node> _element_node;

    public:
        ArrayNode(const std::shared_ptr<Node>& element_node) : _element_node(element_node) {}

        std::shared_ptr<Node> get_element_node() const override { return _element_node; }
    };

    class MapNode : public Node {
        std::shared_ptr<Node> _key_node;
        std::shared_ptr<Node> _value_node;

    public:
        MapNode(const std::shared_ptr<Node>& key_node, const std::shared_ptr<Node>& value_node)
                : _key_node(key_node), _value_node(value_node) {}

        std::shared_ptr<Node> get_key_node() const override { return _key_node; }

        std::shared_ptr<Node> get_value_node() const override { return _value_node; }
    };

    class ConstNode : public Node {
        // If you can be sure that there has been no schema change between the table and the file,
        // you can use constNode (of course, you need to pay attention to case sensitivity).
    public:
        std::shared_ptr<Node> get_children_node(std::string table_column_name) const override {
            return get_instance();
        };

        std::string children_file_column_name(std::string table_column_name) const override {
            return table_column_name;
        }

        bool children_column_exists(std::string table_column_name) const override { return true; }

        std::shared_ptr<Node> get_element_node() const override { return get_instance(); }

        std::shared_ptr<Node> get_key_node() const override { return get_instance(); }

        std::shared_ptr<Node> get_value_node() const override { return get_instance(); }

        static const std::shared_ptr<ConstNode>& get_instance() {
            static const std::shared_ptr<ConstNode> instance = std::make_shared<ConstNode>();
            return instance;
        }
    };

    static std::string debug(const std::shared_ptr<Node>& root, size_t level = 0);

protected:
    // Whenever external components invoke the Parquet/ORC reader (e.g., init_reader, get_next_block, set_fill_columns),
    // the parameters passed in are based on `table column names`.
    // The table_info_node_ptr assists the Parquet/ORC reader in mapping these to the actual
    // `file columns name` to be read and enables min/max filtering.
    std::shared_ptr<Node> table_info_node_ptr = std::make_shared<StructNode>();

protected:
    Status gen_table_info_node_by_field_id(const TFileScanRangeParams& params,
                                           int64_t split_schema_id,
                                           const TupleDescriptor* tuple_descriptor,
                                           const FieldDescriptor& parquet_field_desc) {
        if (!params.__isset.history_schema_info) [[unlikely]] {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                    tuple_descriptor, parquet_field_desc, table_info_node_ptr));
            return Status::OK();
        }
        return gen_table_info_node_by_field_id(params, split_schema_id);
    }

    Status gen_table_info_node_by_field_id(const TFileScanRangeParams& params,
                                           int64_t split_schema_id,
                                           const TupleDescriptor* tuple_descriptor,
                                           const orc::Type* orc_type_ptr) {
        if (!params.__isset.history_schema_info) [[unlikely]] {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(tuple_descriptor, orc_type_ptr,
                                                            table_info_node_ptr));
            return Status::OK();
        }
        return gen_table_info_node_by_field_id(params, split_schema_id);
    }

private:
    // The filed id of both the table and the file come from the pass from fe. (params.history_schema_info)
    Status gen_table_info_node_by_field_id(const TFileScanRangeParams& params,
                                           int64_t split_schema_id) {
        if (params.current_schema_id == split_schema_id) {
            table_info_node_ptr = ConstNode::get_instance();
            return Status::OK();
        }

        int32_t table_schema_idx = -1;
        int32_t file_schema_idx = -1;
        //todo : Perhaps this process can be optimized by pre-generating a map
        for (int32_t idx = 0; idx < params.history_schema_info.size(); idx++) {
            if (params.history_schema_info[idx].schema_id == params.current_schema_id) {
                table_schema_idx = idx;
            } else if (params.history_schema_info[idx].schema_id == split_schema_id) {
                file_schema_idx = idx;
            }
        }

        if (table_schema_idx == -1 || file_schema_idx == -1) [[unlikely]] {
            return Status::InternalError(
                    "miss table/file schema info, table_schema_idx:{}  file_schema_idx:{}",
                    table_schema_idx, file_schema_idx);
        }
        RETURN_IF_ERROR(BuildTableInfoUtil::by_table_field_id(
                params.history_schema_info.at(table_schema_idx).root_field,
                params.history_schema_info.at(file_schema_idx).root_field, table_info_node_ptr));
        return Status::OK();
    }

public:
    /* Schema change Util. Used to generate `std::shared_ptr<TableSchemaChangeHelper::Node> node`.
        Passed node to parquet/orc reader to find file columns based on table columns,
    */
    struct BuildTableInfoUtil {
        static const Status SCHEMA_ERROR;

        // todo : Maybe I can use templates to implement this functionality.

        // for hive parquet : The table column names passed from fe are lowercase, so use lowercase file column names to match table column names.
        static Status by_parquet_name(const TupleDescriptor* table_tuple_descriptor,
                                      const FieldDescriptor& parquet_field_desc,
                                      std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                      const std::set<TSlotId>* is_file_slot = nullptr);

        // for hive parquet
        static Status by_parquet_name(const DataTypePtr& table_data_type,
                                      const FieldSchema& file_field,
                                      std::shared_ptr<TableSchemaChangeHelper::Node>& node);

        // for hive orc: The table column names passed from fe are lowercase, so use lowercase file column names to match table column names.
        static Status by_orc_name(const TupleDescriptor* table_tuple_descriptor,
                                  const orc::Type* orc_type_ptr,
                                  std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                  const std::set<TSlotId>* is_file_slot = nullptr);
        // for hive orc
        static Status by_orc_name(const DataTypePtr& table_data_type, const orc::Type* orc_root,
                                  std::shared_ptr<TableSchemaChangeHelper::Node>& node);

        // for paimon hudi: Use the field id in the `table schema` and `history table schema` to match columns.
        static Status by_table_field_id(const schema::external::TField table_schema,
                                        const schema::external::TField file_schema,
                                        std::shared_ptr<TableSchemaChangeHelper::Node>& node);

        // for paimon hudi
        static Status by_table_field_id(const schema::external::TStructField& table_schema,
                                        const schema::external::TStructField& file_schema,
                                        std::shared_ptr<TableSchemaChangeHelper::Node>& node);

        //for iceberg parquet: Use the field id in the `table schema` and the parquet file to match columns.
        static Status by_parquet_field_id(const schema::external::TStructField& table_schema,
                                          const FieldDescriptor& parquet_field_desc,
                                          std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                          bool& exist_field_id);

        // for iceberg parquet
        static Status by_parquet_field_id(const schema::external::TField& table_schema,
                                          const FieldSchema& parquet_field,
                                          std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                          bool& exist_field_id);

        // for iceberg orc : Use the field id in the `table schema` and the orc file to match columns.
        static Status by_orc_field_id(const schema::external::TStructField& table_schema,
                                      const orc::Type* orc_root,
                                      const std::string& field_id_attribute_key,
                                      std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                      bool& exist_field_id);

        // for iceberg orc
        static Status by_orc_field_id(const schema::external::TField& table_schema,
                                      const orc::Type* orc_root,
                                      const std::string& field_id_attribute_key,
                                      std::shared_ptr<TableSchemaChangeHelper::Node>& node,
                                      bool& exist_field_id);
    };
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
