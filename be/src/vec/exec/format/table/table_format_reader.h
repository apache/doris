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
#include "vec/core/block.h"
#include "vec/exec/format/generic_reader.h"

#include "vec/exec/format/parquet/schema_desc.h"
#include "util/string_util.h"

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
                      const TFileRangeDesc& range, io::IOContext* io_ctx)
            : _file_format_reader(std::move(file_format_reader)),
              _state(state),
              _profile(profile),
              _params(params),
              _range(range),
              _io_ctx(io_ctx) {
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
namespace TableSchemaChange {

    struct node {
        virtual ~node() = default;
        virtual bool children_exists_in_file(std::string table_column_name) const = 0 ;
        virtual std::string children_name_in_file(std::string table_column_name) const= 0;
        virtual const std::shared_ptr<node> get_children_node(std::string table_column_name) const= 0;

        const std::shared_ptr<node> get_key_node() const {
            return get_children_node("key");
        };
        const std::shared_ptr<node> get_value_node() {
            return get_children_node("value");
        };
        const std::shared_ptr<node> get_element_node() {
            return get_children_node("element");
        };
    };


    struct tableNode : public node {
//    private:
        std::map<std::string, std::shared_ptr<tableNode>> children;// table name => node
        std::string file_name;
        bool exists_in_file;
    public:
        bool children_exists_in_file(std::string table_column_name) const override{
            DCHECK(children.contains(table_column_name));
            return children.at(table_column_name)->exists_in_file;
        }

        std::string children_name_in_file(std::string table_column_name) const override {
            DCHECK(children.contains(table_column_name));
            return children.at(table_column_name)->file_name;
        }

        const std::shared_ptr<node> get_children_node(std::string table_column_name) const override {
            DCHECK(children.contains(table_column_name));
            return children.at(table_column_name);
        }



    };

    struct constNode;
    static std::shared_ptr<constNode> const_node = std::make_shared<constNode>();

    struct constNode : public node {
        bool children_exists_in_file(std::string table_column_name) const override{
            return true;
        }

        std::string children_name_in_file(std::string table_column_name) const override {
            return table_column_name;
        }

        const std::shared_ptr<node> get_children_node(std::string table_column_name) const override {
            return const_node;
        }
    };



}


class TableSchemaChangeHelper {
public:
    /** Get the mapping from the unique ID of the column in the current file to the file column name.
     * Iceberg/Hudi/Paimon usually maintains field IDs to support schema changes. If you cannot obtain this
     * information (maybe the old version does not have this information), you need to set `exist_schema` = `false`.
     */
    virtual Status get_file_col_id_to_name(bool& exist_schema,
                                           TSchemaInfoNode &file_col_id_to_name) = 0;

    virtual ~TableSchemaChangeHelper() = default;

protected:
    /** table_id_to_name  : table column unique id to table name map */
    Status init_schema_info(const TSchemaInfoNode& table_id_to_name);
    std::shared_ptr<TableSchemaChange::tableNode> table_info_node_ptr;

public:
    static Status parquet_use_name( const TupleDescriptor* table_tuple_descriptor, const FieldDescriptor& parquet_field_desc,
              std::shared_ptr<TableSchemaChange::tableNode>& root) {

        auto parquet_fields_schema  = parquet_field_desc.get_fields_schema();
        std::map<std::string, size_t>  file_column_name_idx_map;
        for (size_t idx =0 ; idx < parquet_fields_schema.size(); idx++) {
            file_column_name_idx_map.emplace(parquet_fields_schema[idx].name,  idx);   // todo :   name to lower ???
        }

        for (const auto& slot : table_tuple_descriptor->slots()) {
            const auto &table_column_name = slot->col_name();
            auto field_node = std::make_shared<TableSchemaChange::tableNode>();
            if (file_column_name_idx_map.contains(table_column_name)) {
                field_node->exists_in_file = true;
                field_node->file_name = table_column_name;
                auto file_column_idx =  file_column_name_idx_map[table_column_name];
                RETURN_IF_ERROR(parquet_subcolumn_use_name(slot->type(),field_node, parquet_fields_schema[file_column_idx]));
                root->children.emplace(table_column_name, field_node);
            } else {
                field_node->exists_in_file = false;

            }
            root->children.emplace(table_column_name, field_node);
        }

        return Status::OK();
    }

    static Status parquet_subcolumn_use_name(const TypeDescriptor table_type_desc ,std::shared_ptr<TableSchemaChange::tableNode>& node,
                       const  FieldSchema& field)  {
        switch(table_type_desc.type) {
            case TYPE_MAP:{
                if (field.type.type != TYPE_MAP) {
//                    return Status::
                }
                {
                    auto key_node = std::make_shared<TableSchemaChange::tableNode>();
                    key_node->exists_in_file = true;
                    key_node->file_name = field.children[0].children[0].name;
                    RETURN_IF_ERROR(parquet_subcolumn_use_name(table_type_desc.children[0], key_node, field.children[0].children[0]));
                    node->children.emplace("key",key_node);
                }
                {
                    auto value_node = std::make_shared<TableSchemaChange::tableNode>();
                    value_node->exists_in_file = true;
                    value_node->file_name = field.children[0].children[1].name;
                    RETURN_IF_ERROR(parquet_subcolumn_use_name(table_type_desc.children[1], value_node, field.children[0].children[1]));
                    node->children.emplace("value",value_node);
                }
                break;
            }
            case TYPE_ARRAY:{
                if (field.type.type != TYPE_ARRAY) {

                }
                auto element_node = std::make_shared<TableSchemaChange::tableNode>();
                element_node->exists_in_file = true;
                element_node->file_name = field.children[0].name;
                RETURN_IF_ERROR(parquet_subcolumn_use_name(table_type_desc.children[0], element_node, field.children[0]));
                node->children.emplace("element",element_node);
                break;
            }
            case TYPE_STRUCT:{
                if (field.type.type != TYPE_STRUCT) {

                }

                std::map<std::string,size_t> parquet_field_names;
                for (size_t idx =0 ; idx < field.children.size(); idx++) {
                    parquet_field_names.emplace(field.children[idx].name, idx);
                }
                for (size_t idx = 0; idx < table_type_desc.field_names.size();idx++)  {
                    const auto& doris_field_name = table_type_desc.field_names[idx];
                    auto field_node = std::make_shared<TableSchemaChange::tableNode>();
                    if (parquet_field_names.contains(doris_field_name)){
                        field_node->exists_in_file = true;
                        field_node->file_name = doris_field_name;
                        auto parquet_field_idx = parquet_field_names[doris_field_name];
                        RETURN_IF_ERROR(parquet_subcolumn_use_name(table_type_desc.children[idx],field_node, field.children[parquet_field_idx]));
                    } else {
                        field_node->exists_in_file = false;
                    }
                    node->children.emplace(doris_field_name,  field_node);
                }
                break;
            }
            default:{
                break;
            }
        }
        return Status::OK();
    };



    static Status orc_use_name( const TupleDescriptor* table_tuple_descriptor, const orc::Type* orc_type_ptr,
                                std::shared_ptr<TableSchemaChange::tableNode>& root) {

        std::map<std::string, uint64_t>  file_column_name_idx_map;
        for (uint64_t idx =0 ; idx < orc_type_ptr->getSubtypeCount();idx++) {
            // to_lower for match table column name.
            file_column_name_idx_map.emplace(to_lower(orc_type_ptr->getFieldName(idx)) ,idx);
        }

        for (const auto& slot : table_tuple_descriptor->slots()) {
            const auto &table_column_name = slot->col_name();
            auto field_node = std::make_shared<TableSchemaChange::tableNode>();
            if (file_column_name_idx_map.contains(table_column_name)) {
                field_node->exists_in_file = true;
                field_node->file_name = table_column_name;
                auto file_column_idx =  file_column_name_idx_map[table_column_name];
                RETURN_IF_ERROR(orc_subcolumn_use_name(slot->type(), orc_type_ptr->getSubtype(file_column_idx),field_node));
                root->children.emplace(table_column_name, field_node);
            } else {
                field_node->exists_in_file = false;

            }
            root->children.emplace(table_column_name, field_node);
        }
        return Status::OK();
    }
    static Status orc_subcolumn_use_name(const TypeDescriptor type_desc,
                            const orc::Type* orc_root , std::shared_ptr<TableSchemaChange::tableNode>& node) {
        switch(type_desc.type) {
            case TYPE_MAP:{
                if (orc::TypeKind::MAP != orc_root->getKind()) {
//                    return Status::
                }
                {
                    auto key_node = std::make_shared<TableSchemaChange::tableNode>();
                    key_node->exists_in_file = true;
                    key_node->file_name = "key";
                    RETURN_IF_ERROR(orc_subcolumn_use_name(type_desc.children[0], orc_root->getSubtype(0), key_node));
                    node->children.emplace("key",key_node);
                }
                {
                    auto value_node = std::make_shared<TableSchemaChange::tableNode>();
                    value_node->exists_in_file = true;
                    value_node->file_name = "value";
                    RETURN_IF_ERROR(orc_subcolumn_use_name(type_desc.children[1], orc_root->getSubtype(1), value_node));
                    node->children.emplace("value",value_node);
                }
                break;
            }
            case TYPE_ARRAY:{
                if (orc::TypeKind::LIST != orc_root->getKind()) {

                }
                auto element_node = std::make_shared<TableSchemaChange::tableNode>();
                element_node->exists_in_file = true;
                element_node->file_name = "element";
                RETURN_IF_ERROR(orc_subcolumn_use_name(type_desc.children[0], orc_root->getSubtype(0), element_node));
                node->children.emplace("element",element_node);
                break;
            }
            case TYPE_STRUCT:{

                if (orc::TypeKind::STRUCT != orc_root->getKind()) {

                }

                std::map<std::string,uint64_t> orc_field_names;
                for (uint64_t idx =0; idx  < orc_root->getSubtypeCount(); idx++) {
                    orc_field_names.emplace(to_lower(orc_root->getFieldName(idx)), idx);
                }

                for (size_t idx = 0; idx <type_desc.field_names.size();idx++)  {
                    const auto& doris_field_name = type_desc.field_names[idx];
                    auto field_node = std::make_shared<TableSchemaChange::tableNode>();
                    if (orc_field_names.contains(doris_field_name)){
                        field_node->exists_in_file = true;
                        field_node->file_name = doris_field_name;
                        auto orc_field_idx = orc_field_names[doris_field_name];
                        RETURN_IF_ERROR(orc_subcolumn_use_name(type_desc.children[idx], orc_root->getSubtype(orc_field_idx),field_node));
                    } else {
                        field_node->exists_in_file = false;
                    }
                    node->children.emplace(doris_field_name,  field_node);
                }
                break;
            }
            default:{
            }
        }
        return Status::OK();
    };
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
