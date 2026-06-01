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

#include "format/table/transactional_hive_reader.h"

#include <re2/re2.h>

#include "core/data_type/data_type_factory.hpp"
#include "format/orc/vorc_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "transactional_hive_common.h"

namespace doris {

namespace io {
struct IOContext;
} // namespace io
class VExprContext;
} // namespace doris

namespace doris {

TransactionalHiveReader::TransactionalHiveReader(RuntimeProfile* profile, RuntimeState* state,
                                                 const TFileScanRangeParams& params,
                                                 const TFileRangeDesc& range, size_t batch_size,
                                                 const std::string& ctz, io::IOContext* io_ctx,
                                                 FileMetaCache* meta_cache)
        : OrcReader(profile, state, params, range, batch_size, ctz, io_ctx, meta_cache, false) {
    static const char* transactional_hive_profile = "TransactionalHiveProfile";
    ADD_TIMER(get_profile(), transactional_hive_profile);
    _transactional_orc_profile.num_delete_files = ADD_CHILD_COUNTER(
            get_profile(), "NumDeleteFiles", TUnit::UNIT, transactional_hive_profile);
    _transactional_orc_profile.num_delete_rows = ADD_CHILD_COUNTER(
            get_profile(), "NumDeleteRows", TUnit::UNIT, transactional_hive_profile);
    _transactional_orc_profile.delete_files_read_time =
            ADD_CHILD_TIMER(get_profile(), "DeleteFileReadTime", transactional_hive_profile);
}

// ============================================================================
// on_before_init_reader: ACID schema mapping
// ============================================================================
Status TransactionalHiveReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(
            _extract_partition_values(*ctx->range, ctx->tuple_descriptor, _fill_partition_values));
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            _col_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::SYNTHESIZED &&
                   desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
            this->register_synthesized_column_handler(
                    desc.name,
                    [iter = std::move(topn_row_id_column_iter), this, &desc](
                            Block* block, size_t rows) -> Status {
                        return fill_topn_row_id(iter, desc.name, block, rows);
                    });
            continue;
        }
    }

    _is_acid = true;
    // Add ACID column names (originalTransaction, bucket, rowId, etc.)
    _col_names.insert(_col_names.end(), TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                      TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end());
    ctx->column_names = _col_names;

    // Get ORC file type
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));
    const auto& orc_type = *orc_type_ptr;

    // Add ACID metadata columns to table_info_node
    for (auto idx = 0; idx < TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.size(); idx++) {
        table_info_node_ptr->add_children(TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE[idx],
                                          TransactionalHive::READ_ROW_COLUMN_NAMES[idx],
                                          std::make_shared<ScalarNode>());
    }

    // https://issues.apache.org/jira/browse/HIVE-15190
    auto row_orc_type = orc_type.getSubtype(TransactionalHive::ROW_OFFSET);
    std::vector<std::string> row_names;
    std::map<std::string, uint64_t> row_names_map;
    for (uint64_t idx = 0; idx < row_orc_type->getSubtypeCount(); idx++) {
        const auto& file_column_name = row_orc_type->getFieldName(idx);
        row_names.emplace_back(file_column_name);
        row_names_map.emplace(file_column_name, idx);
    }

    // Match table columns to file columns by name
    for (const auto& slot : ctx->tuple_descriptor->slots()) {
        const auto& slot_name = slot->col_name();

        if (std::count(TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                       TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end(), slot_name) > 0) {
            return Status::InternalError("Column {} conflicts with ACID metadata column",
                                         slot_name);
        }

        if (row_names_map.contains(slot_name)) {
            std::shared_ptr<Node> child_node = nullptr;
            RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(
                    slot->type(), row_orc_type->getSubtype(row_names_map[slot_name]), child_node));
            auto file_column_name = fmt::format(
                    "{}.{}", TransactionalHive::ACID_COLUMN_NAMES[TransactionalHive::ROW_OFFSET],
                    slot_name);
            table_info_node_ptr->add_children(slot_name, file_column_name, child_node);
        } else {
            table_info_node_ptr->add_not_exist_children(slot_name);
        }
    }
    ctx->table_info_node = table_info_node_ptr;
    return Status::OK();
}

// ============================================================================
// on_after_init_reader: read delete delta files
// ============================================================================
Status TransactionalHiveReader::on_after_init_reader(ReaderInitContext* /*ctx*/) {
    std::string data_file_path = get_scan_range().path;
    // the path in _range has the namenode prefix removed,
    // and the file_path in delete file is full path, so we should add it back.
    if (get_scan_params().__isset.hdfs_params && get_scan_params().hdfs_params.__isset.fs_name) {
        std::string fs_name = get_scan_params().hdfs_params.fs_name;
        if (!starts_with(data_file_path, fs_name)) {
            data_file_path = fs_name + data_file_path;
        }
    }

    std::vector<std::string> delete_file_col_names;
    int64_t num_delete_rows = 0;
    int64_t num_delete_files = 0;
    std::filesystem::path file_path(data_file_path);

    // bucket_xxx_attemptId => bucket_xxx
    auto remove_bucket_attemptId = [](const std::string& str) {
        re2::RE2 pattern("^bucket_\\d+_\\d+$");
        if (re2::RE2::FullMatch(str, pattern)) {
            size_t pos = str.rfind('_');
            if (pos != std::string::npos) {
                return str.substr(0, pos);
            }
        }
        return str;
    };

    SCOPED_TIMER(_transactional_orc_profile.delete_files_read_time);
    for (const auto& delete_delta :
         get_scan_range().table_format_params.transactional_hive_params.delete_deltas) {
        const std::string file_name = file_path.filename().string();

        std::vector<std::string> delete_delta_file_names;
        for (const auto& x : delete_delta.file_names) {
            delete_delta_file_names.emplace_back(remove_bucket_attemptId(x));
        }
        auto iter = std::find(delete_delta_file_names.begin(), delete_delta_file_names.end(),
                              remove_bucket_attemptId(file_name));
        if (iter == delete_delta_file_names.end()) {
            continue;
        }
        auto delete_file =
                fmt::format("{}/{}", delete_delta.directory_location,
                            delete_delta.file_names[iter - delete_delta_file_names.begin()]);

        TFileRangeDesc delete_range;
        delete_range.__set_fs_name(get_scan_range().fs_name);
        delete_range.path = delete_file;
        delete_range.start_offset = 0;
        delete_range.size = -1;
        delete_range.file_size = -1;

        OrcReader delete_reader(get_profile(), get_state(), get_scan_params(), delete_range,
                                256 /*batch_size*/, get_state()->timezone(), get_io_ctx(),
                                _meta_cache, false);

        auto acid_info_node = std::make_shared<StructNode>();
        for (auto idx = 0; idx < TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE.size();
             idx++) {
            auto const& table_column_name =
                    TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE[idx];
            auto const& file_column_name = TransactionalHive::DELETE_ROW_COLUMN_NAMES[idx];
            acid_info_node->add_children(table_column_name, file_column_name,
                                         std::make_shared<ScalarNode>());
        }

        OrcInitContext delete_ctx;
        delete_ctx.column_names.assign(
                TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE.end());
        delete_ctx.col_name_to_block_idx = const_cast<std::unordered_map<std::string, uint32_t>*>(
                &TransactionalHive::DELETE_COL_NAME_TO_BLOCK_IDX);
        delete_ctx.table_info_node = acid_info_node;
        RETURN_IF_ERROR(delete_reader.init_reader(&delete_ctx));

        bool eof = false;
        while (!eof) {
            Block block;
            for (const auto& i : TransactionalHive::DELETE_ROW_PARAMS) {
                DataTypePtr data_type = DataTypeFactory::instance().create_data_type(i.type, false);
                MutableColumnPtr data_column = data_type->create_column();
                block.insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                   i.column_lower_case));
            }
            eof = false;
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader.get_next_block(&block, &read_rows, &eof));
            if (read_rows > 0) {
                static int ORIGINAL_TRANSACTION_INDEX = 0;
                static int BUCKET_ID_INDEX = 1;
                static int ROW_ID_INDEX = 2;
                const auto& original_transaction_column = assert_cast<const ColumnInt64&>(
                        *block.get_by_position(ORIGINAL_TRANSACTION_INDEX).column);
                const auto& bucket_id_column = assert_cast<const ColumnInt32&>(
                        *block.get_by_position(BUCKET_ID_INDEX).column);
                const auto& row_id_column = assert_cast<const ColumnInt64&>(
                        *block.get_by_position(ROW_ID_INDEX).column);

                DCHECK_EQ(original_transaction_column.size(), read_rows);
                DCHECK_EQ(bucket_id_column.size(), read_rows);
                DCHECK_EQ(row_id_column.size(), read_rows);

                for (int i = 0; i < read_rows; ++i) {
                    Int64 original_transaction = original_transaction_column.get_int(i);
                    Int64 bucket_id = bucket_id_column.get_int(i);
                    Int64 row_id = row_id_column.get_int(i);
                    AcidRowID delete_row_id = {original_transaction, bucket_id, row_id};
                    _acid_delete_rows.insert(delete_row_id);
                    ++num_delete_rows;
                }
            }
        }
        ++num_delete_files;
    }
    if (num_delete_rows > 0) {
        set_push_down_agg_type(TPushAggOp::NONE);
        set_delete_rows(&_acid_delete_rows);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_files, num_delete_files);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}

// ============================================================================
// on_before_read_block: expand ACID columns into block
// TODO: Consider caching ACID column templates at init time to avoid repeated
// create_column + map update on every batch. Requires a block template mechanism.
// ============================================================================
Status TransactionalHiveReader::on_before_read_block(Block* block) {
    for (const auto& i : TransactionalHive::READ_PARAMS) {
        DataTypePtr data_type = get_data_type_with_default_argument(
                DataTypeFactory::instance().create_data_type(i.type, false));
        MutableColumnPtr data_column = data_type->create_column();
        (*col_name_to_block_idx_ref())[i.column_lower_case] =
                static_cast<uint32_t>(block->columns());
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, i.column_lower_case));
    }
    return Status::OK();
}

// ============================================================================
// on_after_read_block: shrink ACID columns from block
// ============================================================================
Status TransactionalHiveReader::on_after_read_block(Block* block, size_t* /*read_rows*/) {
    Block::erase_useless_column(block, block->columns() - TransactionalHive::READ_PARAMS.size());
    for (const auto& i : TransactionalHive::READ_PARAMS) {
        col_name_to_block_idx_ref()->erase(i.column_lower_case);
    }
    return Status::OK();
}

} // namespace doris
