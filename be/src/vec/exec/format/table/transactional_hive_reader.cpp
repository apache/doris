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

#include "transactional_hive_reader.h"

#include <re2/re2.h>

#include "transactional_hive_common.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace io {
struct IOContext;
} // namespace io
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

TransactionalHiveReader::TransactionalHiveReader(std::unique_ptr<GenericReader> file_format_reader,
                                                 RuntimeProfile* profile, RuntimeState* state,
                                                 const TFileScanRangeParams& params,
                                                 const TFileRangeDesc& range, io::IOContext* io_ctx,
                                                 FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache) {
    static const char* transactional_hive_profile = "TransactionalHiveProfile";
    ADD_TIMER(_profile, transactional_hive_profile);
    _transactional_orc_profile.num_delete_files =
            ADD_CHILD_COUNTER(_profile, "NumDeleteFiles", TUnit::UNIT, transactional_hive_profile);
    _transactional_orc_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, transactional_hive_profile);
    _transactional_orc_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", transactional_hive_profile);
}

Status TransactionalHiveReader::init_reader(
        const std::vector<std::string>& column_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
    _col_names.insert(_col_names.end(), column_names.begin(), column_names.end());
    _col_names.insert(_col_names.end(), TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                      TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end());

    // https://issues.apache.org/jira/browse/HIVE-15190
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
    const auto& orc_type = *orc_type_ptr;

    for (auto idx = 0; idx < TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.size(); idx++) {
        table_info_node_ptr->add_children(TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE[idx],
                                          TransactionalHive::READ_ROW_COLUMN_NAMES[idx],
                                          std::make_shared<ScalarNode>());
    }

    auto row_orc_type = orc_type.getSubtype(TransactionalHive::ROW_OFFSET);
    // struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint,row:struct<id:int,name:string>>
    std::vector<std::string> row_names;
    std::map<std::string, uint64_t> row_names_map;
    for (uint64_t idx = 0; idx < row_orc_type->getSubtypeCount(); idx++) {
        const auto& file_column_name = row_orc_type->getFieldName(idx);
        row_names.emplace_back(file_column_name);
        row_names_map.emplace(file_column_name, idx);
    }

    // use name for match.
    for (const auto& slot : tuple_descriptor->slots()) {
        const auto& slot_name = slot->col_name();

        if (std::count(TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                       TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end(), slot_name) > 0) {
            return Status::InternalError("xxxx");
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

    Status status = orc_reader->init_reader(
            &_col_names, colname_to_value_range, conjuncts, true, tuple_descriptor, row_descriptor,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts, table_info_node_ptr);
    return status;
}

Status TransactionalHiveReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    for (const auto& i : TransactionalHive::READ_PARAMS) {
        DataTypePtr data_type = get_data_type_with_default_argument(
                DataTypeFactory::instance().create_data_type(i.type, false));
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, i.column_lower_case));
    }
    auto res = _file_format_reader->get_next_block(block, read_rows, eof);
    Block::erase_useless_column(block, block->columns() - TransactionalHive::READ_PARAMS.size());
    return res;
}

Status TransactionalHiveReader::init_row_filters() {
    std::string data_file_path = _range.path;
    // the path in _range is remove the namenode prefix,
    // and the file_path in delete file is full path, so we should add it back.
    if (_params.__isset.hdfs_params && _params.hdfs_params.__isset.fs_name) {
        std::string fs_name = _params.hdfs_params.fs_name;
        if (!starts_with(data_file_path, fs_name)) {
            data_file_path = fs_name + data_file_path;
        }
    }

    auto* orc_reader = (OrcReader*)(_file_format_reader.get());
    std::vector<std::string> delete_file_col_names;
    int64_t num_delete_rows = 0;
    int64_t num_delete_files = 0;
    std::filesystem::path file_path(data_file_path);

    //See https://github.com/apache/hive/commit/ffee30e6267e85f00a22767262192abb9681cfb7#diff-5fe26c36b4e029dcd344fc5d484e7347R165
    // bucket_xxx_attemptId => bucket_xxx
    // bucket_xxx           => bucket_xxx
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
         _range.table_format_params.transactional_hive_params.delete_deltas) {
        const std::string file_name = file_path.filename().string();

        //need opt.
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
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = delete_file;
        delete_range.start_offset = 0;
        delete_range.size = -1;
        delete_range.file_size = -1;

        OrcReader delete_reader(_profile, _state, _params, delete_range, _MIN_BATCH_SIZE,
                                _state->timezone(), _io_ctx, _meta_cache, false);

        auto acid_info_node = std::make_shared<StructNode>();
        for (auto idx = 0; idx < TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE.size();
             idx++) {
            auto const& table_column_name =
                    TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE[idx];
            auto const& file_column_name = TransactionalHive::DELETE_ROW_COLUMN_NAMES[idx];
            acid_info_node->add_children(table_column_name, file_column_name,
                                         std::make_shared<ScalarNode>());
        }

        RETURN_IF_ERROR(delete_reader.init_reader(
                &TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE, nullptr, {}, false, nullptr,
                nullptr, nullptr, nullptr, acid_info_node));

        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        RETURN_IF_ERROR(delete_reader.set_fill_columns(partition_columns, missing_columns));

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
                    _delete_rows.insert(delete_row_id);
                    ++num_delete_rows;
                }
            }
        }
        ++num_delete_files;
    }
    if (num_delete_rows > 0) {
        orc_reader->set_push_down_agg_type(TPushAggOp::NONE);
        orc_reader->set_delete_rows(&_delete_rows);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_files, num_delete_files);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
