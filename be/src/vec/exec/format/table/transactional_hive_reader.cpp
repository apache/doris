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

#include "runtime/runtime_state.h"
#include "transactional_hive_common.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/orc/vorc_reader.h"

namespace doris {

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
                                                 const TFileRangeDesc& range, io::IOContext* io_ctx)
        : TableFormatReader(std::move(file_format_reader)),
          _profile(profile),
          _state(state),
          _params(params),
          _range(range),
          _io_ctx(io_ctx) {
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
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    OrcReader* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
    _col_names.insert(_col_names.end(), column_names.begin(), column_names.end());
    _col_names.insert(_col_names.end(), TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                      TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end());
    Status status = orc_reader->init_reader(
            &_col_names, colname_to_value_range, conjuncts, true, tuple_descriptor, row_descriptor,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    return status;
}

Status TransactionalHiveReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    for (int i = 0; i < TransactionalHive::READ_PARAMS.size(); ++i) {
        DataTypePtr data_type = DataTypeFactory::instance().create_data_type(
                TypeDescriptor(TransactionalHive::READ_PARAMS[i].type), false);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                            TransactionalHive::READ_PARAMS[i].column_lower_case));
    }
    auto res = _file_format_reader->get_next_block(block, read_rows, eof);
    Block::erase_useless_column(block, block->columns() - TransactionalHive::READ_PARAMS.size());
    return res;
}

Status TransactionalHiveReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    return _file_format_reader->get_columns(name_to_type, missing_cols);
}

Status TransactionalHiveReader::init_row_filters(const TFileRangeDesc& range,
                                                 io::IOContext* io_ctx) {
    std::string data_file_path = _range.path;
    // the path in _range is remove the namenode prefix,
    // and the file_path in delete file is full path, so we should add it back.
    if (_params.__isset.hdfs_params && _params.hdfs_params.__isset.fs_name) {
        std::string fs_name = _params.hdfs_params.fs_name;
        if (!starts_with(data_file_path, fs_name)) {
            data_file_path = fs_name + data_file_path;
        }
    }

    OrcReader* orc_reader = (OrcReader*)(_file_format_reader.get());
    std::vector<std::string> delete_file_col_names;
    int64_t num_delete_rows = 0;
    int64_t num_delete_files = 0;
    std::filesystem::path file_path(data_file_path);

    SCOPED_TIMER(_transactional_orc_profile.delete_files_read_time);
    for (auto& delete_delta : range.table_format_params.transactional_hive_params.delete_deltas) {
        const std::string file_name = file_path.filename().string();
        auto iter = std::find(delete_delta.file_names.begin(), delete_delta.file_names.end(),
                              file_name);
        if (iter == delete_delta.file_names.end()) {
            continue;
        }
        auto delete_file = fmt::format("{}/{}", delete_delta.directory_location, file_name);

        TFileRangeDesc delete_range;
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = delete_file;
        delete_range.start_offset = 0;
        delete_range.size = -1;
        delete_range.file_size = -1;

        OrcReader delete_reader(_profile, _state, _params, delete_range, _MIN_BATCH_SIZE,
                                _state->timezone(), _io_ctx, false);

        RETURN_IF_ERROR(
                delete_reader.init_reader(&TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE,
                                          nullptr, {}, false, nullptr, nullptr, nullptr, nullptr));

        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        RETURN_IF_ERROR(delete_reader.set_fill_columns(partition_columns, missing_columns));

        bool eof = false;
        while (!eof) {
            Block block;
            for (int i = 0; i < TransactionalHive::DELETE_ROW_PARAMS.size(); ++i) {
                DataTypePtr data_type = DataTypeFactory::instance().create_data_type(
                        TransactionalHive::DELETE_ROW_PARAMS[i].type, false);
                MutableColumnPtr data_column = data_type->create_column();
                block.insert(ColumnWithTypeAndName(
                        std::move(data_column), data_type,
                        TransactionalHive::DELETE_ROW_PARAMS[i].column_lower_case));
            }
            eof = false;
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader.get_next_block(&block, &read_rows, &eof));
            if (read_rows > 0) {
                static int ORIGINAL_TRANSACTION_INDEX = 0;
                static int BUCKET_ID_INDEX = 1;
                static int ROW_ID_INDEX = 2;
                const ColumnInt64& original_transaction_column = assert_cast<const ColumnInt64&>(
                        *block.get_by_position(ORIGINAL_TRANSACTION_INDEX).column);
                const ColumnInt32& bucket_id_column = assert_cast<const ColumnInt32&>(
                        *block.get_by_position(BUCKET_ID_INDEX).column);
                const ColumnInt64& row_id_column = assert_cast<const ColumnInt64&>(
                        *block.get_by_position(ROW_ID_INDEX).column);

                DCHECK_EQ(original_transaction_column.size(), read_rows);
                DCHECK_EQ(bucket_id_column.size(), read_rows);
                DCHECK_EQ(row_id_column.size(), read_rows);

                for (int i = 0; i < read_rows; ++i) {
                    Int64 original_transaction = original_transaction_column.get_int(i);
                    Int32 bucket_id = bucket_id_column.get_int(i);
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
        orc_reader->set_delete_rows(&_delete_rows);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_files, num_delete_files);
        COUNTER_UPDATE(_transactional_orc_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}
} // namespace doris::vectorized
