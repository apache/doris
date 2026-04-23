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
#include <memory>
#include <vector>

#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/table_schema_change_helper.h"
namespace doris {

class HiveOrcReader final : public OrcReader, public TableSchemaChangeHelper {
public:
    ENABLE_FACTORY_CREATOR(HiveOrcReader);
    HiveOrcReader(RuntimeProfile* profile, RuntimeState* state, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, const std::string& ctz,
                  io::IOContext* io_ctx, const std::set<TSlotId>* is_file_slot,
                  FileMetaCache* meta_cache = nullptr, bool enable_lazy_mat = true)
            : OrcReader(profile, state, params, range, batch_size, ctz, io_ctx, meta_cache,
                        enable_lazy_mat),
              _is_file_slot(is_file_slot) {}

    ~HiveOrcReader() final = default;

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override;

private:
    static ColumnIdResult _create_column_ids(const orc::Type* orc_type,
                                             const TupleDescriptor* tuple_descriptor);

    static ColumnIdResult _create_column_ids_by_top_level_col_index(
            const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor);

    const std::set<TSlotId>* _is_file_slot = nullptr;
};

class HiveParquetReader final : public ParquetReader, public TableSchemaChangeHelper {
public:
    ENABLE_FACTORY_CREATOR(HiveParquetReader);
    HiveParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                      io::IOContext* io_ctx, RuntimeState* state,
                      const std::set<TSlotId>* is_file_slot, FileMetaCache* meta_cache = nullptr,
                      bool enable_lazy_mat = true)
            : ParquetReader(profile, params, range, batch_size, ctz, io_ctx, state, meta_cache,
                            enable_lazy_mat),
              _is_file_slot(is_file_slot) {}

    ~HiveParquetReader() final = default;

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override;

private:
    static ColumnIdResult _create_column_ids(const FieldDescriptor* field_desc,
                                             const TupleDescriptor* tuple_descriptor);

    static ColumnIdResult _create_column_ids_by_top_level_col_index(
            const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor);

    const std::set<TSlotId>* _is_file_slot = nullptr;
};
} // namespace doris
