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

#include <gen_cpp/DataSinks_types.h>

#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exec/format/table/iceberg/partition_spec_parser.h"
#include "vec/exec/format/table/iceberg/schema_parser.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/async_result_writer.h"
#include "vec/sink/writer/iceberg/partition_data.h"
#include "vec/sink/writer/iceberg/partition_transformers.h"

namespace doris {

class ObjectPool;
class RuntimeState;

namespace vectorized {

class IColumn;
class IPartitionWriterBase;
class VIcebergSortWriter;
struct ColumnWithTypeAndName;

class VIcebergTableWriter final : public AsyncResultWriter {
public:
    VIcebergTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                        std::shared_ptr<pipeline::Dependency> dep,
                        std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VIcebergTableWriter() = default;

    Status init_properties(ObjectPool* pool, const RowDescriptor& row_desc) {
        _row_desc = &row_desc;
        return Status::OK();
    }

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status) override;

    std::shared_ptr<IPartitionWriterBase> _current_writer;

private:
    class IcebergPartitionColumn {
    public:
        IcebergPartitionColumn(const iceberg::PartitionField& field,
                               const PrimitiveType& source_type, int source_idx,
                               std::unique_ptr<PartitionColumnTransform> partition_column_transform)
                : _field(field),
                  _source_type(source_type),
                  _source_idx(source_idx),
                  _partition_column_transform(std::move(partition_column_transform)) {}

    public:
        const iceberg::PartitionField& field() const { return _field; }

        const PrimitiveType& source_type() const { return _source_type; }
        int source_idx() const { return _source_idx; }

        const PartitionColumnTransform& partition_column_transform() const {
            return *_partition_column_transform;
        }

        PartitionColumnTransform& partition_column_transform() {
            return *_partition_column_transform;
        }

    private:
        const iceberg::PartitionField& _field;
        PrimitiveType _source_type;
        int _source_idx;
        std::unique_ptr<PartitionColumnTransform> _partition_column_transform;
    };

    std::vector<IcebergPartitionColumn> _to_iceberg_partition_columns();

    std::string _partition_to_path(const doris::iceberg::StructLike& data);
    std::string _escape(const std::string& path);
    std::vector<std::string> _partition_values(const doris::iceberg::StructLike& data);

    // Initialize static partition values from Thrift config
    void _init_static_partition_values();
    // Build static partition path from static partition values
    std::string _build_static_partition_path();

    std::shared_ptr<IPartitionWriterBase> _create_partition_writer(
            vectorized::Block* transformed_block, int position,
            const std::string* file_name = nullptr, int file_name_index = 0);

    PartitionData _get_partition_data(vectorized::Block* block, int position);

    std::any _get_iceberg_partition_value(const PrimitiveType& type_desc,
                                          const ColumnWithTypeAndName& partition_column,
                                          int position);

    std::string _compute_file_name();

    Status _filter_block(doris::vectorized::Block& block, const vectorized::IColumn::Filter* filter,
                         doris::vectorized::Block* output_block);

    // Currently it is a copy, maybe it is better to use move semantics to eliminate it.
    TDataSink _t_sink;
    RuntimeState* _state = nullptr;

    // Target file size in bytes for controlling when to split files
    int64_t _target_file_size_bytes = 0;

    std::shared_ptr<doris::iceberg::Schema> _schema;
    std::unique_ptr<doris::iceberg::PartitionSpec> _partition_spec;

    std::set<size_t> _non_write_columns_indices;
    std::vector<IcebergPartitionColumn> _iceberg_partition_columns;

    // Static partition values for each partition column (indexed by column index)
    // If _partition_column_is_static[i] is true, this stores the static value.
    std::vector<std::string> _partition_column_static_values;
    // Flags to indicate if the partition column at index i is static
    std::vector<uint8_t> _partition_column_is_static;

    // Whether any static partition columns are specified
    bool _has_static_partition = false;
    // Whether ALL partition columns are statically specified (full static mode)
    // If false but _has_static_partition is true, it's partial static (hybrid) mode
    bool _is_full_static_partition = false;
    // Pre-computed static partition path prefix (for full static mode, this is the complete path)
    std::string _static_partition_path;
    // Pre-computed static partition value list (for full static mode only)
    std::vector<std::string> _static_partition_value_list;

    std::unordered_map<std::string, std::shared_ptr<IPartitionWriterBase>> _partitions_to_writers;
    VExprContextSPtrs _write_output_vexpr_ctxs;
    size_t _row_count = 0;
    const RowDescriptor* _row_desc = nullptr;

    // profile counters
    int64_t _send_data_ns = 0;
    int64_t _partition_writers_dispatch_ns = 0;
    int64_t _partition_writers_write_ns = 0;
    int64_t _close_ns = 0;
    int64_t _write_file_count = 0;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_dispatch_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_write_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_count = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _write_file_counter = nullptr;
};
} // namespace vectorized
} // namespace doris
