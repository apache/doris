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

#include "vec/aggregate_functions/aggregate_function_python_udaf.h"

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <fmt/format.h>

#include "common/exception.h"
#include "common/logging.h"
#include "runtime/define_primitive_type.h"
#include "runtime/user_function_cache.h"
#include "udf/python/python_env.h"
#include "udf/python/python_server.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/timezone_utils.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

Status AggregatePythonUDAFData::create(int64_t place) {
    DCHECK(client) << "Client must be set before calling create";
    RETURN_IF_ERROR(client->create(place));
    return Status::OK();
}

Status AggregatePythonUDAFData::add(int64_t place_id, const IColumn** columns,
                                    int64_t row_num_start, int64_t row_num_end,
                                    const DataTypes& argument_types) {
    DCHECK(client) << "Client must be set before calling add";

    // Zero-copy: Use full columns with range specification
    Block input_block;
    for (size_t i = 0; i < argument_types.size(); ++i) {
        input_block.insert(
                ColumnWithTypeAndName(columns[i]->get_ptr(), argument_types[i], std::to_string(i)));
    }

    std::shared_ptr<arrow::Schema> schema;
    RETURN_IF_ERROR(
            get_arrow_schema_from_block(input_block, &schema, TimezoneUtils::default_time_zone));
    cctz::time_zone timezone_obj;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, timezone_obj);

    std::shared_ptr<arrow::RecordBatch> batch;
    // Zero-copy: convert only the specified range
    RETURN_IF_ERROR(convert_to_arrow_batch(input_block, schema, arrow::default_memory_pool(),
                                           &batch, timezone_obj, row_num_start, row_num_end));
    // Send the batch (already sliced in convert_to_arrow_batch)
    // Single place mode: no places column needed
    RETURN_IF_ERROR(client->accumulate(place_id, true, *batch, 0, batch->num_rows()));
    return Status::OK();
}

Status AggregatePythonUDAFData::add_batch(AggregateDataPtr* places, size_t place_offset,
                                          size_t num_rows, const IColumn** columns,
                                          const DataTypes& argument_types, size_t start,
                                          size_t end) {
    DCHECK(client) << "Client must be set before calling add_batch";
    DCHECK(end > start) << "end must be greater than start";
    DCHECK(end <= num_rows) << "end must not exceed num_rows";

    size_t slice_rows = end - start;
    Block input_block;
    for (size_t i = 0; i < argument_types.size(); ++i) {
        DCHECK(columns[i]->size() == num_rows) << "Column size must match num_rows";
        input_block.insert(
                ColumnWithTypeAndName(columns[i]->get_ptr(), argument_types[i], std::to_string(i)));
    }

    auto places_col = ColumnInt64::create(num_rows);
    auto& places_data = places_col->get_data();

    // Fill places column with place IDs for the slice [start, end)
    for (size_t i = start; i < end; ++i) {
        places_data[i] = reinterpret_cast<int64_t>(places[i] + place_offset);
    }

    static DataTypePtr places_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, false);
    input_block.insert(ColumnWithTypeAndName(std::move(places_col), places_type, "places"));

    std::shared_ptr<arrow::Schema> schema;
    RETURN_IF_ERROR(
            get_arrow_schema_from_block(input_block, &schema, TimezoneUtils::default_time_zone));
    cctz::time_zone timezone_obj;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, timezone_obj);

    std::shared_ptr<arrow::RecordBatch> batch;
    // Zero-copy: convert only the [start, end) range
    // This slice includes the places column automatically
    RETURN_IF_ERROR(convert_to_arrow_batch(input_block, schema, arrow::default_memory_pool(),
                                           &batch, timezone_obj, start, end));
    // Send entire batch (already contains places column) to Python
    // place_id=0 is ignored when is_single_place=false
    RETURN_IF_ERROR(client->accumulate(0, false, *batch, 0, slice_rows));
    return Status::OK();
}

Status AggregatePythonUDAFData::merge(const AggregatePythonUDAFData& rhs, int64_t place) {
    DCHECK(client) << "Client must be set before calling merge";

    // Get serialized state from rhs (already stored in serialize_data by read())
    auto serialized_state = arrow::Buffer::Wrap(
            reinterpret_cast<const uint8_t*>(rhs.serialize_data.data()), rhs.serialize_data.size());
    RETURN_IF_ERROR(client->merge(place, serialized_state));
    return Status::OK();
}

Status AggregatePythonUDAFData::write(BufferWritable& buf, int64_t place) const {
    DCHECK(client) << "Client must be set before calling write";

    // Serialize state from Python server
    std::shared_ptr<arrow::Buffer> serialized_state;
    RETURN_IF_ERROR(client->serialize(place, &serialized_state));
    const char* data = reinterpret_cast<const char*>(serialized_state->data());
    size_t size = serialized_state->size();
    buf.write_binary(StringRef {data, size});
    return Status::OK();
}

void AggregatePythonUDAFData::read(BufferReadable& buf) {
    // Read serialized state from buffer into serialize_data
    // This will be used later by merge() in deserialize_and_merge()
    buf.read_binary(serialize_data);
}

Status AggregatePythonUDAFData::reset(int64_t place) {
    DCHECK(client) << "Client must be set before calling reset";
    RETURN_IF_ERROR(client->reset(place));
    // After reset, state still exists but is back to initial state
    return Status::OK();
}

Status AggregatePythonUDAFData::destroy(int64_t place) {
    DCHECK(client) << "Client must be set before calling destroy";
    RETURN_IF_ERROR(client->destroy(place));
    return Status::OK();
}

Status AggregatePythonUDAFData::get(IColumn& to, const DataTypePtr& result_type,
                                    int64_t place) const {
    DCHECK(client) << "Client must be set before calling get";

    // Get final result from Python server
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(client->finalize(place, &result));

    // Convert Arrow RecordBatch to Block
    Block result_block;
    DataTypes types = {result_type};
    cctz::time_zone timezone_obj;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, timezone_obj);
    RETURN_IF_ERROR(convert_from_arrow_batch(result, types, &result_block, timezone_obj));

    // Insert the result value into output column
    if (result_block.rows() != 1) {
        return Status::InternalError("Expected 1 row in result block, got {}", result_block.rows());
    }

    auto& result_column = result_block.get_by_position(0).column;
    to.insert_from(*result_column, 0);
    return Status::OK();
}

Status AggregatePythonUDAF::open() {
    // Build function metadata from TFunction
    _func_meta.id = _fn.id;
    _func_meta.name = _fn.name.function_name;

    // For UDAF, symbol is in aggregate_fn
    if (_fn.__isset.aggregate_fn && _fn.aggregate_fn.__isset.symbol) {
        _func_meta.symbol = _fn.aggregate_fn.symbol;
    } else {
        return Status::InvalidArgument("Python UDAF symbol is not set");
    }

    // Determine load type (inline code or module)
    if (!_fn.function_code.empty()) {
        _func_meta.type = PythonUDFLoadType::INLINE;
        _func_meta.location = "inline";
        _func_meta.inline_code = _fn.function_code;
    } else if (!_fn.hdfs_location.empty()) {
        _func_meta.type = PythonUDFLoadType::MODULE;
        _func_meta.location = _fn.hdfs_location;
        _func_meta.checksum = _fn.checksum;
    } else {
        _func_meta.type = PythonUDFLoadType::UNKNOWN;
        _func_meta.location = "unknown";
    }

    _func_meta.input_types = argument_types;
    _func_meta.return_type = _return_type;
    _func_meta.client_type = PythonClientType::UDAF;

    // Get Python version
    if (_fn.__isset.runtime_version && !_fn.runtime_version.empty()) {
        RETURN_IF_ERROR(PythonVersionManager::instance().get_version(_fn.runtime_version,
                                                                     &_python_version));
    } else {
        return Status::InvalidArgument("Python UDAF runtime version is not set");
    }

    _func_meta.runtime_version = _python_version.full_version;
    RETURN_IF_ERROR(_func_meta.check());
    _func_meta.always_nullable = _return_type->is_nullable();

    LOG(INFO) << fmt::format("Creating Python UDAF: {}, runtime_version: {}, func_meta: {}",
                             _fn.name.function_name, _python_version.to_string(),
                             _func_meta.to_string());

    if (_func_meta.type == PythonUDFLoadType::MODULE) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_pypath(
                _func_meta.id, _func_meta.location, _func_meta.checksum, &_func_meta.location));
    }

    return Status::OK();
}

void AggregatePythonUDAF::create(AggregateDataPtr __restrict place) const {
    std::call_once(_schema_init_flag, [this]() {
        std::vector<std::shared_ptr<arrow::Field>> fields;

        std::string timezone = TimezoneUtils::default_time_zone;
        for (size_t i = 0; i < argument_types.size(); ++i) {
            std::shared_ptr<arrow::DataType> arrow_type;
            Status st = convert_to_arrow_type(argument_types[i], &arrow_type, timezone);
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "Failed to convert argument type {} to Arrow type: {}", i,
                                       st.to_string());
            }
            fields.push_back(arrow::field(std::to_string(i), arrow_type));
        }

        // Add places column for GROUP BY aggregation (always included, NULL in single-place mode)
        fields.push_back(arrow::field("places", arrow::int64()));
        // Add binary_data column for merge operations
        fields.push_back(arrow::field("binary_data", arrow::binary()));
        _schema = arrow::schema(fields);
    });

    // Initialize the data structure
    new (place) Data();
    DCHECK(reinterpret_cast<Data*>(place)) << "Place must not be null";

    if (Status st = PythonServerManager::instance().get_client(
                _func_meta, _python_version, &(this->data(place).client), _schema);
        UNLIKELY(!st.ok())) {
        this->data(place).~Data();
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Failed to get Python UDAF client: {}",
                               st.to_string());
    }

    // Initialize UDAF state in Python server
    int64_t place_id = reinterpret_cast<int64_t>(place);
    if (Status st = this->data(place).create(place_id); UNLIKELY(!st.ok())) {
        this->data(place).~Data();
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::destroy(AggregateDataPtr __restrict place) const noexcept {
    try {
        int64_t place_id = reinterpret_cast<int64_t>(place);

        // Destroy state in Python server
        if (this->data(place).client) {
            Status st = this->data(place).destroy(place_id);
            if (UNLIKELY(!st.ok())) {
                LOG(WARNING) << "Failed to destroy Python UDAF state for place_id=" << place_id
                             << ", function=" << _func_meta.name << ": " << st.to_string();
            }

            this->data(place).client.reset();
        }

        this->data(place).~Data();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in AggregatePythonUDAF::destroy: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Unknown exception in AggregatePythonUDAF::destroy";
    }
}

void AggregatePythonUDAF::add(AggregateDataPtr __restrict place, const IColumn** columns,
                              ssize_t row_num, Arena&) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).add(place_id, columns, row_num, row_num + 1, argument_types);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::add_batch(size_t batch_size, AggregateDataPtr* places,
                                    size_t place_offset, const IColumn** columns, Arena&,
                                    bool /*agg_many*/) const {
    if (batch_size == 0) return;

    size_t start = 0;
    while (start < batch_size) {
        // Get the starting place for this segment
        AggregateDataPtr start_place = places[start] + place_offset;
        auto& start_place_data = this->data(start_place);
        // Get the process for this segment
        const auto* current_process = start_place_data.client->get_process().get();

        // Scan forward to find the end of this consecutive segment (same process)
        size_t end = start + 1;
        while (end < batch_size) {
            AggregateDataPtr end_place = places[end] + place_offset;
            auto& end_place_data = this->data(end_place);
            const auto* next_process = end_place_data.client->get_process().get();
            // If different process, end the current segment
            if (*next_process != *current_process) break;
            ++end;
        }

        // Send this segment to Python with zero-copy
        // Pass places array and let add_batch construct place_ids on-demand
        Status st = start_place_data.add_batch(places, place_offset, batch_size, columns,
                                               argument_types, start, end);

        if (UNLIKELY(!st.ok())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Failed to send segment to Python: " + st.to_string());
        }

        start = end;
    }
}

void AggregatePythonUDAF::add_batch_single_place(size_t batch_size, AggregateDataPtr place,
                                                 const IColumn** columns, Arena&) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).add(place_id, columns, 0, batch_size, argument_types);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::add_range_single_place(int64_t partition_start, int64_t partition_end,
                                                 int64_t frame_start, int64_t frame_end,
                                                 AggregateDataPtr place, const IColumn** columns,
                                                 Arena& arena, UInt8* current_window_empty,
                                                 UInt8* current_window_has_inited) const {
    // Calculate actual frame range
    frame_start = std::max<int64_t>(frame_start, partition_start);
    frame_end = std::min<int64_t>(frame_end, partition_end);

    if (frame_start >= frame_end) {
        if (!*current_window_has_inited) {
            *current_window_empty = true;
        }
        return;
    }

    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).add(place_id, columns, frame_start, frame_end, argument_types);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }

    *current_window_empty = false;
    *current_window_has_inited = true;
}

void AggregatePythonUDAF::reset(AggregateDataPtr place) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).reset(place_id);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
                                Arena&) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).merge(this->data(rhs), place_id);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::serialize(ConstAggregateDataPtr __restrict place,
                                    BufferWritable& buf) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).write(buf, place_id);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

void AggregatePythonUDAF::deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                                      Arena&) const {
    this->data(place).read(buf);
}

void AggregatePythonUDAF::insert_result_into(ConstAggregateDataPtr __restrict place,
                                             IColumn& to) const {
    int64_t place_id = reinterpret_cast<int64_t>(place);
    Status st = this->data(place).get(to, _return_type, place_id);
    if (UNLIKELY(!st.ok())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, st.to_string());
    }
}

} // namespace doris::vectorized
