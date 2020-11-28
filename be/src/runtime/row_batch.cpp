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

#include "runtime/row_batch.h"

#include <snappy/snappy.h>
#include <stdint.h> // for intptr_t

#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
//#include "runtime/mem_tracker.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/data.pb.h"
#include "util/debug_util.h"

using std::vector;

namespace doris {

const int RowBatch::AT_CAPACITY_MEM_USAGE = 8 * 1024 * 1024;
const int RowBatch::FIXED_LEN_BUFFER_LIMIT = AT_CAPACITY_MEM_USAGE / 2;

RowBatch::RowBatch(const RowDescriptor& row_desc, int capacity, MemTracker* mem_tracker)
        : _mem_tracker(mem_tracker),
          _has_in_flight_row(false),
          _num_rows(0),
          _capacity(capacity),
          _flush(FlushMode::NO_FLUSH_RESOURCES),
          _needs_deep_copy(false),
          _num_tuples_per_row(row_desc.tuple_descriptors().size()),
          _row_desc(row_desc),
          _auxiliary_mem_usage(0),
          _need_to_return(false),
          _tuple_data_pool(new MemPool(_mem_tracker)),
          _agg_object_pool(new ObjectPool()) {
    DCHECK(_mem_tracker != NULL);
    DCHECK_GT(capacity, 0);
    _tuple_ptrs_size = _capacity * _num_tuples_per_row * sizeof(Tuple*);
    DCHECK_GT(_tuple_ptrs_size, 0);
    // TODO: switch to Init() pattern so we can check memory limit and return Status.
    if (config::enable_partitioned_aggregation) {
        _mem_tracker->Consume(_tuple_ptrs_size);
        _tuple_ptrs = reinterpret_cast<Tuple**>(malloc(_tuple_ptrs_size));
        DCHECK(_tuple_ptrs != NULL);
    } else {
        _tuple_ptrs = reinterpret_cast<Tuple**>(_tuple_data_pool->allocate(_tuple_ptrs_size));
    }
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const PRowBatch& input_batch, MemTracker* tracker)
        : _mem_tracker(tracker),
          _has_in_flight_row(false),
          _num_rows(input_batch.num_rows()),
          _capacity(_num_rows),
          _flush(FlushMode::NO_FLUSH_RESOURCES),
          _needs_deep_copy(false),
          _num_tuples_per_row(input_batch.row_tuples_size()),
          _row_desc(row_desc),
          _auxiliary_mem_usage(0),
          _need_to_return(false),
          _tuple_data_pool(new MemPool(_mem_tracker)),
          _agg_object_pool(new ObjectPool()) {
    DCHECK(_mem_tracker != nullptr);
    _tuple_ptrs_size = _num_rows * _num_tuples_per_row * sizeof(Tuple*);
    DCHECK_GT(_tuple_ptrs_size, 0);
    // TODO: switch to Init() pattern so we can check memory limit and return Status.
    if (config::enable_partitioned_aggregation) {
        _mem_tracker->Consume(_tuple_ptrs_size);
        _tuple_ptrs = reinterpret_cast<Tuple**>(malloc(_tuple_ptrs_size));
        DCHECK(_tuple_ptrs != nullptr);
    } else {
        _tuple_ptrs = reinterpret_cast<Tuple**>(_tuple_data_pool->allocate(_tuple_ptrs_size));
    }

    uint8_t* tuple_data = nullptr;
    if (input_batch.is_compressed()) {
        // Decompress tuple data into data pool
        const char* compressed_data = input_batch.tuple_data().c_str();
        size_t compressed_size = input_batch.tuple_data().size();
        size_t uncompressed_size = 0;
        bool success =
                snappy::GetUncompressedLength(compressed_data, compressed_size, &uncompressed_size);
        DCHECK(success) << "snappy::GetUncompressedLength failed";
        tuple_data = reinterpret_cast<uint8_t*>(_tuple_data_pool->allocate(uncompressed_size));
        success = snappy::RawUncompress(compressed_data, compressed_size,
                                        reinterpret_cast<char*>(tuple_data));
        DCHECK(success) << "snappy::RawUncompress failed";
    } else {
        // Tuple data uncompressed, copy directly into data pool
        tuple_data = _tuple_data_pool->allocate(input_batch.tuple_data().size());
        memcpy(tuple_data, input_batch.tuple_data().c_str(), input_batch.tuple_data().size());
    }

    // convert input_batch.tuple_offsets into pointers
    int tuple_idx = 0;
    for (auto offset : input_batch.tuple_offsets()) {
        if (offset == -1) {
            _tuple_ptrs[tuple_idx++] = nullptr;
        } else {
            _tuple_ptrs[tuple_idx++] = reinterpret_cast<Tuple*>(tuple_data + offset);
        }
    }

    // Check whether we have slots that require offset-to-pointer conversion.
    if (!_row_desc.has_varlen_slots()) {
        return;
    }
    const std::vector<TupleDescriptor*>& tuple_descs = _row_desc.tuple_descriptors();

    // For every unique tuple, convert string offsets contained in tuple data into
    // pointers. Tuples were serialized in the order we are deserializing them in,
    // so the first occurrence of a tuple will always have a higher offset than any tuple
    // we already converted.
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        std::vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
        for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
            if ((*desc)->string_slots().empty()) {
                continue;
            }
            Tuple* tuple = row->get_tuple(j);
            if (tuple == nullptr) {
                continue;
            }

            for (auto slot : (*desc)->string_slots()) {
                DCHECK(slot->type().is_string_type());
                StringValue* string_val = tuple->get_string_slot(slot->tuple_offset());
                int offset = reinterpret_cast<intptr_t>(string_val->ptr);
                string_val->ptr = reinterpret_cast<char*>(tuple_data + offset);

                // Why we do this mask? Field len of StringValue is changed from int to size_t in
                // Doris 0.11. When upgrading, some bits of len sent from 0.10 is random value,
                // this works fine in version 0.10, however in 0.11 this will lead to an invalid
                // length. So we make the high bits zero here.
                string_val->len &= 0x7FFFFFFFL;
            }
        }
    }
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch, MemTracker* tracker)
        : _mem_tracker(tracker),
          _has_in_flight_row(false),
          _num_rows(input_batch.num_rows),
          _capacity(_num_rows),
          _flush(FlushMode::NO_FLUSH_RESOURCES),
          _needs_deep_copy(false),
          _num_tuples_per_row(input_batch.row_tuples.size()),
          _row_desc(row_desc),
          _auxiliary_mem_usage(0),
          _need_to_return(false),
          _tuple_data_pool(new MemPool(_mem_tracker)),
          _agg_object_pool(new ObjectPool()) {
    DCHECK(_mem_tracker != NULL);
    _tuple_ptrs_size = _num_rows * input_batch.row_tuples.size() * sizeof(Tuple*);
    DCHECK_GT(_tuple_ptrs_size, 0);
    // TODO: switch to Init() pattern so we can check memory limit and return Status.
    if (config::enable_partitioned_aggregation) {
        _mem_tracker->Consume(_tuple_ptrs_size);
        _tuple_ptrs = reinterpret_cast<Tuple**>(malloc(_tuple_ptrs_size));
        DCHECK(_tuple_ptrs != NULL);
    } else {
        _tuple_ptrs = reinterpret_cast<Tuple**>(_tuple_data_pool->allocate(_tuple_ptrs_size));
    }

    uint8_t* tuple_data = NULL;
    if (input_batch.is_compressed) {
        // Decompress tuple data into data pool
        const char* compressed_data = input_batch.tuple_data.c_str();
        size_t compressed_size = input_batch.tuple_data.size();
        size_t uncompressed_size = 0;
        bool success =
                snappy::GetUncompressedLength(compressed_data, compressed_size, &uncompressed_size);
        DCHECK(success) << "snappy::GetUncompressedLength failed";
        tuple_data = reinterpret_cast<uint8_t*>(_tuple_data_pool->allocate(uncompressed_size));
        success = snappy::RawUncompress(compressed_data, compressed_size,
                                        reinterpret_cast<char*>(tuple_data));
        DCHECK(success) << "snappy::RawUncompress failed";
    } else {
        // Tuple data uncompressed, copy directly into data pool
        tuple_data = _tuple_data_pool->allocate(input_batch.tuple_data.size());
        memcpy(tuple_data, input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
    }

    // convert input_batch.tuple_offsets into pointers
    int tuple_idx = 0;
    for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets.begin();
         offset != input_batch.tuple_offsets.end(); ++offset) {
        if (*offset == -1) {
            _tuple_ptrs[tuple_idx++] = NULL;
        } else {
            // _tuple_ptrs[tuple_idx++] =
            //     reinterpret_cast<Tuple*>(_tuple_data_pool->get_data_ptr(*offset));
            _tuple_ptrs[tuple_idx++] = reinterpret_cast<Tuple*>(tuple_data + *offset);
        }
    }

    // Check whether we have slots that require offset-to-pointer conversion.
    if (!_row_desc.has_varlen_slots()) {
        return;
    }
    const std::vector<TupleDescriptor*>& tuple_descs = _row_desc.tuple_descriptors();

    // For every unique tuple, convert string offsets contained in tuple data into
    // pointers. Tuples were serialized in the order we are deserializing them in,
    // so the first occurrence of a tuple will always have a higher offset than any tuple
    // we already converted.
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        std::vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
        for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
            if ((*desc)->string_slots().empty()) {
                continue;
            }

            Tuple* tuple = row->get_tuple(j);
            if (tuple == NULL) {
                continue;
            }

            std::vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
            for (; slot != (*desc)->string_slots().end(); ++slot) {
                DCHECK((*slot)->type().is_string_type());
                StringValue* string_val = tuple->get_string_slot((*slot)->tuple_offset());

                int offset = reinterpret_cast<intptr_t>(string_val->ptr);
                string_val->ptr = reinterpret_cast<char*>(tuple_data + offset);

                // Why we do this mask? Field len of StringValue is changed from int to size_t in
                // Doris 0.11. When upgrading, some bits of len sent from 0.10 is random value,
                // this works fine in version 0.10, however in 0.11 this will lead to an invalid
                // length. So we make the high bits zero here.
                string_val->len &= 0x7FFFFFFFL;
            }
        }
    }
}

void RowBatch::clear() {
    if (_cleared) {
        return;
    }

    _tuple_data_pool->free_all();
    _agg_object_pool.reset(new ObjectPool());
    for (int i = 0; i < _io_buffers.size(); ++i) {
        _io_buffers[i]->return_buffer();
    }

    for (BufferInfo& buffer_info : _buffers) {
        ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(buffer_info.client, &buffer_info.buffer);
    }

    close_tuple_streams();
    for (int i = 0; i < _blocks.size(); ++i) {
        _blocks[i]->del();
    }
    if (config::enable_partitioned_aggregation) {
        DCHECK(_tuple_ptrs != NULL);
        free(_tuple_ptrs);
        _mem_tracker->Release(_tuple_ptrs_size);
        _tuple_ptrs = NULL;
    }
    _cleared = true;
}

RowBatch::~RowBatch() {
    clear();
}

int RowBatch::serialize(TRowBatch* output_batch) {
    // why does Thrift not generate a Clear() function?
    output_batch->row_tuples.clear();
    output_batch->tuple_offsets.clear();
    output_batch->is_compressed = false;

    output_batch->num_rows = _num_rows;
    _row_desc.to_thrift(&output_batch->row_tuples);
    output_batch->tuple_offsets.reserve(_num_rows * _num_tuples_per_row);

    int size = total_byte_size();
    output_batch->tuple_data.resize(size);

    // Copy tuple data, including strings, into output_batch (converting string
    // pointers into offsets in the process)
    int offset = 0; // current offset into output_batch->tuple_data
    char* tuple_data = const_cast<char*>(output_batch->tuple_data.c_str());

    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        const std::vector<TupleDescriptor*>& tuple_descs = _row_desc.tuple_descriptors();
        std::vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();

        for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
            if (row->get_tuple(j) == NULL) {
                // NULLs are encoded as -1
                output_batch->tuple_offsets.push_back(-1);
                continue;
            }

            // Record offset before creating copy (which increments offset and tuple_data)
            output_batch->tuple_offsets.push_back(offset);
            row->get_tuple(j)->deep_copy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
            DCHECK_LE(offset, size);
        }
    }

    DCHECK_EQ(offset, size);

    if (config::compress_rowbatches && size > 0) {
        // Try compressing tuple_data to _compression_scratch, swap if compressed data is
        // smaller
        int max_compressed_size = snappy::MaxCompressedLength(size);

        if (_compression_scratch.size() < max_compressed_size) {
            _compression_scratch.resize(max_compressed_size);
        }

        size_t compressed_size = 0;
        char* compressed_output = const_cast<char*>(_compression_scratch.c_str());
        snappy::RawCompress(output_batch->tuple_data.c_str(), size, compressed_output,
                            &compressed_size);

        if (LIKELY(compressed_size < size)) {
            _compression_scratch.resize(compressed_size);
            output_batch->tuple_data.swap(_compression_scratch);
            output_batch->is_compressed = true;
        }

        VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
    }

    // The size output_batch would be if we didn't compress tuple_data (will be equal to
    // actual batch size if tuple_data isn't compressed)
    return get_batch_size(*output_batch) - output_batch->tuple_data.size() + size;
}

int RowBatch::serialize(PRowBatch* output_batch) {
    // num_rows
    output_batch->set_num_rows(_num_rows);
    // row_tuples
    _row_desc.to_protobuf(output_batch->mutable_row_tuples());
    // tuple_offsets: must clear before reserve
    output_batch->clear_tuple_offsets();
    output_batch->mutable_tuple_offsets()->Reserve(_num_rows * _num_tuples_per_row);
    // is_compressed
    output_batch->set_is_compressed(false);
    // tuple data
    int size = total_byte_size();
    auto mutable_tuple_data = output_batch->mutable_tuple_data();
    mutable_tuple_data->resize(size);

    // Copy tuple data, including strings, into output_batch (converting string
    // pointers into offsets in the process)
    int offset = 0; // current offset into output_batch->tuple_data
    char* tuple_data = const_cast<char*>(mutable_tuple_data->data());
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        const std::vector<TupleDescriptor*>& tuple_descs = _row_desc.tuple_descriptors();
        std::vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
        for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
            if (row->get_tuple(j) == nullptr) {
                // NULLs are encoded as -1
                output_batch->mutable_tuple_offsets()->Add(-1);
                continue;
            }
            // Record offset before creating copy (which increments offset and tuple_data)
            output_batch->mutable_tuple_offsets()->Add(offset);
            row->get_tuple(j)->deep_copy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
            DCHECK_LE(offset, size);
        }
    }

    DCHECK_EQ(offset, size);

    if (config::compress_rowbatches && size > 0) {
        // Try compressing tuple_data to _compression_scratch, swap if compressed data is
        // smaller
        int max_compressed_size = snappy::MaxCompressedLength(size);

        if (_compression_scratch.size() < max_compressed_size) {
            _compression_scratch.resize(max_compressed_size);
        }

        size_t compressed_size = 0;
        char* compressed_output = const_cast<char*>(_compression_scratch.c_str());
        snappy::RawCompress(mutable_tuple_data->data(), size, compressed_output, &compressed_size);

        if (LIKELY(compressed_size < size)) {
            _compression_scratch.resize(compressed_size);
            mutable_tuple_data->swap(_compression_scratch);
            output_batch->set_is_compressed(true);
        }

        VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
    }

    // The size output_batch would be if we didn't compress tuple_data (will be equal to
    // actual batch size if tuple_data isn't compressed)
    return get_batch_size(*output_batch) - mutable_tuple_data->size() + size;
}

void RowBatch::add_io_buffer(DiskIoMgr::BufferDescriptor* buffer) {
    DCHECK(buffer != NULL);
    _io_buffers.push_back(buffer);
    _auxiliary_mem_usage += buffer->buffer_len();
    buffer->set_mem_tracker(std::shared_ptr<MemTracker>(_mem_tracker)); // TODO(yingchun): fixme
}

Status RowBatch::resize_and_allocate_tuple_buffer(RuntimeState* state, int64_t* tuple_buffer_size,
                                                  uint8_t** buffer) {
    const int row_size = _row_desc.get_row_size();
    // Avoid divide-by-zero. Don't need to modify capacity for empty rows anyway.
    if (row_size != 0) {
        _capacity = std::max(1, std::min(_capacity, FIXED_LEN_BUFFER_LIMIT / row_size));
    }
    *tuple_buffer_size = static_cast<int64_t>(row_size) * _capacity;
    // TODO(dhc): change allocate to try_allocate?
    *buffer = _tuple_data_pool->allocate(*tuple_buffer_size);
    if (*buffer == NULL) {
        std::stringstream ss;
        ss << "Failed to allocate tuple buffer" << *tuple_buffer_size;
        LOG(WARNING) << ss.str();
        return state->set_mem_limit_exceeded(ss.str());
    }
    return Status::OK();
}

void RowBatch::add_tuple_stream(BufferedTupleStream2* stream) {
    DCHECK(stream != NULL);
    _tuple_streams.push_back(stream);
    _auxiliary_mem_usage += stream->byte_size();
}

void RowBatch::add_block(BufferedBlockMgr2::Block* block) {
    DCHECK(block != NULL);
    _blocks.push_back(block);
    _auxiliary_mem_usage += block->buffer_len();
}

void RowBatch::reset() {
    DCHECK(_tuple_data_pool.get() != NULL);
    _num_rows = 0;
    _capacity = _tuple_ptrs_size / (_num_tuples_per_row * sizeof(Tuple*));
    _has_in_flight_row = false;

    // TODO: Change this to Clear() and investigate the repercussions.
    _tuple_data_pool->free_all();
    _agg_object_pool.reset(new ObjectPool());
    for (int i = 0; i < _io_buffers.size(); ++i) {
        _io_buffers[i]->return_buffer();
    }
    _io_buffers.clear();

    for (BufferInfo& buffer_info : _buffers) {
        ExecEnv::GetInstance()->buffer_pool()->FreeBuffer(buffer_info.client, &buffer_info.buffer);
    }
    _buffers.clear();

    close_tuple_streams();
    for (int i = 0; i < _blocks.size(); ++i) {
        _blocks[i]->del();
    }
    _blocks.clear();
    _auxiliary_mem_usage = 0;
    if (!config::enable_partitioned_aggregation) {
        _tuple_ptrs = reinterpret_cast<Tuple**>(_tuple_data_pool->allocate(_tuple_ptrs_size));
    }
    _need_to_return = false;
    _flush = FlushMode::NO_FLUSH_RESOURCES;
    _needs_deep_copy = false;
}

void RowBatch::close_tuple_streams() {
    for (int i = 0; i < _tuple_streams.size(); ++i) {
        _tuple_streams[i]->close();
        delete _tuple_streams[i];
    }
    _tuple_streams.clear();
}

void RowBatch::transfer_resource_ownership(RowBatch* dest) {
    dest->_auxiliary_mem_usage += _tuple_data_pool->total_allocated_bytes();
    dest->_tuple_data_pool->acquire_data(_tuple_data_pool.get(), false);
    dest->_agg_object_pool->acquire_data(_agg_object_pool.get());
    for (int i = 0; i < _io_buffers.size(); ++i) {
        DiskIoMgr::BufferDescriptor* buffer = _io_buffers[i];
        dest->_io_buffers.push_back(buffer);
        dest->_auxiliary_mem_usage += buffer->buffer_len();
        buffer->set_mem_tracker(
                std::shared_ptr<MemTracker>(dest->_mem_tracker)); // TODO(yingchun): fixme
    }
    _io_buffers.clear();

    for (BufferInfo& buffer_info : _buffers) {
        dest->add_buffer(buffer_info.client, std::move(buffer_info.buffer),
                         FlushMode::NO_FLUSH_RESOURCES);
    }
    _buffers.clear();

    for (int i = 0; i < _tuple_streams.size(); ++i) {
        dest->_tuple_streams.push_back(_tuple_streams[i]);
        dest->_auxiliary_mem_usage += _tuple_streams[i]->byte_size();
    }
    // Resource release should be done by dest RowBatch. if we don't clear the corresponding resources.
    // This Rowbatch calls the reset() method, dest Rowbatch will also call the reset() method again,
    // which will cause the core problem of double delete
    _tuple_streams.clear();

    for (int i = 0; i < _blocks.size(); ++i) {
        dest->_blocks.push_back(_blocks[i]);
        dest->_auxiliary_mem_usage += _blocks[i]->buffer_len();
    }
    _blocks.clear();

    dest->_need_to_return |= _need_to_return;

    if (_needs_deep_copy) {
        dest->mark_needs_deep_copy();
    } else if (_flush == FlushMode::FLUSH_RESOURCES) {
        dest->mark_flush_resources();
    }
    reset();
}

int RowBatch::get_batch_size(const TRowBatch& batch) {
    int result = batch.tuple_data.size();
    result += batch.row_tuples.size() * sizeof(TTupleId);
    result += batch.tuple_offsets.size() * sizeof(int32_t);
    return result;
}

int RowBatch::get_batch_size(const PRowBatch& batch) {
    int result = batch.tuple_data().size();
    result += batch.row_tuples().size() * sizeof(int32_t);
    result += batch.tuple_offsets().size() * sizeof(int32_t);
    return result;
}

void RowBatch::acquire_state(RowBatch* src) {
    // DCHECK(_row_desc.equals(src->_row_desc));
    DCHECK_EQ(_num_tuples_per_row, src->_num_tuples_per_row);
    DCHECK_EQ(_tuple_ptrs_size, src->_tuple_ptrs_size);
    DCHECK_EQ(_auxiliary_mem_usage, 0);

    // The destination row batch should be empty.
    DCHECK(!_has_in_flight_row);
    DCHECK_EQ(_num_rows, 0);

    for (int i = 0; i < src->_io_buffers.size(); ++i) {
        DiskIoMgr::BufferDescriptor* buffer = src->_io_buffers[i];
        _io_buffers.push_back(buffer);
        _auxiliary_mem_usage += buffer->buffer_len();
        buffer->set_mem_tracker(std::shared_ptr<MemTracker>(_mem_tracker)); // TODO(yingchun): fixme
    }
    src->_io_buffers.clear();
    src->_auxiliary_mem_usage = 0;

    DCHECK(src->_tuple_streams.empty());
    DCHECK(src->_blocks.empty());

    _has_in_flight_row = src->_has_in_flight_row;
    _num_rows = src->_num_rows;
    _capacity = src->_capacity;
    _need_to_return = src->_need_to_return;
    if (!config::enable_partitioned_aggregation) {
        // Tuple pointers are allocated from tuple_data_pool_ so are transferred.
        _tuple_ptrs = src->_tuple_ptrs;
        src->_tuple_ptrs = NULL;
    } else {
        // tuple_ptrs_ were allocated with malloc so can be swapped between batches.
        std::swap(_tuple_ptrs, src->_tuple_ptrs);
    }
    src->transfer_resource_ownership(this);
}

// TODO: consider computing size of batches as they are built up
int RowBatch::total_byte_size() {
    int result = 0;

    // Sum total variable length byte sizes.
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        const std::vector<TupleDescriptor*>& tuple_descs = _row_desc.tuple_descriptors();
        std::vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();

        for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
            Tuple* tuple = row->get_tuple(j);
            if (tuple == NULL) {
                continue;
            }
            result += (*desc)->byte_size();
            std::vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
            for (; slot != (*desc)->string_slots().end(); ++slot) {
                DCHECK((*slot)->type().is_string_type());
                if (tuple->is_null((*slot)->null_indicator_offset())) {
                    continue;
                }
                StringValue* string_val = tuple->get_string_slot((*slot)->tuple_offset());
                result += string_val->len;
            }
        }
    }

    return result;
}

int RowBatch::max_tuple_buffer_size() {
    int row_size = _row_desc.get_row_size();
    if (row_size > AT_CAPACITY_MEM_USAGE) {
        return row_size;
    }
    int num_rows = 0;
    if (row_size != 0) {
        num_rows = std::min(_capacity, AT_CAPACITY_MEM_USAGE / row_size);
    }
    int tuple_buffer_size = num_rows * row_size;
    DCHECK_LE(tuple_buffer_size, AT_CAPACITY_MEM_USAGE);
    return tuple_buffer_size;
}

void RowBatch::add_buffer(BufferPool::ClientHandle* client, BufferPool::BufferHandle&& buffer,
                          FlushMode flush) {
    _auxiliary_mem_usage += buffer.len();
    BufferInfo buffer_info;
    buffer_info.client = client;
    buffer_info.buffer = std::move(buffer);
    _buffers.push_back(std::move(buffer_info));
    if (flush == FlushMode::FLUSH_RESOURCES) mark_flush_resources();
}

std::string RowBatch::to_string() {
    std::stringstream out;
    for (int i = 0; i < _num_rows; ++i) {
        out << get_row(i)->to_string(_row_desc) << "\n";
    }
    return out.str();
}

} // end namespace doris
