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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/row-batch.cc
// and modified by Doris

#include "runtime/row_batch.h"

#include <snappy/snappy.h>
#include <stdint.h> // for intptr_t

#include "common/utils.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/data.pb.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/collection_value.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/thread_context.h"
#include "runtime/tuple_row.h"
#include "util/exception.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"

using std::vector;

namespace doris {

const int RowBatch::AT_CAPACITY_MEM_USAGE = 8 * 1024 * 1024;
const int RowBatch::FIXED_LEN_BUFFER_LIMIT = AT_CAPACITY_MEM_USAGE / 2;

RowBatch::RowBatch(const RowDescriptor& row_desc, int capacity)
        : _has_in_flight_row(false),
          _num_rows(0),
          _num_uncommitted_rows(0),
          _capacity(capacity),
          _flush(FlushMode::NO_FLUSH_RESOURCES),
          _needs_deep_copy(false),
          _num_tuples_per_row(row_desc.tuple_descriptors().size()),
          _row_desc(row_desc),
          _auxiliary_mem_usage(0),
          _need_to_return(false),
          _tuple_data_pool() {
    DCHECK_GT(capacity, 0);
    _tuple_ptrs_size = _capacity * _num_tuples_per_row * sizeof(Tuple*);
    DCHECK_GT(_tuple_ptrs_size, 0);
    _tuple_ptrs = (Tuple**)(malloc(_tuple_ptrs_size));
    DCHECK(_tuple_ptrs != nullptr);
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const PRowBatch& input_batch)
        : _has_in_flight_row(false),
          _num_rows(input_batch.num_rows()),
          _num_uncommitted_rows(0),
          _capacity(_num_rows),
          _flush(FlushMode::NO_FLUSH_RESOURCES),
          _needs_deep_copy(false),
          _num_tuples_per_row(input_batch.row_tuples_size()),
          _row_desc(row_desc),
          _auxiliary_mem_usage(0),
          _need_to_return(false) {
    _tuple_ptrs_size = _num_rows * _num_tuples_per_row * sizeof(Tuple*);
    DCHECK_GT(_tuple_ptrs_size, 0);
    _tuple_ptrs = (Tuple**)(malloc(_tuple_ptrs_size));
    DCHECK(_tuple_ptrs != nullptr);

    char* tuple_data = nullptr;
    if (input_batch.is_compressed()) {
        // Decompress tuple data into data pool
        const char* compressed_data = input_batch.tuple_data().c_str();
        size_t compressed_size = input_batch.tuple_data().size();
        size_t uncompressed_size = 0;
        bool success =
                snappy::GetUncompressedLength(compressed_data, compressed_size, &uncompressed_size);
        DCHECK(success) << "snappy::GetUncompressedLength failed";
        tuple_data = (char*)_tuple_data_pool.allocate(uncompressed_size);
        success = snappy::RawUncompress(compressed_data, compressed_size, tuple_data);
        DCHECK(success) << "snappy::RawUncompress failed";
    } else {
        // Tuple data uncompressed, copy directly into data pool
        tuple_data = (char*)_tuple_data_pool.allocate(input_batch.tuple_data().size());
        memcpy(tuple_data, input_batch.tuple_data().c_str(), input_batch.tuple_data().size());
    }

    // convert input_batch.tuple_offsets into pointers
    int tuple_idx = 0;
    // For historical reasons, the original offset was stored using int32,
    // so that if a rowbatch is larger than 2GB, the passed offset may generate an error due to value overflow.
    // So in the new version, a new_tuple_offsets structure is added to store offsets using int64.
    // Here, to maintain compatibility, both versions of offsets are used, with preference given to new_tuple_offsets.
    // TODO(cmy): in the next version, the original tuple_offsets should be removed.
    if (input_batch.new_tuple_offsets_size() > 0) {
        for (int64_t offset : input_batch.new_tuple_offsets()) {
            if (offset == -1) {
                _tuple_ptrs[tuple_idx++] = nullptr;
            } else {
                _tuple_ptrs[tuple_idx++] = convert_to<Tuple*>(tuple_data + offset);
            }
        }
    } else {
        for (int32_t offset : input_batch.tuple_offsets()) {
            if (offset == -1) {
                _tuple_ptrs[tuple_idx++] = nullptr;
            } else {
                _tuple_ptrs[tuple_idx++] = convert_to<Tuple*>(tuple_data + offset);
            }
        }
    }

    // Check whether we have slots that require offset-to-pointer conversion.
    if (!_row_desc.has_varlen_slots()) {
        return;
    }

    const auto& tuple_descs = _row_desc.tuple_descriptors();

    // For every unique tuple, convert string offsets contained in tuple data into
    // pointers. Tuples were serialized in the order we are deserializing them in,
    // so the first occurrence of a tuple will always have a higher offset than any tuple
    // we already converted.
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        for (size_t j = 0; j < tuple_descs.size(); ++j) {
            auto desc = tuple_descs[j];
            if (desc->string_slots().empty() && desc->collection_slots().empty()) {
                continue;
            }

            Tuple* tuple = row->get_tuple(j);
            if (tuple == nullptr) {
                continue;
            }

            for (auto slot : desc->string_slots()) {
                DCHECK(slot->type().is_string_type());
                if (tuple->is_null(slot->null_indicator_offset())) {
                    continue;
                }

                StringValue* string_val = tuple->get_string_slot(slot->tuple_offset());
                int64_t offset = convert_to<int64_t>(string_val->ptr);
                string_val->ptr = tuple_data + offset;

                // Why we do this mask? Field len of StringValue is changed from int to size_t in
                // Doris 0.11. When upgrading, some bits of len sent from 0.10 is random value,
                // this works fine in version 0.10, however in 0.11 this will lead to an invalid
                // length. So we make the high bits zero here.
                string_val->len &= 0x7FFFFFFFL;
            }

            // copy collection slots
            for (auto slot_collection : desc->collection_slots()) {
                DCHECK(slot_collection->type().is_collection_type());
                if (tuple->is_null(slot_collection->null_indicator_offset())) {
                    continue;
                }

                CollectionValue* array_val =
                        tuple->get_collection_slot(slot_collection->tuple_offset());
                const auto& item_type_desc = slot_collection->type().children[0];
                CollectionValue::deserialize_collection(array_val, tuple_data, item_type_desc);
            }
        }
    }
}

void RowBatch::clear() {
    if (_cleared) {
        return;
    }

    _tuple_data_pool.free_all();
    _agg_object_pool.clear();
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
    DCHECK(_tuple_ptrs != nullptr);
    free(_tuple_ptrs);
    _tuple_ptrs = nullptr;
    _cleared = true;
}

RowBatch::~RowBatch() {
    clear();
}

static inline size_t align_tuple_offset(size_t offset) {
    if (config::rowbatch_align_tuple_offset) {
        return (offset + alignof(std::max_align_t) - 1) & (~(alignof(std::max_align_t) - 1));
    }

    return offset;
}

Status RowBatch::serialize(PRowBatch* output_batch, size_t* uncompressed_size,
                           size_t* compressed_size, bool allow_transfer_large_data) {
    // num_rows
    output_batch->set_num_rows(_num_rows);
    // row_tuples
    _row_desc.to_protobuf(output_batch->mutable_row_tuples());
    // tuple_offsets: must clear before reserve
    // TODO(cmy): the tuple_offsets should be removed after v1.1.0, use new_tuple_offsets instead.
    // keep tuple_offsets here is just for compatibility.
    output_batch->clear_tuple_offsets();
    output_batch->mutable_tuple_offsets()->Reserve(_num_rows * _num_tuples_per_row);
    output_batch->clear_new_tuple_offsets();
    output_batch->mutable_new_tuple_offsets()->Reserve(_num_rows * _num_tuples_per_row);
    // is_compressed
    output_batch->set_is_compressed(false);
    // tuple data
    size_t tuple_byte_size = total_byte_size();
    std::string* mutable_tuple_data = output_batch->mutable_tuple_data();
    mutable_tuple_data->resize(tuple_byte_size);

    // Copy tuple data, including strings, into output_batch (converting string
    // pointers into offsets in the process)
    int64_t offset = 0; // current offset into output_batch->tuple_data
    char* tuple_data = mutable_tuple_data->data();
    const auto& tuple_descs = _row_desc.tuple_descriptors();
    const auto& mutable_tuple_offsets = output_batch->mutable_tuple_offsets();
    const auto& mutable_new_tuple_offsets = output_batch->mutable_new_tuple_offsets();

    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        for (size_t j = 0; j < tuple_descs.size(); ++j) {
            auto desc = tuple_descs[j];
            if (row->get_tuple(j) == nullptr) {
                // NULLs are encoded as -1
                mutable_tuple_offsets->Add(-1);
                mutable_new_tuple_offsets->Add(-1);
                continue;
            }

            int64_t old_offset = offset;
            offset = align_tuple_offset(offset);
            tuple_data += offset - old_offset;

            // Record offset before creating copy (which increments offset and tuple_data)
            mutable_tuple_offsets->Add((int32_t)offset);
            mutable_new_tuple_offsets->Add(offset);
            row->get_tuple(j)->deep_copy(*desc, &tuple_data, &offset, /* convert_ptrs */ true);
            CHECK_GE(offset, 0);
        }
    }
    CHECK_EQ(offset, tuple_byte_size)
            << "offset: " << offset << " vs. tuple_byte_size: " << tuple_byte_size;

    size_t max_compressed_size = snappy::MaxCompressedLength(tuple_byte_size);
    bool can_compress = config::compress_rowbatches && tuple_byte_size > 0;
    if (can_compress) {
        try {
            // Allocation of extra-long contiguous memory may fail, and data compression cannot be used if it fails
            _compression_scratch.resize(max_compressed_size);
        } catch (...) {
            can_compress = false;
            std::exception_ptr p = std::current_exception();
            LOG(WARNING) << "Try to alloc " << max_compressed_size
                         << " bytes for compression scratch failed. "
                         << get_current_exception_type_name(p);
        }
    }
    if (can_compress) {
        // Try compressing tuple_data to _compression_scratch, swap if compressed data is
        // smaller
        size_t compressed_size = 0;
        char* compressed_output = _compression_scratch.data();
        snappy::RawCompress(mutable_tuple_data->data(), tuple_byte_size, compressed_output,
                            &compressed_size);
        if (LIKELY(compressed_size < tuple_byte_size)) {
            _compression_scratch.resize(compressed_size);
            mutable_tuple_data->swap(_compression_scratch);
            output_batch->set_is_compressed(true);
        }

        VLOG_ROW << "uncompressed tuple_byte_size: " << tuple_byte_size
                 << ", compressed size: " << compressed_size;
    }

    // return compressed and uncompressed size
    size_t pb_size = get_batch_size(*output_batch);
    *uncompressed_size = pb_size - mutable_tuple_data->size() + tuple_byte_size;
    *compressed_size = pb_size;
    if (!allow_transfer_large_data && pb_size > std::numeric_limits<int32_t>::max()) {
        // the protobuf has a hard limit of 2GB for serialized data.
        return Status::InternalError(
                "The rowbatch is large than 2GB({}), can not send by Protobuf.", pb_size);
    }
    return Status::OK();
}

// when row from files can't fill into tuple with schema limitation, increase the _num_uncommitted_rows in row batch,
void RowBatch::increase_uncommitted_rows() {
    _num_uncommitted_rows++;
}

void RowBatch::add_io_buffer(DiskIoMgr::BufferDescriptor* buffer) {
    DCHECK(buffer != nullptr);
    _io_buffers.push_back(buffer);
    _auxiliary_mem_usage += buffer->buffer_len();
}

Status RowBatch::resize_and_allocate_tuple_buffer(RuntimeState* state, int64_t* tuple_buffer_size,
                                                  uint8_t** buffer) {
    int64_t row_size = _row_desc.get_row_size();
    // Avoid divide-by-zero. Don't need to modify capacity for empty rows anyway.
    if (row_size != 0) {
        _capacity = std::max(1, std::min<int>(_capacity, FIXED_LEN_BUFFER_LIMIT / row_size));
    }
    *tuple_buffer_size = row_size * _capacity;
    // TODO(dhc): change allocate to try_allocate?
    *buffer = _tuple_data_pool.allocate(*tuple_buffer_size);
    if (*buffer == nullptr) {
        std::stringstream ss;
        ss << "Failed to allocate tuple buffer" << *tuple_buffer_size;
        LOG(WARNING) << ss.str();
        return state->set_mem_limit_exceeded(ss.str());
    }
    return Status::OK();
}

void RowBatch::add_tuple_stream(BufferedTupleStream2* stream) {
    DCHECK(stream != nullptr);
    _tuple_streams.push_back(stream);
    _auxiliary_mem_usage += stream->byte_size();
}

void RowBatch::add_block(BufferedBlockMgr2::Block* block) {
    DCHECK(block != nullptr);
    _blocks.push_back(block);
    _auxiliary_mem_usage += block->buffer_len();
}

void RowBatch::reset() {
    _num_rows = 0;
    _capacity = _tuple_ptrs_size / (_num_tuples_per_row * sizeof(Tuple*));
    _has_in_flight_row = false;

    // TODO: Change this to Clear() and investigate the repercussions.
    _tuple_data_pool.free_all();
    _agg_object_pool.clear();
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
    dest->_auxiliary_mem_usage += _tuple_data_pool.total_allocated_bytes();
    dest->_tuple_data_pool.acquire_data(&_tuple_data_pool, false);
    dest->_agg_object_pool.acquire_data(&_agg_object_pool);
    for (int i = 0; i < _io_buffers.size(); ++i) {
        DiskIoMgr::BufferDescriptor* buffer = _io_buffers[i];
        dest->_io_buffers.push_back(buffer);
        dest->_auxiliary_mem_usage += buffer->buffer_len();
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

vectorized::Block RowBatch::convert_to_vec_block() const {
    std::vector<vectorized::MutableColumnPtr> columns;
    for (const auto tuple_desc : _row_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            columns.emplace_back(slot_desc->get_empty_mutable_column());
        }
    }

    std::vector<SlotDescriptor*> slot_descs;
    std::vector<int> tuple_idx;
    int column_numbers = 0;
    for (int i = 0; i < _row_desc.tuple_descriptors().size(); ++i) {
        auto tuple_desc = _row_desc.tuple_descriptors()[i];
        for (int j = 0; j < tuple_desc->slots().size(); ++j) {
            slot_descs.push_back(tuple_desc->slots()[j]);
            tuple_idx.push_back(i);
        }
        column_numbers += tuple_desc->slots().size();
    }
    for (int i = 0; i < column_numbers; ++i) {
        auto slot_desc = slot_descs[i];
        for (int j = 0; j < _num_rows; ++j) {
            TupleRow* src_row = get_row(j);
            auto tuple = src_row->get_tuple(tuple_idx[i]);
            if (slot_desc->is_nullable() && tuple->is_null(slot_desc->null_indicator_offset())) {
                columns[i]->insert_data(nullptr, 0);
            } else if (slot_desc->type().is_string_type()) {
                auto string_value =
                        static_cast<const StringValue*>(tuple->get_slot(slot_desc->tuple_offset()));
                columns[i]->insert_data(string_value->ptr, string_value->len);
            } else {
                columns[i]->insert_data(
                        static_cast<const char*>(tuple->get_slot(slot_desc->tuple_offset())),
                        slot_desc->slot_size());
            }
        }
    }

    doris::vectorized::ColumnsWithTypeAndName columns_with_type_and_name;
    auto n_columns = 0;
    for (const auto tuple_desc : _row_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            columns_with_type_and_name.emplace_back(columns[n_columns++]->get_ptr(),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name());
        }
    }

    return {columns_with_type_and_name};
}

size_t RowBatch::get_batch_size(const PRowBatch& batch) {
    size_t result = batch.tuple_data().size();
    result += batch.row_tuples().size() * sizeof(int32_t);
    // TODO(cmy): remove batch.tuple_offsets
    result += batch.tuple_offsets().size() * sizeof(int32_t);
    result += batch.new_tuple_offsets().size() * sizeof(int64_t);
    return result;
}

void RowBatch::acquire_state(RowBatch* src) {
    // DCHECK(_row_desc.equals(src->_row_desc));
    DCHECK_EQ(_num_tuples_per_row, src->_num_tuples_per_row);
    // DCHECK_EQ(_tuple_ptrs_size, src->_tuple_ptrs_size);
    DCHECK_EQ(_auxiliary_mem_usage, 0);

    // The destination row batch should be empty.
    DCHECK(!_has_in_flight_row);
    DCHECK_EQ(_num_rows, 0);

    for (int i = 0; i < src->_io_buffers.size(); ++i) {
        DiskIoMgr::BufferDescriptor* buffer = src->_io_buffers[i];
        _io_buffers.push_back(buffer);
        _auxiliary_mem_usage += buffer->buffer_len();
    }
    src->_io_buffers.clear();
    src->_auxiliary_mem_usage = 0;

    DCHECK(src->_tuple_streams.empty());
    DCHECK(src->_blocks.empty());

    _has_in_flight_row = src->_has_in_flight_row;
    _num_rows = src->_num_rows;
    _capacity = src->_capacity;
    _need_to_return = src->_need_to_return;
    // tuple_ptrs_ were allocated with malloc so can be swapped between batches.
    std::swap(_tuple_ptrs, src->_tuple_ptrs);
    src->transfer_resource_ownership(this);
}

void RowBatch::deep_copy_to(RowBatch* dst) {
    DCHECK(dst->_row_desc.equals(_row_desc));
    DCHECK_EQ(dst->_num_rows, 0);
    DCHECK_GE(dst->_capacity, _num_rows);
    dst->add_rows(_num_rows);
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* src_row = get_row(i);
        TupleRow* dst_row = convert_to<TupleRow*>(dst->_tuple_ptrs + i * _num_tuples_per_row);
        src_row->deep_copy(dst_row, _row_desc.tuple_descriptors(), &dst->_tuple_data_pool, false);
    }
    dst->commit_rows(_num_rows);
}

// TODO: consider computing size of batches as they are built up
size_t RowBatch::total_byte_size() const {
    size_t result = 0;

    // Sum total variable length byte sizes.
    for (int i = 0; i < _num_rows; ++i) {
        TupleRow* row = get_row(i);
        const auto& tuple_descs = _row_desc.tuple_descriptors();
        for (size_t j = 0; j < tuple_descs.size(); ++j) {
            auto desc = tuple_descs[j];
            Tuple* tuple = row->get_tuple(j);
            if (tuple == nullptr) {
                continue;
            }
            result = align_tuple_offset(result);
            result += desc->byte_size();

            for (auto slot : desc->string_slots()) {
                DCHECK(slot->type().is_string_type());
                if (tuple->is_null(slot->null_indicator_offset())) {
                    continue;
                }
                StringValue* string_val = tuple->get_string_slot(slot->tuple_offset());
                result += string_val->len;
            }

            // compute slot collection size
            for (auto slot_collection : desc->collection_slots()) {
                DCHECK(slot_collection->type().is_collection_type());
                if (tuple->is_null(slot_collection->null_indicator_offset())) {
                    continue;
                }
                CollectionValue* array_val =
                        tuple->get_collection_slot(slot_collection->tuple_offset());
                const auto& item_type_desc = slot_collection->type().children[0];
                result += array_val->get_byte_size(item_type_desc);
            }
        }
    }

    return result;
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
