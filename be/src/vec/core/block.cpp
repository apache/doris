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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.cpp
// and modified by Doris

#include "vec/core/block.h"

#include <fmt/format.h>
#include <snappy.h>

#include "agent/heartbeat_server.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "udf/udf.h"
#include "util/block_compression.h"
#include "util/exception.h"
#include "util/faststring.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {

Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data {il} {
    initialize_index_by_name();
}

Block::Block(const ColumnsWithTypeAndName& data_) : data {data_} {
    initialize_index_by_name();
}

Block::Block(const std::vector<SlotDescriptor*>& slots, size_t block_size) {
    for (const auto slot_desc : slots) {
        auto column_ptr = slot_desc->get_empty_mutable_column();
        column_ptr->reserve(block_size);
        insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                     slot_desc->col_name()));
    }
}

Block::Block(const PBlock& pblock) {
    CHECK(HeartbeatServer::check_be_exec_version(pblock.be_exec_version()));

    const char* buf = nullptr;
    std::string compression_scratch;
    if (pblock.compressed()) {
        // Decompress
        SCOPED_RAW_TIMER(&_decompress_time_ns);
        const char* compressed_data = pblock.column_values().c_str();
        size_t compressed_size = pblock.column_values().size();
        size_t uncompressed_size = 0;
        if (pblock.has_compression_type() && pblock.has_uncompressed_size()) {
            BlockCompressionCodec* codec;
            get_block_compression_codec(pblock.compression_type(), &codec);
            uncompressed_size = pblock.uncompressed_size();
            compression_scratch.resize(uncompressed_size);
            Slice decompressed_slice(compression_scratch);
            codec->decompress(Slice(compressed_data, compressed_size), &decompressed_slice);
            DCHECK(uncompressed_size == decompressed_slice.size);
        } else {
            bool success = snappy::GetUncompressedLength(compressed_data, compressed_size,
                                                         &uncompressed_size);
            DCHECK(success) << "snappy::GetUncompressedLength failed";
            compression_scratch.resize(uncompressed_size);
            success = snappy::RawUncompress(compressed_data, compressed_size,
                                            compression_scratch.data());
            DCHECK(success) << "snappy::RawUncompress failed";
        }
        _decompressed_bytes = uncompressed_size;
        buf = compression_scratch.data();
    } else {
        buf = pblock.column_values().data();
    }

    for (const auto& pcol_meta : pblock.column_metas()) {
        DataTypePtr type = DataTypeFactory::instance().create_data_type(pcol_meta);
        MutableColumnPtr data_column = type->create_column();
        buf = type->deserialize(buf, data_column.get(), pblock.be_exec_version());
        data.emplace_back(data_column->get_ptr(), type, pcol_meta.name());
    }
    initialize_index_by_name();
}

void Block::initialize_index_by_name() {
    for (size_t i = 0, size = data.size(); i < size; ++i) {
        index_by_name[data[i].name] = i;
    }
}

void Block::insert(size_t position, const ColumnWithTypeAndName& elem) {
    if (position > data.size()) {
        LOG(FATAL) << fmt::format("Position out of bound in Block::insert(), max position = {}",
                                  data.size());
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, elem);
}

void Block::insert(size_t position, ColumnWithTypeAndName&& elem) {
    if (position > data.size()) {
        LOG(FATAL) << fmt::format("Position out of bound in Block::insert(), max position = {}",
                                  data.size());
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, std::move(elem));
}

void Block::insert(const ColumnWithTypeAndName& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName&& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(std::move(elem));
}

void Block::insert_unique(const ColumnWithTypeAndName& elem) {
    if (index_by_name.end() == index_by_name.find(elem.name)) {
        insert(elem);
    }
}

void Block::insert_unique(ColumnWithTypeAndName&& elem) {
    if (index_by_name.end() == index_by_name.find(elem.name)) {
        insert(std::move(elem));
    }
}

void Block::erase(const std::set<size_t>& positions) {
    for (auto it = positions.rbegin(); it != positions.rend(); ++it) {
        erase(*it);
    }
}

void Block::erase(size_t position) {
    if (data.empty()) {
        LOG(FATAL) << "Block is empty";
    }

    if (position >= data.size()) {
        LOG(FATAL) << fmt::format("Position out of bound in Block::erase(), max position = {}",
                                  data.size() - 1);
    }

    erase_impl(position);
}

void Block::erase_impl(size_t position) {
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second == position)
            index_by_name.erase(it++);
        else {
            if (it->second > position) --it->second;
            ++it;
        }
    }
}

void Block::erase(const String& name) {
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end()) {
        LOG(FATAL) << fmt::format("No such name in Block::erase(): '{}'", name);
    }

    erase_impl(index_it->second);
}

ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) {
    if (data.empty()) {
        LOG(FATAL) << "Block is empty";
    }

    if (position >= data.size()) {
        LOG(FATAL) << fmt::format(
                "Position {} is out of bound in Block::safe_get_by_position(), max position = {}, "
                "there are columns: {}",
                position, data.size() - 1, dump_names());
    }

    return data[position];
}

const ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) const {
    if (data.empty()) {
        LOG(FATAL) << "Block is empty";
    }

    if (position >= data.size()) {
        LOG(FATAL) << fmt::format(
                "Position {} is out of bound in Block::safe_get_by_position(), max position = {}, "
                "there are columns: {}",
                position, data.size() - 1, dump_names());
    }

    return data[position];
}

ColumnWithTypeAndName& Block::get_by_name(const std::string& name) {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        LOG(FATAL) << fmt::format("Not found column {} in block. There are only columns: {}", name,
                                  dump_names());
    }

    return data[it->second];
}

const ColumnWithTypeAndName& Block::get_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        LOG(FATAL) << fmt::format("Not found column {} in block. There are only columns: {}", name,
                                  dump_names());
    }

    return data[it->second];
}

ColumnWithTypeAndName* Block::try_get_by_name(const std::string& name) {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        return nullptr;
    }
    return &data[it->second];
}

const ColumnWithTypeAndName* Block::try_get_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        return nullptr;
    }
    return &data[it->second];
}

bool Block::has(const std::string& name) const {
    return index_by_name.end() != index_by_name.find(name);
}

size_t Block::get_position_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        LOG(FATAL) << fmt::format("Not found column {} in block. There are only columns: {}", name,
                                  dump_names());
    }

    return it->second;
}

void Block::check_number_of_rows(bool allow_null_columns) const {
    ssize_t rows = -1;
    for (const auto& elem : data) {
        if (!elem.column && allow_null_columns) continue;

        if (!elem.column) {
            LOG(FATAL) << fmt::format(
                    "Column {} in block is nullptr, in method check_number_of_rows.", elem.name);
        }

        ssize_t size = elem.column->size();

        if (rows == -1) {
            rows = size;
        } else if (rows != size) {
            LOG(FATAL) << fmt::format("Sizes of columns doesn't match: {}:{},{}:{}",
                                      data.front().name, rows, elem.name, size);
        }
    }
}

size_t Block::rows() const {
    for (const auto& elem : data) {
        if (elem.column) {
            return elem.column->size();
        }
    }

    return 0;
}

void Block::set_num_rows(size_t length) {
    if (rows() > length) {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->cut(0, length);
            }
        }
    }
}

void Block::skip_num_rows(int64_t& length) {
    auto origin_rows = rows();
    if (origin_rows <= length) {
        clear();
        length -= origin_rows;
    } else {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->cut(length, origin_rows - length);
            }
        }
    }
}

size_t Block::bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        res += elem.column->byte_size();
    }

    return res;
}

size_t Block::allocated_bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        res += elem.column->allocated_bytes();
    }

    return res;
}

std::string Block::dump_names() const {
    std::stringstream out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) out << ", ";
        out << it->name;
    }
    return out.str();
}

std::string Block::dump_data(size_t begin, size_t row_limit) const {
    std::vector<std::string> headers;
    std::vector<size_t> headers_size;
    for (const auto& it : data) {
        std::string s = fmt::format("{}({})", it.name, it.type->get_name());
        headers_size.push_back(s.size() > 15 ? s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }
    // content
    for (size_t row_num = begin; row_num < rows() && row_num < row_limit + begin; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            std::string s = "";
            if (data[i].column) {
                s = data[i].to_string(row_num);
            }
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::string Block::dump_one_line(size_t row, int column_end) const {
    assert(column_end <= columns());
    fmt::memory_buffer line;
    for (int i = 0; i < column_end; ++i) {
        if (LIKELY(i != 0)) {
            // TODO: need more effective function of to string. now the impl is slow
            fmt::format_to(line, " {}", data[i].to_string(row));
        } else {
            fmt::format_to(line, "{}", data[i].to_string(row));
        }
    }
    return fmt::to_string(line);
}

std::string Block::dump_structure() const {
    // WriteBufferFromOwnString out;
    std::stringstream out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out << ", ";
        }
        out << it->dump_structure();
    }
    return out.str();
}

Block Block::clone_empty() const {
    Block res;
    for (const auto& elem : data) {
        res.insert(elem.clone_empty());
    }
    return res;
}

MutableColumns Block::clone_empty_columns() const {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column ? data[i].column->clone_empty() : data[i].type->create_column();
    }
    return columns;
}

Columns Block::get_columns() const {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column;
    }
    return columns;
}

MutableColumns Block::mutate_columns() {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column ? (*std::move(data[i].column)).mutate()
                                    : data[i].type->create_column();
    }
    return columns;
}

void Block::set_columns(MutableColumns&& columns) {
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = std::move(columns[i]);
    }
}

void Block::set_columns(const Columns& columns) {
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = columns[i];
    }
}

Block Block::clone_with_columns(MutableColumns&& columns) const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        res.insert({std::move(columns[i]), data[i].type, data[i].name});
    }

    return res;
}

Block Block::clone_with_columns(const Columns& columns) const {
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size()) {
        LOG(FATAL) << fmt::format(
                "Cannot clone block with columns because block has {} columns, but {} columns "
                "given.",
                num_columns, columns.size());
    }

    for (size_t i = 0; i < num_columns; ++i) {
        res.insert({columns[i], data[i].type, data[i].name});
    }

    return res;
}

Block Block::clone_without_columns() const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        res.insert({nullptr, data[i].type, data[i].name});
    }

    return res;
}

Block Block::sort_columns() const {
    Block sorted_block;

    for (const auto& name : index_by_name) {
        sorted_block.insert(data[name.second]);
    }

    return sorted_block;
}

const ColumnsWithTypeAndName& Block::get_columns_with_type_and_name() const {
    return data;
}

Names Block::get_names() const {
    Names res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.name);
    }

    return res;
}

DataTypes Block::get_data_types() const {
    DataTypes res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.type);
    }

    return res;
}

void Block::clear() {
    data.clear();
    index_by_name.clear();
}

void Block::clear_column_data(int column_size) noexcept {
    // data.size() greater than column_size, means here have some
    // function exec result in block, need erase it here
    if (column_size != -1 and data.size() > column_size) {
        for (int i = data.size() - 1; i >= column_size; --i) {
            erase(i);
        }
    }
    for (auto& d : data) {
        DCHECK(d.column->use_count() == 1);
        (*std::move(d.column)).assume_mutable()->clear();
    }
}

void Block::swap(Block& other) noexcept {
    data.swap(other.data);
    index_by_name.swap(other.index_by_name);
}

void Block::swap(Block&& other) noexcept {
    clear();
    data = std::move(other.data);
    initialize_index_by_name();
}

void Block::update_hash(SipHash& hash) const {
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no) {
        for (const auto& col : data) {
            col.column->update_hash_with_value(row_no, hash);
        }
    }
}

void Block::filter_block_internal(Block* block, const IColumn::Filter& filter,
                                  uint32_t column_to_keep) {
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        for (size_t i = 0; i < column_to_keep; ++i) {
            std::move(*block->get_by_position(i).column).assume_mutable()->clear();
        }
    } else {
        if (count != block->rows()) {
            for (size_t i = 0; i < column_to_keep; ++i) {
                block->get_by_position(i).column =
                        block->get_by_position(i).column->filter(filter, count);
            }
        }
    }
}

Block Block::copy_block(const std::vector<int>& column_offset) const {
    ColumnsWithTypeAndName columns_with_type_and_name;
    for (auto offset : column_offset) {
        DCHECK(offset < data.size());
        columns_with_type_and_name.emplace_back(data[offset]);
    }
    return columns_with_type_and_name;
}

void Block::append_block_by_selector(MutableColumns& columns,
                                     const IColumn::Selector& selector) const {
    DCHECK(data.size() == columns.size());
    for (size_t i = 0; i < data.size(); i++) {
        data[i].column->append_data_by_selector(columns[i], selector);
    }
}

Status Block::filter_block(Block* block, int filter_column_id, int column_to_keep) {
    ColumnPtr filter_column = block->get_by_position(filter_column_id).column;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        ColumnPtr nested_column = nullable_column->get_nested_column_ptr();

        MutableColumnPtr mutable_holder =
                nested_column->use_count() == 1
                        ? nested_column->assume_mutable()
                        : nested_column->clone_resized(nested_column->size());

        ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
        if (!concrete_column) {
            return Status::InvalidArgument(
                    "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8).",
                    filter_column->get_name());
        }
        auto* __restrict null_map = nullable_column->get_null_map_data().data();
        IColumn::Filter& filter = concrete_column->get_data();
        auto* __restrict filter_data = filter.data();

        const size_t size = filter.size();
        for (size_t i = 0; i < size; ++i) {
            filter_data[i] &= !null_map[i];
        }
        filter_block_internal(block, filter, column_to_keep);
    } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            for (size_t i = 0; i < column_to_keep; ++i) {
                std::move(*block->get_by_position(i).column).assume_mutable()->clear();
            }
        }
    } else {
        const IColumn::Filter& filter =
                assert_cast<const doris::vectorized::ColumnVector<UInt8>&>(*filter_column)
                        .get_data();
        filter_block_internal(block, filter, column_to_keep);
    }

    erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status Block::serialize(PBlock* pblock,
                        /*std::string* compressed_buffer,*/ size_t* uncompressed_bytes,
                        size_t* compressed_bytes, segment_v2::CompressionTypePB compression_type,
                        bool allow_transfer_large_data) const {
    pblock->set_be_exec_version(HeartbeatServer::be_exec_version);

    // calc uncompressed size for allocation
    size_t content_uncompressed_size = 0;
    for (const auto& c : *this) {
        PColumnMeta* pcm = pblock->add_column_metas();
        c.to_pb_column_meta(pcm);
        // get serialized size
        content_uncompressed_size +=
                c.type->get_uncompressed_serialized_bytes(*(c.column), pblock->be_exec_version());
    }

    // serialize data values
    // when data type is HLL, content_uncompressed_size maybe larger than real size.
    std::string column_values;
    try {
        column_values.resize(content_uncompressed_size);
    } catch (...) {
        std::exception_ptr p = std::current_exception();
        std::string msg =
                fmt::format("Try to alloc {} bytes for pblock column values failed. reason {}",
                            content_uncompressed_size, get_current_exception_type_name(p));
        LOG(WARNING) << msg;
        return Status::BufferAllocFailed(msg);
    }
    char* buf = column_values.data();

    for (const auto& c : *this) {
        buf = c.type->serialize(*(c.column), buf, pblock->be_exec_version());
    }
    *uncompressed_bytes = content_uncompressed_size;

    // compress
    if (config::compress_rowbatches && content_uncompressed_size > 0) {
        SCOPED_RAW_TIMER(&_compress_time_ns);
        pblock->set_compression_type(compression_type);
        pblock->set_uncompressed_size(content_uncompressed_size);

        BlockCompressionCodec* codec;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));

        faststring buf_compressed;
        RETURN_IF_ERROR(codec->compress(Slice(column_values.data(), content_uncompressed_size),
                                        &buf_compressed));
        size_t compressed_size = buf_compressed.size();
        if (LIKELY(compressed_size < content_uncompressed_size)) {
            pblock->set_column_values(buf_compressed.data(), buf_compressed.size());
            pblock->set_compressed(true);
            *compressed_bytes = compressed_size;
        } else {
            pblock->set_column_values(std::move(column_values));
            *compressed_bytes = content_uncompressed_size;
        }

        VLOG_ROW << "uncompressed size: " << content_uncompressed_size
                 << ", compressed size: " << compressed_size;
    }
    if (!allow_transfer_large_data && *compressed_bytes >= std::numeric_limits<int32_t>::max()) {
        return Status::InternalError("The block is large than 2GB({}), can not send by Protobuf.",
                                     *compressed_bytes);
    }
    return Status::OK();
}

void Block::serialize(RowBatch* output_batch, const RowDescriptor& row_desc) {
    auto num_rows = rows();
    auto mem_pool = output_batch->tuple_data_pool();

    for (int i = 0; i < num_rows; ++i) {
        auto tuple_row = output_batch->get_row(i);
        const auto& tuple_descs = row_desc.tuple_descriptors();
        auto column_offset = 0;

        for (int j = 0; j < tuple_descs.size(); ++j) {
            auto tuple_desc = tuple_descs[j];
            tuple_row->set_tuple(j, deep_copy_tuple(*tuple_desc, mem_pool, i, column_offset));
            column_offset += tuple_desc->slots().size();
        }
        output_batch->commit_last_row();
    }
}

doris::Tuple* Block::deep_copy_tuple(const doris::TupleDescriptor& desc, MemPool* pool, int row,
                                     int column_offset, bool padding_char) {
    auto dst = reinterpret_cast<doris::Tuple*>(pool->allocate(desc.byte_size()));

    for (int i = 0; i < desc.slots().size(); ++i) {
        auto slot_desc = desc.slots()[i];
        auto& type_desc = slot_desc->type();
        const auto& column = get_by_position(column_offset + i).column;
        const auto& data_ref =
                type_desc.type != TYPE_ARRAY ? column->get_data_at(row) : StringRef();
        bool is_null = is_column_data_null(slot_desc->type(), data_ref, column, row);
        if (is_null) {
            dst->set_null(slot_desc->null_indicator_offset());
        } else {
            dst->set_not_null(slot_desc->null_indicator_offset());
            deep_copy_slot(dst->get_slot(slot_desc->tuple_offset()), pool, type_desc, data_ref,
                           column.get(), row, padding_char);
        }
    }
    return dst;
}

inline bool Block::is_column_data_null(const doris::TypeDescriptor& type_desc,
                                       const StringRef& data_ref, const IColumn* column, int row) {
    if (type_desc.type != TYPE_ARRAY) {
        return data_ref.data == nullptr;
    } else {
        Field array;
        column->get(row, array);
        return array.is_null();
    }
}

// TODO: need to refactor this function, too long.
void Block::deep_copy_slot(void* dst, MemPool* pool, const doris::TypeDescriptor& type_desc,
                           const StringRef& data_ref, const IColumn* column, int row,
                           bool padding_char) {
    if (type_desc.is_collection_type()) {
        if (type_desc.type != TYPE_ARRAY) {
            return;
        }

        Field field;
        column->get(row, field);
        const auto& array = field.get<Array>();
        auto collection_value = reinterpret_cast<CollectionValue*>(dst);
        auto item_type_desc = type_desc.children.front();
        CollectionValue::init_collection(pool, array.size(), item_type_desc.type, collection_value);

        const ColumnArray* array_column = nullptr;
        if (is_column_nullable(*column)) {
            auto& nested_column =
                    reinterpret_cast<const ColumnNullable*>(column)->get_nested_column();
            array_column = reinterpret_cast<const ColumnArray*>(&nested_column);
        } else {
            array_column = reinterpret_cast<const ColumnArray*>(column);
        }
        auto item_column = array_column->get_data_ptr().get();
        auto offset = array_column->get_offsets()[row - 1];
        auto iterator = collection_value->iterator(item_type_desc.type);
        for (int i = 0; i < collection_value->length(); ++i) {
            if (array[i].is_null()) {
                const auto& null_value = doris_udf::AnyVal(true);
                iterator.set(&null_value);
            } else {
                auto item_offset = offset + i;
                const auto& data_ref = item_type_desc.type != TYPE_ARRAY
                                               ? item_column->get_data_at(item_offset)
                                               : StringRef();
                if (item_type_desc.is_date_type()) {
                    // In CollectionValue, date type data is stored as either uint24_t or uint64_t.
                    DateTimeValue datetime_value;
                    deep_copy_slot(&datetime_value, pool, item_type_desc, data_ref, item_column,
                                   item_offset, padding_char);
                    DateTimeVal datetime_val;
                    datetime_value.to_datetime_val(&datetime_val);
                    iterator.set(&datetime_val);
                } else if (item_type_desc.is_decimal_type()) {
                    // In CollectionValue, decimal type data is stored as decimal12_t.
                    DecimalV2Value decimal_value;
                    deep_copy_slot(&decimal_value, pool, item_type_desc, data_ref, item_column,
                                   item_offset, padding_char);
                    DecimalV2Val decimal_val;
                    decimal_value.to_decimal_val(&decimal_val);
                    iterator.set(&decimal_val);
                } else {
                    deep_copy_slot(iterator.get(), pool, item_type_desc, data_ref, item_column,
                                   item_offset, padding_char);
                }
            }
            iterator.next();
        }
    } else if (type_desc.is_date_type()) {
        VecDateTimeValue ts =
                *reinterpret_cast<const doris::vectorized::VecDateTimeValue*>(data_ref.data);
        DateTimeValue dt;
        ts.convert_vec_dt_to_dt(&dt);
        memcpy(dst, &dt, sizeof(DateTimeValue));
    } else if (type_desc.type == TYPE_OBJECT) {
        auto bitmap_value = (BitmapValue*)(data_ref.data);
        auto size = bitmap_value->getSizeInBytes();

        // serialize the content of string
        auto string_slot = reinterpret_cast<StringValue*>(dst);
        string_slot->ptr = reinterpret_cast<char*>(pool->allocate(size));
        bitmap_value->write(string_slot->ptr);
        string_slot->len = size;
    } else if (type_desc.type == TYPE_HLL) {
        auto hll_value = (HyperLogLog*)(data_ref.data);
        auto size = hll_value->max_serialized_size();
        auto string_slot = reinterpret_cast<StringValue*>(dst);
        string_slot->ptr = reinterpret_cast<char*>(pool->allocate(size));
        size_t actual_size = hll_value->serialize((uint8_t*)string_slot->ptr);
        string_slot->len = actual_size;
    } else if (type_desc.is_string_type()) { // TYPE_OBJECT and TYPE_HLL must be handled before.
        memcpy(dst, (const void*)(&data_ref), sizeof(data_ref));
        // Copy the content of string
        if (padding_char && type_desc.type == TYPE_CHAR) {
            // serialize the content of string
            auto string_slot = reinterpret_cast<StringValue*>(dst);
            string_slot->ptr = reinterpret_cast<char*>(pool->allocate(type_desc.len));
            string_slot->len = type_desc.len;
            memset(string_slot->ptr, 0, type_desc.len);
            memcpy(string_slot->ptr, data_ref.data, data_ref.size);
        } else {
            auto str_ptr = pool->allocate(data_ref.size);
            memcpy(str_ptr, data_ref.data, data_ref.size);
            auto string_slot = reinterpret_cast<StringValue*>(dst);
            string_slot->ptr = reinterpret_cast<char*>(str_ptr);
            string_slot->len = data_ref.size;
        }
    } else {
        memcpy(dst, data_ref.data, data_ref.size);
    }
}

MutableBlock::MutableBlock(const std::vector<TupleDescriptor*>& tuple_descs, int reserve_size) {
    for (auto tuple_desc : tuple_descs) {
        for (auto slot_desc : tuple_desc->slots()) {
            _data_types.emplace_back(slot_desc->get_data_type_ptr());
            _columns.emplace_back(_data_types.back()->create_column());
            if (reserve_size != 0) {
                _columns.back()->reserve(reserve_size);
            }
        }
    }
}

size_t MutableBlock::rows() const {
    for (const auto& column : _columns) {
        if (column) {
            return column->size();
        }
    }

    return 0;
}

void MutableBlock::swap(MutableBlock& another) noexcept {
    _columns.swap(another._columns);
    _data_types.swap(another._data_types);
}

void MutableBlock::swap(MutableBlock&& another) noexcept {
    clear();
    _columns = std::move(another._columns);
    _data_types = std::move(another._data_types);
}

void MutableBlock::add_row(const Block* block, int row) {
    auto& block_data = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->insert_from(*block_data[i].column.get(), row);
    }
}

void MutableBlock::add_rows(const Block* block, const int* row_begin, const int* row_end) {
    auto& block_data = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        auto& dst = _columns[i];
        auto& src = *block_data[i].column.get();
        dst->insert_indices_from(src, row_begin, row_end);
    }
}

void MutableBlock::add_rows(const Block* block, size_t row_begin, size_t length) {
    auto& block_data = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        auto& dst = _columns[i];
        auto& src = *block_data[i].column.get();
        dst->insert_range_from(src, row_begin, length);
    }
}

Block MutableBlock::to_block(int start_column) {
    return to_block(start_column, _columns.size());
}

Block MutableBlock::to_block(int start_column, int end_column) {
    ColumnsWithTypeAndName columns_with_schema;
    for (size_t i = start_column; i < end_column; ++i) {
        columns_with_schema.emplace_back(std::move(_columns[i]), _data_types[i], "");
    }
    return {columns_with_schema};
}

std::string MutableBlock::dump_data(size_t row_limit) const {
    std::vector<std::string> headers;
    std::vector<size_t> headers_size;
    for (size_t i = 0; i < columns(); ++i) {
        std::string s = _data_types[i]->get_name();
        headers_size.push_back(s.size() > 15 ? s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }
    // content
    for (size_t row_num = 0; row_num < rows() && row_num < row_limit; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            std::string s = _data_types[i]->to_string(*_columns[i].get(), row_num);
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::unique_ptr<Block> Block::create_same_struct_block(size_t size) const {
    auto temp_block = std::make_unique<Block>();
    for (const auto& d : data) {
        auto column = d.type->create_column();
        column->resize(size);
        temp_block->insert({std::move(column), d.type, d.name});
    }
    return temp_block;
}

void Block::shrink_char_type_column_suffix_zero(const std::vector<size_t>& char_type_idx) {
    for (auto idx : char_type_idx) {
        if (idx < data.size()) {
            auto& col_and_name = this->get_by_position(idx);
            col_and_name.column = col_and_name.column->assume_mutable()->get_shrinked_column();
        }
    }
}

size_t MutableBlock::allocated_bytes() const {
    size_t res = 0;
    for (const auto& col : _columns) {
        res += col->allocated_bytes();
    }

    return res;
}

void MutableBlock::clear_column_data() noexcept {
    for (auto& col : _columns) {
        if (col) {
            col->clear();
        }
    }
}

} // namespace doris::vectorized
