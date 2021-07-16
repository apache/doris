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

#include "vec/core/block.h"

#include <fmt/format.h>

#include <iomanip>
#include <iterator>
#include <memory>

#include "common/status.h"
#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/exception.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

inline DataTypePtr get_data_type(const PColumn& pcolumn) {
    switch (pcolumn.type()) {
    case PColumn::UINT8: {
        return std::make_shared<DataTypeUInt8>();
    }
    case PColumn::UINT16: {
        return std::make_shared<DataTypeUInt16>();
    }
    case PColumn::UINT32: {
        return std::make_shared<DataTypeUInt32>();
    }
    case PColumn::UINT64: {
        return std::make_shared<DataTypeUInt64>();
    }
    case PColumn::UINT128: {
        return std::make_shared<DataTypeUInt128>();
    }
    case PColumn::INT8: {
        return std::make_shared<DataTypeInt8>();
    }
    case PColumn::INT16: {
        return std::make_shared<DataTypeInt16>();
    }
    case PColumn::INT32: {
        return std::make_shared<DataTypeInt32>();
    }
    case PColumn::INT64: {
        return std::make_shared<DataTypeInt64>();
    }
    case PColumn::INT128: {
        return std::make_shared<DataTypeInt128>();
    }
    case PColumn::FLOAT32: {
        return std::make_shared<DataTypeFloat32>();
    }
    case PColumn::FLOAT64: {
        return std::make_shared<DataTypeFloat64>();
    }
    case PColumn::STRING: {
        return std::make_shared<DataTypeString>();
    }
    case PColumn::DATE: {
        return std::make_shared<DataTypeDate>();
    }
    case PColumn::DATETIME: {
        return std::make_shared<DataTypeDateTime>();
    }
    case PColumn::DECIMAL32: {
        return std::make_shared<DataTypeDecimal<Decimal32>>(pcolumn.decimal_param().precision(),
                                                            pcolumn.decimal_param().scale());
    }
    case PColumn::DECIMAL64: {
        return std::make_shared<DataTypeDecimal<Decimal64>>(pcolumn.decimal_param().precision(),
                                                            pcolumn.decimal_param().scale());
    }
    case PColumn::DECIMAL128: {
        return std::make_shared<DataTypeDecimal<Decimal128>>(pcolumn.decimal_param().precision(),
                                                             pcolumn.decimal_param().scale());
    }
    case PColumn::BITMAP: {
        return std::make_shared<DataTypeBitMap>();
    }
    default: {
        LOG(FATAL) << fmt::format("Unknown data type: {}, data type name: {}", pcolumn.type(),
                                  PColumn::DataType_Name(pcolumn.type()));
        return nullptr;
    }
    }
}

PColumn::DataType get_pdata_type(DataTypePtr data_type) {
    switch (data_type->get_type_id()) {
    case TypeIndex::UInt8:
        return PColumn::UINT8;
    case TypeIndex::UInt16:
        return PColumn::UINT16;
    case TypeIndex::UInt32:
        return PColumn::UINT32;
    case TypeIndex::UInt64:
        return PColumn::UINT64;
    case TypeIndex::UInt128:
        return PColumn::UINT128;
    case TypeIndex::Int8:
        return PColumn::INT8;
    case TypeIndex::Int16:
        return PColumn::INT16;
    case TypeIndex::Int32:
        return PColumn::INT32;
    case TypeIndex::Int64:
        return PColumn::INT64;
    case TypeIndex::Int128:
        return PColumn::INT128;
    case TypeIndex::Float32:
        return PColumn::FLOAT32;
    case TypeIndex::Float64:
        return PColumn::FLOAT64;
    case TypeIndex::Decimal32:
        return PColumn::DECIMAL32;
    case TypeIndex::Decimal64:
        return PColumn::DECIMAL64;
    case TypeIndex::Decimal128:
        return PColumn::DECIMAL128;
    case TypeIndex::String:
        return PColumn::STRING;
    case TypeIndex::Date:
        return PColumn::DATE;
    case TypeIndex::DateTime:
        return PColumn::DATETIME;
    case TypeIndex::BitMap:
        return PColumn::BITMAP;
    default:
        return PColumn::UNKNOWN;
    }
}

Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il} {
    initialize_index_by_name();
}

Block::Block(const ColumnsWithTypeAndName& data_) : data{data_} {
    initialize_index_by_name();
}

Block::Block(const PBlock& pblock) {
    for (const auto& pcolumn : pblock.columns()) {
        DataTypePtr type = get_data_type(pcolumn);
        MutableColumnPtr data_column;
        if (pcolumn.is_null_size() > 0) {
            data_column =
                    ColumnNullable::create(std::move(type->create_column()), ColumnUInt8::create());
            type = make_nullable(type);
        } else {
            data_column = type->create_column();
        }
        type->deserialize(pcolumn, data_column.get());
        data.emplace_back(data_column->get_ptr(), type, pcolumn.name());
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

std::string Block::dump_data(size_t row_limit) const {
    if (rows() == 0) {
        return "empty block.";
    }
    std::vector<std::string> headers;
    std::vector<size_t> headers_size;
    for (auto it = data.begin(); it != data.end(); ++it) {
        std::string s = fmt::format("{}({})", it->name, it->type->get_name());
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
    // content
    for (size_t row_num = 0; row_num < rows() && row_num < row_limit; ++row_num) {
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
    info = BlockInfo();
    data.clear();
    index_by_name.clear();
}

void Block::swap(Block& other) noexcept {
    std::swap(info, other.info);
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

void filter_block_internal(Block* block, const IColumn::Filter& filter, int column_to_keep) {
    auto count = count_bytes_in_filter(filter);
    if (count == 0) {
        block->get_by_position(0).column = block->get_by_position(0).column->clone_empty();
    } else {
        if (count != block->rows()) {
            for (size_t i = 0; i < column_to_keep; ++i) {
                block->get_by_position(i).column =
                        block->get_by_position(i).column->filter(filter, 0);
            }
        }
        for (size_t i = column_to_keep; i < block->columns(); ++i) {
            block->erase(i);
        }
    }
}

Status Block::filter_block(Block* block, int filter_column_id, int column_to_keep) {
    ColumnPtr filter_column = block->get_by_position(filter_column_id).column;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        ColumnPtr nested_column = nullable_column->get_nested_column_ptr();

        MutableColumnPtr mutable_holder = (*std::move(nested_column)).mutate();

        ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
        if (!concrete_column) {
            return Status::InvalidArgument(
                    "Illegal type " + filter_column->get_name() +
                    " of column for filter. Must be UInt8 or Nullable(UInt8).");
        }
        const NullMap& null_map = nullable_column->get_null_map_data();
        IColumn::Filter& filter = concrete_column->get_data();

        size_t size = filter.size();
        for (size_t i = 0; i < size; ++i) {
            filter[i] = filter[i] && !null_map[i];
        }
        filter_block_internal(block, filter, column_to_keep);
    } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (ret) {
            for (size_t i = column_to_keep; i < block->columns(); ++i) {
                block->erase(i);
            }
        } else {
            block->get_by_position(0).column = block->get_by_position(0).column->clone_empty();
        }
    } else {
        const IColumn::Filter& filter =
                assert_cast<const doris::vectorized::ColumnVector<UInt8>&>(*filter_column)
                        .get_data();
        filter_block_internal(block, filter, column_to_keep);
    }
    return Status::OK();
}

size_t Block::serialize(PBlock* pblock) const {
    size_t block_size_before_compress = 0;

    for (const auto& c : *this) {
        // name serialize
        PColumn* pc = pblock->add_columns();
        pc->set_name(c.name);
        block_size_before_compress += c.name.size();

        // type serialize
        if (c.type->is_nullable()) {
            pc->set_type(get_pdata_type(
                    std::dynamic_pointer_cast<const DataTypeNullable>(c.type)->get_nested_type()));
        } else {
            pc->set_type(get_pdata_type(c.type));
        }
        // content serialize
        block_size_before_compress += c.type->serialize(*(c.column), pc);
    }

    return block_size_before_compress;
}

size_t MutableBlock::rows() const {
    for (const auto& column : _columns) {
        if (column) {
            return column->size();
        }
    }

    return 0;
}

void MutableBlock::add_row(const Block* block, int row) {
    auto& src_columns_with_schema = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->insert_from(*src_columns_with_schema[i].column.get(), row);
    }
}

Block MutableBlock::to_block() {
    ColumnsWithTypeAndName columns_with_schema;
    for (size_t i = 0; i < _columns.size(); ++i) {
        columns_with_schema.emplace_back(std::move(_columns[i]), _data_types[i], "");
    }
    return {columns_with_schema};
}
std::string MutableBlock::dump_data(size_t row_limit) const {
    if (rows() == 0) {
        return "empty block.";
    }
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
} // namespace doris::vectorized
