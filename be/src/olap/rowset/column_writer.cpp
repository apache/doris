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

#include "olap/rowset/column_writer.h"

#include "olap/file_helper.h"
#include "olap/rowset/bit_field_writer.h"

namespace doris {

ColumnWriter* ColumnWriter::create(uint32_t column_id, const TabletSchema& schema,
                                   OutStreamFactory* stream_factory, size_t num_rows_per_row_block,
                                   double bf_fpp) {
    ColumnWriter* column_writer = nullptr;
    const TabletColumn& column = schema.column(column_id);

    switch (column.type()) {
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT: {
        column_writer = new (std::nothrow)
                ByteColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<int16_t, true>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<uint16_t, false>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<int32_t, true>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_INT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<uint32_t, false>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<int64_t, true>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT: {
        column_writer = new (std::nothrow) IntegerColumnWriterWrapper<uint64_t, false>(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        column_writer = new (std::nothrow) FloatColumnWriter(column_id, stream_factory, column,
                                                             num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        column_writer = new (std::nothrow) DoubleColumnWriter(column_id, stream_factory, column,
                                                              num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE: {
        column_writer = new (std::nothrow) DiscreteDoubleColumnWriter(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_CHAR: {
        column_writer = new (std::nothrow) FixLengthStringColumnWriter(
                column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        column_writer = new (std::nothrow) DateTimeColumnWriter(column_id, stream_factory, column,
                                                                num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        column_writer = new (std::nothrow)
                DateColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        column_writer = new (std::nothrow) DecimalColumnWriter(column_id, stream_factory, column,
                                                               num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        column_writer = new (std::nothrow) LargeIntColumnWriter(column_id, stream_factory, column,
                                                                num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_STRING: {
        column_writer = new (std::nothrow) VarStringColumnWriter(column_id, stream_factory, column,
                                                                 num_rows_per_row_block, bf_fpp);
        break;
    }
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    default: {
        LOG(WARNING) << "Unsupported field type. field=" << column.name()
                     << ", type=" << column.type();
        break;
    }
    }

    return column_writer;
}

ColumnWriter::ColumnWriter(uint32_t column_id, OutStreamFactory* stream_factory,
                           const TabletColumn& column, size_t num_rows_per_row_block, double bf_fpp)
        : _column_id(column_id),
          _column(column),
          _stream_factory(stream_factory),
          _index(column.type()),
          _is_present(nullptr),
          _is_present_stream(nullptr),
          _index_stream(nullptr),
          _is_found_nulls(false),
          _bf(nullptr),
          _num_rows_per_row_block(num_rows_per_row_block),
          _bf_fpp(bf_fpp) {}

ColumnWriter::~ColumnWriter() {
    SAFE_DELETE(_is_present);
    SAFE_DELETE(_bf);

    for (std::vector<ColumnWriter*>::iterator it = _sub_writers.begin(); it != _sub_writers.end();
         ++it) {
        SAFE_DELETE(*it);
    }
}

OLAPStatus ColumnWriter::init() {
    if (_column.is_nullable()) {
        _is_present_stream =
                _stream_factory->create_stream(unique_column_id(), StreamInfoMessage::PRESENT);

        if (nullptr == _is_present_stream) {
            OLAP_LOG_WARNING("fail to allocate IS PRESENT STREAM");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _is_present = new (std::nothrow) BitFieldWriter(_is_present_stream);

        if (nullptr == _is_present) {
            OLAP_LOG_WARNING("fail to allocate IS PRESENT Writer");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (OLAP_SUCCESS != _is_present->init()) {
            OLAP_LOG_WARNING("fail to init IS PRESENT Writer");
            return OLAP_ERR_INIT_FAILED;
        }
    }

    OLAPStatus res = _block_statistics.init(_column.type(), true);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("init block statistic failed");
        return res;
    }

    res = _segment_statistics.init(_column.type(), true);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("init segment statistic failed");
        return res;
    }

    _index_stream =
            _stream_factory->create_stream(unique_column_id(), StreamInfoMessage::ROW_INDEX);

    if (nullptr == _index_stream) {
        OLAP_LOG_WARNING("fail to allocate Index STREAM");
        return OLAP_ERR_MALLOC_ERROR;
    }

    // bloom filter index
    if (is_bf_column()) {
        _bf_index_stream =
                _stream_factory->create_stream(unique_column_id(), StreamInfoMessage::BLOOM_FILTER);
        if (nullptr == _bf_index_stream) {
            OLAP_LOG_WARNING("fail to allocate bloom filter index stream");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _bf = new (std::nothrow) BloomFilter();
        if (nullptr == _bf) {
            OLAP_LOG_WARNING("fail to allocate bloom filter");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (!_bf->init(_num_rows_per_row_block, _bf_fpp)) {
            OLAP_LOG_WARNING("fail to init bloom filter. num rows: %u, fpp: %g",
                             _num_rows_per_row_block, _bf_fpp);
            return OLAP_ERR_INIT_FAILED;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnWriter::write(RowCursor* row_cursor) {
    OLAPStatus res = OLAP_SUCCESS;

    bool is_null = row_cursor->is_null(_column_id);
    char* buf = row_cursor->cell_ptr(_column_id);
    if (_is_present) {
        res = _is_present->write(is_null);

        if (is_null) {
            _is_found_nulls = true;
        }
    }

    if (is_bf_column()) {
        if (!is_null) {
            if (_column.type() == OLAP_FIELD_TYPE_CHAR ||
                _column.type() == OLAP_FIELD_TYPE_VARCHAR ||
                _column.type() == OLAP_FIELD_TYPE_HLL || _column.type() == OLAP_FIELD_TYPE_STRING) {
                Slice* slice = reinterpret_cast<Slice*>(buf);
                _bf->add_bytes(slice->data, slice->size);
            } else {
                _bf->add_bytes(buf, row_cursor->column_size(_column_id));
            }
        } else {
            _bf->add_bytes(nullptr, 0);
        }
    }

    return res;
}

OLAPStatus ColumnWriter::flush() {
    return _is_present->flush();
}

OLAPStatus ColumnWriter::create_row_index_entry() {
    OLAPStatus res = OLAP_SUCCESS;
    segment_statistics()->merge(&_block_statistics);
    _index_entry.set_statistic(&_block_statistics);
    _index.add_index_entry(_index_entry);
    _index_entry.reset_write_offset();
    _block_statistics.reset();
    record_position();

    if (is_bf_column()) {
        _bf_index.add_bloom_filter(_bf);

        _bf = new (std::nothrow) BloomFilter();
        if (nullptr == _bf) {
            OLAP_LOG_WARNING("fail to allocate bloom filter");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (!_bf->init(_num_rows_per_row_block, _bf_fpp)) {
            OLAP_LOG_WARNING("fail to init bloom filter. num rows: %u, fpp: %g",
                             _num_rows_per_row_block, _bf_fpp);
            return OLAP_ERR_INIT_FAILED;
        }
    }

    for (std::vector<ColumnWriter*>::iterator it = _sub_writers.begin(); it != _sub_writers.end();
         ++it) {
        if (OLAP_SUCCESS != (res = (*it)->create_row_index_entry())) {
            OLAP_LOG_WARNING("fail to create sub column's index.");
            return res;
        }
    }

    return res;
}

uint64_t ColumnWriter::estimate_buffered_memory() {
    uint64_t result = 0;

    // bloom filter
    if (is_bf_column()) {
        result += _bf_index.estimate_buffered_memory();
    }

    for (std::vector<ColumnWriter*>::iterator it = _sub_writers.begin(); it != _sub_writers.end();
         ++it) {
        result += (*it)->estimate_buffered_memory();
    }

    return result;
}

// Delete the positions used by is_present_stream:
// * OutStream uses 2
// * ByteRunLength uses 1
// * BitRunLength uses 1
// Delete 4 in total
void ColumnWriter::_remove_is_present_positions() {
    for (uint32_t i = 0; i < _index.entry_size(); i++) {
        PositionEntryWriter* entry = _index.mutable_entry(i);
        entry->remove_written_position(0, 4);
    }
}

OLAPStatus ColumnWriter::finalize(ColumnDataHeaderMessage* header) {
    OLAPStatus res = OLAP_SUCCESS;

    if (nullptr != _is_present) {
        if (OLAP_SUCCESS != (res = _is_present->flush())) {
            return res;
        }

        if (!_is_found_nulls) {
            _is_present_stream->suppress();
            _remove_is_present_positions();
        }
    }

    char* index_buf = nullptr;
    // char* index_statistic_buf = NULL;
    // Write index pb
    size_t pb_size = _index.output_size();
    index_buf = new (std::nothrow) char[pb_size];
    ColumnMessage* column = nullptr;

    if (OLAP_SUCCESS != _index.write_to_buffer(index_buf, pb_size)) {
        OLAP_LOG_WARNING("fail to serialize index");
        res = OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
        goto FINALIZE_EXIT;
    }

    res = _index_stream->write(index_buf, pb_size);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to write index to stream");
        goto FINALIZE_EXIT;
    }

    res = _index_stream->flush();

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush index stream");
        goto FINALIZE_EXIT;
    }

    // write bloom filter index
    if (is_bf_column()) {
        res = _bf_index.write_to_buffer(_bf_index_stream);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to write bloom filter stream");
            OLAP_GOTO(FINALIZE_EXIT);
        }

        res = _bf_index_stream->flush();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to flush bloom filter stream");
            OLAP_GOTO(FINALIZE_EXIT);
        }
    }

    // Record a Schema information in the Segment header
    // This makes it not affect the reading of the data in the existing segment after modifying the schema of the table
    column = header->add_column();
    column->set_name(_column.name());
    column->set_type(TabletColumn::get_string_by_field_type(_column.type()));
    column->set_aggregation(TabletColumn::get_string_by_aggregation_type(_column.aggregation()));
    column->set_length(_column.length());
    column->set_is_key(_column.is_key());
    column->set_precision(_column.precision());
    column->set_frac(_column.frac());
    column->set_unique_id(_column.unique_id());
    column->set_is_bf_column(is_bf_column());

    save_encoding(header->add_column_encoding());

FINALIZE_EXIT:
    SAFE_DELETE_ARRAY(index_buf);
    // SAFE_DELETE_ARRAY(index_statistic_buf);
    return res;
}

void ColumnWriter::record_position() {
    if (nullptr != _is_present) {
        _is_present->get_position(&_index_entry);
    }
}

// The default returns DIRECT, String type may return Dict
void ColumnWriter::save_encoding(ColumnEncodingMessage* encoding) {
    encoding->set_kind(ColumnEncodingMessage::DIRECT);
}

void ColumnWriter::get_bloom_filter_info(bool* has_bf_column, uint32_t* bf_hash_function_num,
                                         uint32_t* bf_bit_num) {
    if (is_bf_column()) {
        *has_bf_column = true;
        *bf_hash_function_num = _bf->hash_function_num();
        *bf_bit_num = _bf->bit_num();
        return;
    }

    for (std::vector<ColumnWriter*>::iterator it = _sub_writers.begin(); it != _sub_writers.end();
         ++it) {
        (*it)->get_bloom_filter_info(has_bf_column, bf_hash_function_num, bf_bit_num);
        if (*has_bf_column) {
            return;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
ByteColumnWriter::ByteColumnWriter(uint32_t column_id, OutStreamFactory* stream_factory,
                                   const TabletColumn& column, size_t num_rows_per_row_block,
                                   double bf_fpp)
        : ColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp),
          _writer(nullptr) {}

ByteColumnWriter::~ByteColumnWriter() {
    SAFE_DELETE(_writer);
}

OLAPStatus ByteColumnWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_SUCCESS != (res = ColumnWriter::init())) {
        return res;
    }

    OutStreamFactory* factory = stream_factory();
    OutStream* stream = factory->create_stream(unique_column_id(), StreamInfoMessage::DATA);

    if (nullptr == stream) {
        OLAP_LOG_WARNING("fail to allocate DATA STREAM");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _writer = new (std::nothrow) RunLengthByteWriter(stream);

    if (nullptr == _writer) {
        OLAP_LOG_WARNING("fail to allocate RunLengthByteWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    record_position();
    return OLAP_SUCCESS;
}

OLAPStatus ByteColumnWriter::finalize(ColumnDataHeaderMessage* header) {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_SUCCESS != (res = ColumnWriter::finalize(header))) {
        OLAP_LOG_WARNING("fail to finalize ColumnWriter.");
        return res;
    }

    if (OLAP_SUCCESS != (res = _writer->flush())) {
        OLAP_LOG_WARNING("fail to flush.");
        return res;
    }

    return OLAP_SUCCESS;
}

void ByteColumnWriter::record_position() {
    ColumnWriter::record_position();
    _writer->get_position(index_entry());
}

////////////////////////////////////////////////////////////////////////////////

IntegerColumnWriter::IntegerColumnWriter(uint32_t column_id, uint32_t unique_column_id,
                                         OutStreamFactory* stream_factory, bool is_singed)
        : _column_id(column_id),
          _unique_column_id(unique_column_id),
          _stream_factory(stream_factory),
          _writer(nullptr),
          _is_signed(is_singed) {}

IntegerColumnWriter::~IntegerColumnWriter() {
    SAFE_DELETE(_writer);
}

OLAPStatus IntegerColumnWriter::init() {
    OutStream* stream = _stream_factory->create_stream(_unique_column_id, StreamInfoMessage::DATA);

    if (nullptr == stream) {
        OLAP_LOG_WARNING("fail to allocate DATA STREAM");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _writer = new (std::nothrow) RunLengthIntegerWriter(stream, _is_signed);

    if (nullptr == _writer) {
        OLAP_LOG_WARNING("fail to allocate RunLengthIntegerWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

VarStringColumnWriter::VarStringColumnWriter(uint32_t column_id, OutStreamFactory* stream_factory,
                                             const TabletColumn& column,
                                             size_t num_rows_per_row_block, double bf_fpp)
        : ColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp),
          _use_dictionary_encoding(false),
          _dict_total_size(0),
          _dict_stream(nullptr),
          _length_writer(nullptr),
          _data_stream(nullptr),
          _id_writer(nullptr) {}

VarStringColumnWriter::~VarStringColumnWriter() {
    SAFE_DELETE(_length_writer);
    SAFE_DELETE(_id_writer);
}

OLAPStatus VarStringColumnWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_SUCCESS != (res = ColumnWriter::init())) {
        return res;
    }

    _dict_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::DICTIONARY_DATA);
    _data_stream = stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::DATA);
    OutStream* length_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::LENGTH);

    if (nullptr == _dict_stream || nullptr == length_stream || nullptr == _data_stream) {
        OLAP_LOG_WARNING("fail to create stream.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _length_writer = new (std::nothrow) RunLengthIntegerWriter(length_stream, false);
    _id_writer = new (std::nothrow) RunLengthIntegerWriter(_data_stream, false);

    if (nullptr == _length_writer || nullptr == _id_writer) {
        OLAP_LOG_WARNING("fail to create writer.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    record_position();
    return OLAP_SUCCESS;
}

OLAPStatus VarStringColumnWriter::write(const char* str, uint32_t len) {
    OLAPStatus res = OLAP_SUCCESS;
    // zdb shield the dictionary coding
    //std::string key(str, len);

    if (OLAP_SUCCESS != (res = _data_stream->write(str, len))) {
        OLAP_LOG_WARNING("fail to write string content.");
        return res;
    }

    if (OLAP_SUCCESS != (res = _length_writer->write(len))) {
        OLAP_LOG_WARNING("fail to write string length.");
        return res;
    }

    return OLAP_SUCCESS;
}

uint64_t VarStringColumnWriter::estimate_buffered_memory() {
    // the length of _string_id is short after RLE
    return _dict_total_size;
}

OLAPStatus VarStringColumnWriter::_finalize_dict_encoding() {
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<uint32_t> dump_order;
    uint32_t current_id = 0;

    dump_order.resize(_string_keys.size());

    for (StringDict::iterator it = _string_dict.begin(); it != _string_dict.end(); ++it) {
        dump_order[it->second] = current_id;
        current_id++;
        const std::string& key = it->first.get();
        res = _dict_stream->write(key.c_str(), key.length());

        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to write string dict to stream.");
            return res;
        }

        if (OLAP_SUCCESS != (res = _length_writer->write(key.length()))) {
            OLAP_LOG_WARNING("fail to write string length to stream.");
            return res;
        }
    }

    uint32_t block_id = 0;

    // Suppose there are n ids in total. (total)
    for (uint32_t i = 0; i <= _string_id.size(); i++) {
        while (block_id < _block_row_count.size() - 1 && i == _block_row_count[block_id]) {
            _id_writer->get_position(index()->mutable_entry(block_id), false);
            block_id++;
        }

        if (i != _string_id.size()) {
            res = _id_writer->write(dump_order[_string_id[i]]);

            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to write string id to stream.");
                return res;
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus VarStringColumnWriter::_finalize_direct_encoding() {
    //OLAPStatus res = OLAP_SUCCESS;
    //uint32_t block_id = 0;

    _dict_stream->suppress();
    return OLAP_SUCCESS;
}

OLAPStatus VarStringColumnWriter::finalize(ColumnDataHeaderMessage* header) {
    OLAPStatus res = OLAP_SUCCESS;
    uint64_t ratio_threshold = config::column_dictionary_key_ratio_threshold;
    uint64_t size_threshold = config::column_dictionary_key_size_threshold;

    // the dictionary condition:1 key size < size threshold; 2 key ratio < ratio threshold
    _use_dictionary_encoding = (_string_keys.size() < size_threshold) &&
                               (_string_keys.size() * 100UL < _string_id.size() * ratio_threshold);

    if (_use_dictionary_encoding) {
        res = _finalize_dict_encoding();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to finalize dict encoding.");
            return res;
        }
    } else {
        res = _finalize_direct_encoding();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to finalize direct encoding.");
            return res;
        }
    }

    // The index's supplementary writing has been completed, ColumnWriter::finalize will write the header
    res = ColumnWriter::finalize(header);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize ColumnWriter.");
        return res;
    }

    // id_writer is practical to data_stream, it doesn't matter if you repeat flush
    if (OLAP_SUCCESS != _length_writer->flush() || OLAP_SUCCESS != _id_writer->flush() ||
        OLAP_SUCCESS != _dict_stream->flush() || OLAP_SUCCESS != _data_stream->flush()) {
        OLAP_LOG_WARNING("fail to flush stream.");
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    _string_keys.clear();
    _string_dict.clear();
    _string_id.clear();
    _block_row_count.clear();
    _dict_total_size = 0;
    return OLAP_SUCCESS;
}

void VarStringColumnWriter::save_encoding(ColumnEncodingMessage* encoding) {
    if (_use_dictionary_encoding) {
        encoding->set_kind(ColumnEncodingMessage::DICTIONARY);
        encoding->set_dictionary_size(_string_keys.size());
    } else {
        encoding->set_kind(ColumnEncodingMessage::DIRECT);
    }
}

// Unlike other Writer, data is written to Stream only when it is finalized.
// So it is impossible to record the position of the stream. For this reason, record the number of data written in each block, and when finalize
// Use this information to add stream location information to Index
void VarStringColumnWriter::record_position() {
    ColumnWriter::record_position();
    _block_row_count.push_back(_string_id.size());
    //zdb shield dictionary coding
    _data_stream->get_position(index_entry());
    _length_writer->get_position(index_entry(), false);
}

////////////////////////////////////////////////////////////////////////////////

FixLengthStringColumnWriter::FixLengthStringColumnWriter(uint32_t column_id,
                                                         OutStreamFactory* stream_factory,
                                                         const TabletColumn& column,
                                                         size_t num_rows_per_row_block,
                                                         double bf_fpp)
        : VarStringColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp),
          _length(column.length()) {}

FixLengthStringColumnWriter::~FixLengthStringColumnWriter() {}

////////////////////////////////////////////////////////////////////////////////

DecimalColumnWriter::DecimalColumnWriter(uint32_t column_id, OutStreamFactory* stream_factory,
                                         const TabletColumn& column, size_t num_rows_per_row_block,
                                         double bf_fpp)
        : ColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp),
          _int_writer(nullptr),
          _frac_writer(nullptr) {}

DecimalColumnWriter::~DecimalColumnWriter() {
    SAFE_DELETE(_int_writer);
    SAFE_DELETE(_frac_writer);
}

OLAPStatus DecimalColumnWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    res = ColumnWriter::init();
    if (OLAP_SUCCESS != res) {
        return res;
    }

    OutStream* int_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::DATA);
    OutStream* frac_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::SECONDARY);

    if (nullptr == int_stream || nullptr == frac_stream) {
        OLAP_LOG_WARNING("fail to create stream.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _int_writer = new (std::nothrow) RunLengthIntegerWriter(int_stream, true);
    _frac_writer = new (std::nothrow) RunLengthIntegerWriter(frac_stream, true);

    if (nullptr == _int_writer || nullptr == _frac_writer) {
        OLAP_LOG_WARNING("fail to create writer.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    record_position();
    return OLAP_SUCCESS;
}

OLAPStatus DecimalColumnWriter::finalize(ColumnDataHeaderMessage* header) {
    OLAPStatus res;

    res = ColumnWriter::finalize(header);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize ColumnWriter.");
        return res;
    }

    res = _int_writer->flush();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush integer writer.");
        return res;
    }

    res = _frac_writer->flush();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush fraction writer.");
        return res;
    }

    return OLAP_SUCCESS;
}

void DecimalColumnWriter::record_position() {
    ColumnWriter::record_position();
    _int_writer->get_position(index_entry(), false);
    _frac_writer->get_position(index_entry(), false);
}

////////////////////////////////////////////////////////////////////////////////

LargeIntColumnWriter::LargeIntColumnWriter(uint32_t column_id, OutStreamFactory* stream_factory,
                                           const TabletColumn& column,
                                           size_t num_rows_per_row_block, double bf_fpp)
        : ColumnWriter(column_id, stream_factory, column, num_rows_per_row_block, bf_fpp),
          _high_writer(nullptr),
          _low_writer(nullptr) {}

LargeIntColumnWriter::~LargeIntColumnWriter() {
    SAFE_DELETE(_high_writer);
    SAFE_DELETE(_low_writer);
}

OLAPStatus LargeIntColumnWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    res = ColumnWriter::init();
    if (OLAP_SUCCESS != res) {
        return res;
    }

    OutStream* high_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::DATA);
    OutStream* low_stream =
            stream_factory()->create_stream(unique_column_id(), StreamInfoMessage::SECONDARY);

    if (nullptr == high_stream || nullptr == low_stream) {
        OLAP_LOG_WARNING("fail to create stream.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _high_writer = new (std::nothrow) RunLengthIntegerWriter(high_stream, true);
    _low_writer = new (std::nothrow) RunLengthIntegerWriter(low_stream, true);

    if (nullptr == _high_writer || nullptr == _low_writer) {
        OLAP_LOG_WARNING("fail to create writer.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    record_position();
    return OLAP_SUCCESS;
}

OLAPStatus LargeIntColumnWriter::finalize(ColumnDataHeaderMessage* header) {
    OLAPStatus res;

    res = ColumnWriter::finalize(header);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize ColumnWriter.");
        return res;
    }

    res = _high_writer->flush();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush integer writer.");
        return res;
    }

    res = _low_writer->flush();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush fraction writer.");
        return res;
    }

    return OLAP_SUCCESS;
}

void LargeIntColumnWriter::record_position() {
    ColumnWriter::record_position();
    _high_writer->get_position(index_entry(), false);
    _low_writer->get_position(index_entry(), false);
}

} // namespace doris
