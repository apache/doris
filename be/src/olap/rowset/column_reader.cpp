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

#include "olap/rowset/column_reader.h"

#include <cstring>

#include "olap/rowset/bit_field_reader.h"
#include "olap/file_stream.h"
#include "olap/olap_define.h"

namespace doris {
IntegerColumnReader::IntegerColumnReader(uint32_t column_unique_id): 
        _eof(false),
        _column_unique_id(column_unique_id),
        _data_reader(NULL) {
}

IntegerColumnReader::~IntegerColumnReader() {
    SAFE_DELETE(_data_reader);
}

OLAPStatus IntegerColumnReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams, bool is_sign) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("input streams is NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Get data stream according to column id and type
    ReadOnlyFileStream* data_stream = extract_stream(_column_unique_id,
                                      StreamInfoMessage::DATA,
                                      streams);

    if (data_stream == NULL) {
        OLAP_LOG_WARNING("specified stream is NULL");
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _data_reader = new(std::nothrow) RunLengthIntegerReader(data_stream, is_sign);

    if (NULL == _data_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader");
        return OLAP_ERR_MALLOC_ERROR;
    }

    // reset eof flag when init, to support reinit
    return OLAP_SUCCESS;
}

OLAPStatus IntegerColumnReader::seek(PositionProvider* position) {
    return _data_reader->seek(position);
}

OLAPStatus IntegerColumnReader::skip(uint64_t row_count) {
    return _data_reader->skip(row_count);
}

OLAPStatus IntegerColumnReader::next(int64_t* value) {
    return _data_reader->next(value);
}

StringColumnDirectReader::StringColumnDirectReader(
        uint32_t column_unique_id,
        uint32_t dictionary_size) : 
        _eof(false),
        _column_unique_id(column_unique_id),
        _values(NULL),
        _data_stream(NULL),
        _length_reader(NULL) {
}

StringColumnDirectReader::~StringColumnDirectReader() {
    SAFE_DELETE(_length_reader);
}

OLAPStatus StringColumnDirectReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("input streams is NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Get data stream according to column id and type
    _data_stream = extract_stream(_column_unique_id,
                   StreamInfoMessage::DATA,
                   streams);

    if (NULL == _data_stream) {
        OLAP_LOG_WARNING("specified stream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _values = reinterpret_cast<Slice*>(mem_pool->allocate(size * sizeof(Slice)));

    ReadOnlyFileStream* length_stream = extract_stream(_column_unique_id,
                                        StreamInfoMessage::LENGTH,
                                        streams);

    if (NULL == length_stream) {
        OLAP_LOG_WARNING("specifiedstream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _length_reader = new(std::nothrow) RunLengthIntegerReader(length_stream, false);

    if (NULL == _length_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus StringColumnDirectReader::seek(PositionProvider* position) {
    OLAPStatus res = _data_stream->seek(position);

    // All strings in segment may be empty, so the data stream is EOF and
    // and length stream is not EOF.
    if (OLAP_SUCCESS == res || OLAP_ERR_COLUMN_STREAM_EOF == res) {
        res = _length_reader->seek(position);
    }

    return res;
}

OLAPStatus StringColumnDirectReader::skip(uint64_t row_count) {
    OLAPStatus res = OLAP_SUCCESS;
    int64_t skip_length = 0;
    int64_t tmp_length = 0;

    for (size_t i = 0; i < row_count; ++i) {
        res = _length_reader->next(&tmp_length);

        if (OLAP_SUCCESS != res) {
            return res;
        }

        skip_length += tmp_length;
    }

    if (OLAP_SUCCESS == res) {
        // TODO: skip function of instream is implemented, but not tested
        return _data_stream->skip(skip_length);
    }

    return res;
}

// Return string field of current row_count
OLAPStatus StringColumnDirectReader::next(char* buffer, uint32_t* length) {
    int64_t read_length = 0;
    OLAPStatus res = _length_reader->next(&read_length);
    *length = read_length;
    while (OLAP_SUCCESS == res && read_length > 0) {
        uint64_t buf_size = read_length;
        res = _data_stream->read(buffer, &buf_size);
        read_length -= buf_size;
        buffer += buf_size;
    }
    *length -= read_length;
    return res;
}

OLAPStatus StringColumnDirectReader::next_vector(
            ColumnVector* column_vector,
            uint32_t size,
            MemPool* mem_pool,
            int64_t* read_bytes) {
    /*
     * MemPool here is not the same as MemPool in init function
     * 1. MemPool is created by VectorizedRowBatch,
     *    and reset when load row batch
     * 2. MemPool in init function is created by SegmentReader,
     *    and free by SegmentReader deconstructor. 
     */
    OLAPStatus res = OLAP_SUCCESS;
    int64_t length = 0;
    int64_t string_buffer_size = 0;

    column_vector->set_col_data(_values);
    if (column_vector->no_nulls()) {
        for (int i = 0; i < size; ++i) {
            res = _length_reader->next(&length);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            _values[i].size = length;
            string_buffer_size += length;
        }

        char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(string_buffer_size));
        for (int i = 0; i < size; ++i) {
            length = _values[i].size;
            if (UNLIKELY(length == 0)) {
                _values[i].data = nullptr;
            } else {
                _values[i].data = string_buffer;
            }
            while (length > 0) {
                uint64_t buf_size = length;
                res = _data_stream->read(string_buffer, &buf_size);
                if (res != OLAP_SUCCESS) {
                    return res;
                }
                length -= buf_size;
                string_buffer += buf_size;
            }
        }
    } else {
        bool* is_null = column_vector->is_null();
        for (int i = 0; i < size; ++i) {
            if (!is_null[i]) {
                res = _length_reader->next(&length);
                if (OLAP_SUCCESS != res) {
                    return res;
                }
                _values[i].size = length;
                string_buffer_size += length;
            } else {
                _values[i].size = 0;
            }
        }

        char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(string_buffer_size));
        for (int i = 0; i < size; ++i) {
            if (!is_null[i]) {
                length = _values[i].size;
                if (UNLIKELY(length == 0)) {
                    _values[i].data = nullptr;
                } else {
                    _values[i].data = string_buffer;
                }
                while (length > 0) {
                    uint64_t buf_size = length;
                    res = _data_stream->read(string_buffer, &buf_size);
                    if (res != OLAP_SUCCESS) {
                        return res;
                    }
                    length -= buf_size;
                    string_buffer += buf_size;
                }
            } else {
                _values[i].data = nullptr;
                _values[i].size = 0;
            }
        }
    }
    *read_bytes += string_buffer_size;

    return res;
}

StringColumnDictionaryReader::StringColumnDictionaryReader(
        uint32_t column_unique_id,
        uint32_t dictionary_size) :
        _eof(false),
        _dictionary_size(dictionary_size),
        _column_unique_id(column_unique_id),
        _values(NULL),
        //_dictionary_size(0),
        //_offset_dictionary(NULL),
        //_dictionary_data_buffer(NULL),
        _read_buffer(NULL),
        _data_reader(NULL) {

}

StringColumnDictionaryReader::~StringColumnDictionaryReader() {
    //SAFE_DELETE_ARRAY(_offset_dictionary);
    //SAFE_DELETE(_dictionary_data_buffer);
    SAFE_DELETE(_data_reader);
    SAFE_DELETE_ARRAY(_read_buffer);
}

OLAPStatus StringColumnDictionaryReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool) {
    ReadOnlyFileStream* dictionary_data_stream = extract_stream(_column_unique_id,
            StreamInfoMessage::DICTIONARY_DATA,
            streams);

    if (NULL == dictionary_data_stream) {
        OLAP_LOG_WARNING("dictionary data stream not found. [unique id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    ReadOnlyFileStream* dictionary_length_stream = extract_stream(_column_unique_id,
            StreamInfoMessage::LENGTH,
            streams);

    if (NULL == dictionary_length_stream) {
        OLAP_LOG_WARNING("dictionary length stream not found. [unique id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    RunLengthIntegerReader* dictionary_length_reader =
        new(std::nothrow) RunLengthIntegerReader(dictionary_length_stream, false);
    OLAPStatus res = OLAP_SUCCESS;

    _values = reinterpret_cast<Slice*>(mem_pool->allocate(size * sizeof(Slice)));
    int64_t read_buffer_size = 1024;
    char* _read_buffer = new(std::nothrow) char[read_buffer_size];

    if (NULL == _read_buffer) {
        OLAP_LOG_WARNING("fail to malloc read buffer. [size = %lu]", read_buffer_size);
        return OLAP_ERR_MALLOC_ERROR;
    }

    int64_t length = 0;
    uint64_t read_length = 0;
    std::string dictionary_item;

    for (size_t dictionary_entry = 0; dictionary_entry < _dictionary_size; ++dictionary_entry) {
        res = dictionary_length_reader->next(&length);
        // 理论上应该足够读，读出eof也是不对的。
        if (OLAP_SUCCESS != res || length < 0) {
            OLAP_LOG_WARNING("build offset dictionary failed. [res = %d]", res);
            return res;
        }

        if (length > read_buffer_size) {
            SAFE_DELETE_ARRAY(_read_buffer);
            read_buffer_size = length;

            if (NULL == (_read_buffer = new(std::nothrow) char[read_buffer_size])) {
                OLAP_LOG_WARNING("fail to malloc read buffer. [size = %lu]", read_buffer_size);
                return OLAP_ERR_MALLOC_ERROR;
            }
        }

        read_length = length;
        dictionary_data_stream->read(_read_buffer, &read_length);

        if (static_cast<int64_t>(read_length) != length) {
            OLAP_LOG_WARNING("read stream fail.");
            return OLAP_ERR_COLUMN_READ_STREAM;
        }

        dictionary_item.assign(_read_buffer, length);
        _dictionary.push_back(dictionary_item);
    }

    // 建立数据流读取器
    ReadOnlyFileStream* data_stream = extract_stream(_column_unique_id,
                                      StreamInfoMessage::DATA,
                                      streams);

    if (NULL == data_stream) {
        OLAP_LOG_WARNING("data stream not found. [unique id = %u]", _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _data_reader = new(std::nothrow) RunLengthIntegerReader(data_stream, false);

    if (NULL == _data_reader) {
        OLAP_LOG_WARNING("fail to malloc data reader");
        return OLAP_ERR_MALLOC_ERROR;
    }

    SAFE_DELETE_ARRAY(_read_buffer);
    SAFE_DELETE(dictionary_length_reader);
    return OLAP_SUCCESS;
}

OLAPStatus StringColumnDictionaryReader::seek(PositionProvider* position) {
    return _data_reader->seek(position);
}

OLAPStatus StringColumnDictionaryReader::skip(uint64_t row_count) {
    return _data_reader->skip(row_count);
}

OLAPStatus StringColumnDictionaryReader::next(char* buffer, uint32_t* length) {
    int64_t value;
    OLAPStatus res = _data_reader->next(&value);
    // 错误或是EOF
    if (OLAP_SUCCESS != res) {
        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }

        return res;
    }

    if (value >= static_cast<int64_t>(_dictionary.size())) {
        OLAP_LOG_WARNING("value may indicated an invalid dictionary entry. "
                "[value = %lu, dictionary_size = %lu]",
                value, _dictionary.size());
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    memcpy(buffer, _dictionary[value].c_str(), _dictionary[value].size());
    *length = _dictionary[value].size();

    return OLAP_SUCCESS;
}

OLAPStatus StringColumnDictionaryReader::next_vector(
            ColumnVector* column_vector,
            uint32_t size,
            MemPool* mem_pool,
            int64_t* read_bytes) {

    int64_t index[size];
    int64_t buffer_size = 0;
    OLAPStatus res = OLAP_SUCCESS;

    column_vector->set_col_data(_values);
    if (column_vector->no_nulls()) {
        for (int i = 0; i < size; ++i) {
            res = _data_reader->next(&index[i]);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            if (index[i] >= static_cast<int64_t>(_dictionary.size())) {
                OLAP_LOG_WARNING("value may indicated an invalid dictionary entry. "
                                 "[index = %lu, dictionary_size = %lu]",
                                 index[i], _dictionary.size());
                return OLAP_ERR_BUFFER_OVERFLOW;
            }
            _values[i].size = _dictionary[index[i]].size();
            buffer_size += _values[i].size;
        }

        char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(buffer_size));
        for (int i = 0; i < size; ++i) {
            memory_copy(string_buffer,
                        _dictionary[index[i]].c_str(),
                        _values[i].size);
            _values[i].data = string_buffer;
            string_buffer += _values[i].size;
        }
    } else {
        bool* is_null = column_vector->is_null();
        for (int i = 0; i < size; ++i) {
            if (!is_null[i]) {
                res = _data_reader->next(&index[i]);
                if (OLAP_SUCCESS != res) {
                    return res;
                }
                if (index[i] >= static_cast<int64_t>(_dictionary.size())) {
                    OLAP_LOG_WARNING("value may indicated an invalid dictionary entry. "
                                     "[index = %lu, dictionary_size = %lu]",
                                     index[i], _dictionary.size());
                    return OLAP_ERR_BUFFER_OVERFLOW;
                }
                _values[i].size = _dictionary[index[i]].size();
                buffer_size += _values[i].size;
            }
        }

        char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(buffer_size));
        for (int i = 0; i < size; ++i) {
            if (!is_null[i]) {
                memory_copy(string_buffer,
                            _dictionary[index[i]].c_str(),
                            _values[i].size);
                _values[i].data = string_buffer;
                string_buffer += _values[i].size;
            }
        }
    }
    *read_bytes += buffer_size;

    return res;
}

ColumnReader::ColumnReader(uint32_t column_id, uint32_t column_unique_id) : 
        _value_present(false),
        _is_null(NULL),
        _column_id(column_id),
        _column_unique_id(column_unique_id),
        _present_reader(NULL) {
}

 ColumnReader* ColumnReader::create(uint32_t column_id,
        const TabletSchema& schema,
        const UniqueIdToColumnIdMap& included,
        UniqueIdToColumnIdMap& segment_included,
        const UniqueIdEncodingMap& encodings) {
    return create(column_id, schema.columns(), included, segment_included, encodings);
}

ColumnReader* ColumnReader::create(uint32_t column_id,
        const std::vector<TabletColumn>& schema,
        const UniqueIdToColumnIdMap& included,
        UniqueIdToColumnIdMap& segment_included,
        const UniqueIdEncodingMap& encodings) {
    if (column_id >= schema.size()) {
        LOG(WARNING) << "invalid column_id, column_id=" << column_id
                     << ", columns_size=" << schema.size();
        return NULL;
    }

    const TabletColumn& column = schema[column_id];
    ColumnReader* reader = NULL;
    int32_t column_unique_id = column.unique_id();

    if (0 == included.count(column_unique_id)) {
        return NULL;
    }

    if (0 == segment_included.count(column_unique_id)) {
        if (column.has_default_value()) {
            if (0 == strcasecmp("NULL", column.default_value().c_str())
                    && column.is_nullable()) {
                return new(std::nothrow) NullValueReader(column_id, column_unique_id);
            } else {
                return new(std::nothrow) DefaultValueReader(column_id, column_unique_id,
                        column.default_value(), column.type(), column.length());
            }
        } else if (column.is_nullable()) {
            LOG(WARNING) << "create NullValueReader: " << column.name();
            return new(std::nothrow) NullValueReader(column_id, column_unique_id);
        } else {
            OLAP_LOG_WARNING("not null field has no default value");
            return NULL;
        }
    }

    uint32_t dictionary_size = 0;
    ColumnEncodingMessage::Kind encode_kind = ColumnEncodingMessage::DIRECT;
    UniqueIdEncodingMap::const_iterator it = encodings.find(column_unique_id);

    if (it != encodings.end()) {
        encode_kind = (*it).second.kind();
        dictionary_size = (*it).second.dictionary_size();
    }

    switch (column.type()) {
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT: {
        reader = new(std::nothrow) TinyColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_SMALLINT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<int16_t, true>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<uint16_t, false>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_INT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<int32_t, true>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_UNSIGNED_INT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<uint32_t, false>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_BIGINT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<int64_t, true>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT: {
        reader = new(std::nothrow) IntegerColumnReaderWrapper<uint64_t, false>(
                column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_FLOAT: {
        reader = new(std::nothrow) FloatColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_DOUBLE: {
        reader = new(std::nothrow) DoubleColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE: {
        reader = new(std::nothrow) DiscreteDoubleColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_CHAR: {
        if (ColumnEncodingMessage::DIRECT == encode_kind) {
            reader = new(std::nothrow) FixLengthStringColumnReader<StringColumnDirectReader>(
                    column_id, column_unique_id, column.length(), dictionary_size);
        } else if (ColumnEncodingMessage::DICTIONARY == encode_kind) {
            reader = new(std::nothrow) FixLengthStringColumnReader<StringColumnDictionaryReader>(
                    column_id, column_unique_id, column.length(), dictionary_size);
        } else {
            OLAP_LOG_WARNING("known encoding format. data may be generated by higher version,"
                    "try updating olap/ngine binary to solve this problem");
            // TODO. define a new return code
            return NULL;
        }

        break;
    }

    case OLAP_FIELD_TYPE_DATETIME: {
        reader = new(std::nothrow) DateTimeColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_DATE: {
        reader = new(std::nothrow) DateColumnReader(column_id, column_unique_id);

        break;
    }

    case OLAP_FIELD_TYPE_DECIMAL: {
        reader = new(std::nothrow) DecimalColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_LARGEINT: {
        reader = new(std::nothrow) LargeIntColumnReader(column_id, column_unique_id);
        break;
    }

    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL: {
        if (ColumnEncodingMessage::DIRECT == encode_kind) {
            reader = new(std::nothrow) VarStringColumnReader<StringColumnDirectReader>(
                    column_id, column_unique_id, column.length(), dictionary_size);
        } else if (ColumnEncodingMessage::DICTIONARY == encode_kind) {
            reader = new(std::nothrow) VarStringColumnReader<StringColumnDictionaryReader>(
                    column_id, column_unique_id, column.length(), dictionary_size);
        } else {
            LOG(WARNING) << "known encoding format. data may be generated by higher version, "
                         << "try updating olap/ngine binary to solve this problem";
            // TODO. define a new return code
            return NULL;
        }

        break;
    }

    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_LIST:
    case OLAP_FIELD_TYPE_MAP:
    default: {
        LOG(WARNING) << "unspported filed type. field=" << column.name()
                     << ", type=" << column.type();
        break;
    }
    }

    return reader;
}

ColumnReader::~ColumnReader() {
    SAFE_DELETE(_present_reader);
}

OLAPStatus ColumnReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool,
        OlapReaderStatistics* stats) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("null parameters given.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _stats = stats;

    // 从map中找到需要的流，ColumnReader的数据应该由一条PRESENT流和一条ROW_INDEX流组成
    ReadOnlyFileStream* present_stream = extract_stream(_column_unique_id,
                                         StreamInfoMessage::PRESENT,
                                         streams);

    _is_null = reinterpret_cast<bool*>(mem_pool->allocate(size));
    memset(_is_null, 0, size);

    if (NULL == present_stream) {
        _present_reader = NULL;
        _value_present = false;
    } else {
        VLOG(10) << "create null present_stream for column_id:" << _column_unique_id;
        _present_reader = new(std::nothrow) BitFieldReader(present_stream);

        if (NULL == _present_reader) {
            OLAP_LOG_WARNING("malloc present reader failed.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        if (OLAP_SUCCESS != _present_reader->init()) {
            OLAP_LOG_WARNING("fail to init present reader.");
            return OLAP_ERR_INIT_FAILED;
        }

        _value_present = true;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnReader::seek(PositionProvider* position) {
    if (NULL != _present_reader) {
        return _present_reader->seek(position);
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnReader::skip(uint64_t row_count) {
    return OLAP_SUCCESS;
}

OLAPStatus ColumnReader::next_vector(
            ColumnVector* column_vector,
            uint32_t size,
            MemPool* mem_pool) {
    OLAPStatus res = OLAP_SUCCESS;
    column_vector->set_is_null(_is_null);
    if (NULL != _present_reader) {
        column_vector->set_no_nulls(false);
        for (uint32_t i = 0; i < size; ++i) {
            bool value = false;
            res = _present_reader->next((char*)&value);
            if (OLAP_SUCCESS != res) {
                break;
            }
            _is_null[i] = value;
        }
        _stats->bytes_read += size;
    } else {
        column_vector->set_no_nulls(true);
    }

    return res;
}

uint64_t ColumnReader::_count_none_nulls(uint64_t rows) {
    if (_present_reader != NULL) {
        OLAPStatus res = OLAP_SUCCESS;
        uint64_t result = 0;

        for (uint64_t counter = 0; counter < rows; ++counter) {
            res = _present_reader->next(reinterpret_cast<char*>(&_value_present));

            if (OLAP_SUCCESS == res && (false == _value_present)) {
                result += 1;
            } else {
                break;
            }
        }

        return result;
    } else {
        return rows;
    }
}

TinyColumnReader::TinyColumnReader(uint32_t column_id, uint32_t column_unique_id) : 
        ColumnReader(column_id, column_unique_id),
        _eof(false),
        _values(NULL),
        _data_reader(NULL) {}

TinyColumnReader::~TinyColumnReader() {
    SAFE_DELETE(_data_reader);
}

OLAPStatus TinyColumnReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool,
        OlapReaderStatistics* stats) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("input streams is NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    ColumnReader::init(streams, size, mem_pool, stats);
    ReadOnlyFileStream* data_stream = extract_stream(_column_unique_id,
                                      StreamInfoMessage::DATA,
                                      streams);

    if (NULL == data_stream) {
        OLAP_LOG_WARNING("specified stream not exist");
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _values = reinterpret_cast<char*>(mem_pool->allocate(size));
    _data_reader = new(std::nothrow) RunLengthByteReader(data_stream);

    if (NULL == _data_reader) {
        OLAP_LOG_WARNING("malloc data reader failed");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus TinyColumnReader::seek(PositionProvider* positions) {
    OLAPStatus res;
    if (NULL == _present_reader) {
        res = _data_reader->seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }
    } else {
        res = ColumnReader::seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }
        res = _data_reader->seek(positions);
        if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
            OLAP_LOG_WARNING("fail to seek tinyint stream. [res=%d]", res);
            return res;
        }
    }

    return OLAP_SUCCESS; 
}

OLAPStatus TinyColumnReader::skip(uint64_t row_count) {
    // count_none_nulls 其实就是columnReader的跳过函数。
    return _data_reader->skip(_count_none_nulls(row_count));
}

OLAPStatus TinyColumnReader::next_vector(
        ColumnVector* column_vector,
        uint32_t size,
        MemPool* mem_pool) {
    OLAPStatus res = ColumnReader::next_vector(column_vector, size, mem_pool);
    if (OLAP_SUCCESS != res) {
        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }
        return res;
    }

    bool* is_null = column_vector->is_null();
    column_vector->set_col_data(_values);
    if (column_vector->no_nulls()) {
        for (uint32_t i = 0; i < size; ++i) {
            res = _data_reader->next(_values + i);
            if (OLAP_SUCCESS != res) {
                break;
            }
        }
    } else {
        for (uint32_t i = 0; i < size; ++i) {
            if (!is_null[i]) {
                res = _data_reader->next(_values + i);
                if (OLAP_SUCCESS != res) {
                    break;
                }
            }
        }
    }
    _stats->bytes_read += size;

    if (OLAP_ERR_DATA_EOF == res) {
        _eof = true;
    }

    return res;
}

DecimalColumnReader::DecimalColumnReader(uint32_t column_id, uint32_t column_unique_id) : 
        ColumnReader(column_id, column_unique_id),
        _eof(false),
        _values(NULL),
        _int_reader(NULL),
        _frac_reader(NULL) {
}

DecimalColumnReader::~DecimalColumnReader() {
    SAFE_DELETE(_int_reader);
    SAFE_DELETE(_frac_reader);
}

OLAPStatus DecimalColumnReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool,
        OlapReaderStatistics* stats) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("input streams is NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // reset stream and reader
    ColumnReader::init(streams, size, mem_pool, stats);

    _values = reinterpret_cast<decimal12_t*>(mem_pool->allocate(size * sizeof(decimal12_t)));

    // 从map中找到需要的流，StringColumnReader的数据应该由一条DATA流和一条LENGTH流组成
    ReadOnlyFileStream* int_stream = extract_stream(_column_unique_id,
                                     StreamInfoMessage::DATA,
                                     streams);

    if (NULL == int_stream) {
        OLAP_LOG_WARNING("specified stream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    ReadOnlyFileStream* frac_stream = extract_stream(_column_unique_id,
                                      StreamInfoMessage::SECONDARY,
                                      streams);

    if (NULL == frac_stream) {
        OLAP_LOG_WARNING("specified stream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _int_reader = new(std::nothrow) RunLengthIntegerReader(int_stream, true);

    if (NULL == _int_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _frac_reader = new(std::nothrow) RunLengthIntegerReader(frac_stream, true);

    if (NULL == _frac_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus DecimalColumnReader::seek(PositionProvider* positions) {
    OLAPStatus res;
    if (NULL == _present_reader) {
        res = _int_reader->seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }

        res = _frac_reader->seek(positions);

        if (OLAP_SUCCESS != res) {
            return res;
        }
    } else {
        //all field in the segment can be NULL, so the data stream is EOF
        res = ColumnReader::seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }
        res = _int_reader->seek(positions);
        if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
            OLAP_LOG_WARNING("fail to seek int stream of decimal. [res=%d]", res);
            return res;
        }

        res = _frac_reader->seek(positions);
        if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
            OLAP_LOG_WARNING("fail to seek frac stream of decimal. [res=%d]", res);
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus DecimalColumnReader::skip(uint64_t row_count) {
    OLAPStatus res = _int_reader->skip(row_count);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to create int part reader");
        return res;
    }

    res = _frac_reader->skip(row_count);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to create frac part reader");
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus DecimalColumnReader::next_vector(
        ColumnVector* column_vector,
        uint32_t size,
        MemPool* mem_pool) {
    OLAPStatus res = ColumnReader::next_vector(column_vector, size, mem_pool);
    if (OLAP_SUCCESS != res) {
        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }
        return res;
    }

    bool* is_null = column_vector->is_null();
    column_vector->set_col_data(_values);

    if (column_vector->no_nulls()) {
        for (uint32_t i = 0; i < size; ++i) {
            int64_t value = 0;
            OLAPStatus res = _int_reader->next(&value);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to read decimal int part");
                break;
            }
            _values[i].integer = value;

            res = _frac_reader->next(&value);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to read decimal frac part");
                break;
            }
            _values[i].fraction = value;
        }
    } else {
        for (uint32_t i = 0; i < size; ++i) {
            int64_t value = 0;
            if (!is_null[i]) {
                OLAPStatus res = _int_reader->next(&value);
                if (OLAP_SUCCESS != res) {
                    OLAP_LOG_WARNING("fail to read decimal int part");
                    break;
                }
                _values[i].integer = value;

                res = _frac_reader->next(&value);
                if (OLAP_SUCCESS != res) {
                    OLAP_LOG_WARNING("fail to read decimal frac part");
                    break;
                }
                _values[i].fraction = value;
            }
        }
    }
    _stats->bytes_read += sizeof(decimal12_t) * size;

    return res;
}

LargeIntColumnReader::LargeIntColumnReader(uint32_t column_id, uint32_t column_unique_id) : 
        ColumnReader(column_id, column_unique_id),
        _eof(false),
        _values(NULL),
        _high_reader(NULL),
        _low_reader(NULL) {}

LargeIntColumnReader::~LargeIntColumnReader() {
    SAFE_DELETE(_high_reader);
    SAFE_DELETE(_low_reader);
}

OLAPStatus LargeIntColumnReader::init(
        std::map<StreamName, ReadOnlyFileStream*>* streams,
        int size, MemPool* mem_pool,
        OlapReaderStatistics* stats) {
    if (NULL == streams) {
        OLAP_LOG_WARNING("input streams is NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // reset stream and reader
    ColumnReader::init(streams, size, mem_pool, stats);

    _values = reinterpret_cast<int128_t*>(
        mem_pool->try_allocate_aligned(size * sizeof(int128_t), alignof(int128_t)));

    // 从map中找到需要的流，LargeIntColumnReader的数据应该由一条DATA流组成
    ReadOnlyFileStream* high_stream = extract_stream(_column_unique_id,
                                     StreamInfoMessage::DATA,
                                     streams);
    if (NULL == high_stream) {
        OLAP_LOG_WARNING("specified stream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    ReadOnlyFileStream* low_stream = extract_stream(_column_unique_id,
                                      StreamInfoMessage::SECONDARY,
                                      streams);
    if (NULL == low_stream) {
        OLAP_LOG_WARNING("specified stream not found. [unique_id = %u]",
                _column_unique_id);
        return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
    }

    _high_reader = new(std::nothrow) RunLengthIntegerReader(high_stream, true);
    if (NULL == _high_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    _low_reader = new(std::nothrow) RunLengthIntegerReader(low_stream, true);
    if (NULL == _low_reader) {
        OLAP_LOG_WARNING("fail to malloc RunLengthIntegerReader.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus LargeIntColumnReader::seek(PositionProvider* positions) {
    OLAPStatus res;
    if (NULL == _present_reader) {
        res = _high_reader->seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }

        res = _low_reader->seek(positions);
        if (OLAP_SUCCESS != res) {
            return res;
        }
    } else {
        //all field in the segment can be NULL, so the data stream is EOF
        res = ColumnReader::seek(positions);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to seek null stream of largeint");
            return res;
        }

        res = _high_reader->seek(positions);
        if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
            OLAP_LOG_WARNING("fail to seek high int stream of largeint. [res=%d]", res);
            return res;
        }

        res = _low_reader->seek(positions);
        if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
            OLAP_LOG_WARNING("fail to seek low int stream of largeint. [res=%d]", res);
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus LargeIntColumnReader::skip(uint64_t row_count) {
    OLAPStatus res = _high_reader->skip(row_count);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to skip large int high part. [res=%d]", res);
        return res;
    }

    res = _low_reader->skip(row_count);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to skip large int low part reader. [res=%d]", res);
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus LargeIntColumnReader::next_vector(
        ColumnVector* column_vector,
        uint32_t size,
        MemPool* mem_pool) {
    OLAPStatus res = ColumnReader::next_vector(column_vector, size, mem_pool);
    if (OLAP_SUCCESS != res) {
        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }
        return res;
    }

    bool* is_null = column_vector->is_null();
    column_vector->set_col_data(_values);

    if (column_vector->no_nulls()) {
        for (uint32_t i = 0; i < size; ++i) {
            int64_t* value = NULL;
            value = (int64_t*)(_values + i);
            res = _high_reader->next(value);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to read decimal int part");
                break;
            }

            res = _low_reader->next(++value);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to read decimal frac part");
                break;
            }
        }
    } else {
        for (uint32_t i = 0; i < size; ++i) {
            int64_t* value = NULL;
            if (!is_null[i]) {
                value = (int64_t*)(_values + i);
                res = _high_reader->next(value);
                if (OLAP_SUCCESS != res) {
                    OLAP_LOG_WARNING("fail to read decimal int part");
                    break;
                }

                res = _low_reader->next(++value);
                if (OLAP_SUCCESS != res) {
                    OLAP_LOG_WARNING("fail to read decimal frac part");
                    break;
                }
            }
        }
    }
    _stats->bytes_read += 16 * size;

    return res;
}

}  // namespace doris
