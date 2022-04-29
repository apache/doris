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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_COLUMN_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_COLUMN_READER_H

#include "olap/byte_buffer.h"
#include "olap/field.h"
#include "olap/file_stream.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/run_length_byte_reader.h"
#include "olap/rowset/run_length_integer_reader.h"
#include "olap/stream_name.h"
#include "runtime/vectorized_row_batch.h"
#include "util/date_func.h"

namespace doris {

class StreamName;
class ReadOnlyFileStream;
class BitFieldReader;
class RowIndexEntryMessage;
class ColumnEncodingMessage;

// Solution flow
inline ReadOnlyFileStream* extract_stream(uint32_t column_unique_id, StreamInfoMessage::Kind kind,
                                          std::map<StreamName, ReadOnlyFileStream*>* streams) {
    StreamName stream_name(column_unique_id, kind);
    std::map<StreamName, ReadOnlyFileStream*>::iterator it = streams->find(stream_name);

    if (streams->end() != it) {
        return (*it).second;
    }

    return nullptr;
}

// Unique id -> PositionProvider
typedef std::unordered_map<uint32_t, PositionProvider> UniqueIdPositionProviderMap;
// Unique id -> ColumnEncodingMessage
typedef std::map<uint32_t, ColumnEncodingMessage> UniqueIdEncodingMap;

// Readers for Integer and String.
// Although these readers are also named Reader, note that they do not inherit from ColumnReader
// Therefore, the case of null values is not considered.

// For SHORT/INT/LONG type data, use int64 as the stored data uniformly
// Due to the use of variable length coding, it will not cause waste
// IntegerColumnReader is a reader that reads the int64 data of the bottom layer, when the upper layer returns
// Use IntColumnReaderWrapper to convert to a specific data type
//
// NOTE. Since the RLE reader can only read int64, which is different from java, java shaping does not consider symbols
// Then this thing actually seems to be �
// �� method supports unsigned integer shaping, you need to pay attention to whether to modify RLEReader in the future
class IntegerColumnReader {
public:
    IntegerColumnReader(uint32_t column_unique_id);
    ~IntegerColumnReader();
    /**
      * Initialize the Integer column reader
      * @param streams contains the map of the required stream
      * @param is_sign whether the number read has a sign
      * @return [description]
      */
    Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, bool is_sign);
    // Position the internal pointer to positions
    Status seek(PositionProvider* positions);
    // Move the internal pointer back row_count rows
    Status skip(uint64_t row_count);
    // Return the data of the current row by moving the internal pointer to the next row
    Status next(int64_t* value);
    bool eof() { return _eof; }

private:
    bool _eof;
    uint32_t _column_unique_id;
    RunLengthIntegerReader* _data_reader;
};

// For readers of string columns encoded in Direct mode
// Direct method of String can be read directly
class StringColumnDirectReader {
public:
    StringColumnDirectReader(uint32_t column_unique_id, uint32_t dictionary_size);
    ~StringColumnDirectReader();

    Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size, MemPool* mem_pool);
    Status seek(PositionProvider* positions);
    Status skip(uint64_t row_count);
    // Return the data of the current row and move the internal pointer backward
    // buffer - the buffer of the returned data
    // length - the size of the buffer area when input, and the size of the string when returning
    Status next(char* buffer, uint32_t* length);
    Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool,
                       int64_t* read_bytes);

    size_t get_buffer_size() { return sizeof(RunLengthByteReader); }

private:
    uint32_t _column_unique_id;
    Slice* _values;
    ReadOnlyFileStream* _data_stream;
    RunLengthIntegerReader* _length_reader;
};

// For readers using dictionary-encoded string columns
// The interface is the same as StringColumnDirectReader
// Reading process:
// 1. Read all the dictionary data and save it in the whole buffer
// 2. Read the length data, construct the offset dictionary, the offset is �
// ��The beginning of each string, combined with 1 can read data
// 3. Read the actual saved data when needed.
// �� is an int). Find the offset according to this int, and then read the dictionary data according to the offset
class StringColumnDictionaryReader {
public:
    StringColumnDictionaryReader(uint32_t column_unique_id, uint32_t dictionary_size);
    ~StringColumnDictionaryReader();
    Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size, MemPool* mem_pool);
    Status seek(PositionProvider* positions);
    Status skip(uint64_t row_count);
    Status next(char* buffer, uint32_t* length);
    Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool,
                       int64_t* read_bytes);

    size_t get_buffer_size() { return sizeof(RunLengthByteReader) + _dictionary_size; }

private:
    bool _eof;
    uint32_t _dictionary_size;
    uint32_t _column_unique_id;
    Slice* _values;
    char* _read_buffer;
    //uint64_t _dictionary_size;
    //uint64_t* _offset_dictionary;   // The offset corresponding to the number used to find the response data
    //StorageByteBuffer* _dictionary_data_buffer;   // Save dict data
    std::vector<std::string> _dictionary;
    // Used to read the actual data (represented by an integer)
    RunLengthIntegerReader* _data_reader;
};

// ColumnReader is used to read a column and is the base class of other XXXColumnReader
// ColumnReader maintains the NULL feature of the column through the bit field presented
class ColumnReader {
public:
    // Factory method, create ColumnReader, if the column has sub-columns, recursively create a sub reader
    // If the column to be read does not exist in segment_columns, then:
    //       1.If the column allows Null values, create a NullValueReader
    //       2.If the column does not allow Null values, but has a default value, create a DefaultValueReader
    //       3.Otherwise the creation fails
    // Input:
    //       column_id - the position of the column to be created in the columns
    //       columns - the schema of the table
    //       included - column to be created, if the unique id of a column is included in included
    //       segment_columns - a collection of unique ids of all columns in the segment
    //       encodings - column encoding information, use encodings[_column_unique_id] to access
    static ColumnReader* create(uint32_t column_id, const TabletSchema& schema,
                                const UniqueIdToColumnIdMap& included,
                                UniqueIdToColumnIdMap& segment_included,
                                const UniqueIdEncodingMap& encodings);

    static ColumnReader* create(uint32_t column_id, const std::vector<TabletColumn>& schema,
                                const UniqueIdToColumnIdMap& included,
                                UniqueIdToColumnIdMap& segment_included,
                                const UniqueIdEncodingMap& encodings);

    ColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~ColumnReader();

    // Use streams to initialize Reader
    // ColumnReader is initialized only once, and a new object is allocated each time it is used.
    // Input:
    //       streams-input stream
    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats);

    // Set the position of the next returned data
    // positions are the positions where each column needs to seek, ColumnReader passes (*positions)[_column_unique_id]
    // Get the seek position of this column
    virtual Status seek(PositionProvider* positions);

    virtual Status skip(uint64_t row_count);

    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool);

    uint32_t column_unique_id() { return _column_unique_id; }

    uint32_t column_id() { return _column_id; }

    virtual size_t get_buffer_size() { return 0; }

protected:
    // NOTE. Count the non-blank rows in rows. This is because "blank lines" do not exist in actual storage.
    // So for upper-level fields that may be empty (such as integer), the caller wants to skip 10 lines,
    // but actually for
    uint64_t _count_none_nulls(uint64_t rows);

    bool _value_present;
    bool* _is_null;
    uint32_t _column_id;             // The id of the column in the schema
    uint32_t _column_unique_id;      // the unique id of the column
    BitFieldReader* _present_reader; // NULL value of NULLabel field
    std::vector<ColumnReader*> _sub_readers;
    OlapReaderStatistics* _stats = nullptr;
};

class DefaultValueReader : public ColumnReader {
public:
    DefaultValueReader(uint32_t column_id, uint32_t column_unique_id, std::string default_value,
                       FieldType type, int length)
            : ColumnReader(column_id, column_unique_id),
              _default_value(default_value),
              _values(nullptr),
              _type(type),
              _length(length) {}

    virtual ~DefaultValueReader() {}

    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats) {
        switch (_type) {
        case OLAP_FIELD_TYPE_TINYINT: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(int8_t)));
            int32_t value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((int8_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_SMALLINT: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(int16_t)));
            int16_t value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((int16_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_INT: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(int32_t)));
            int32_t value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((int32_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_BIGINT: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(int64_t)));
            int64_t value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((int64_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_LARGEINT: {
            _values = reinterpret_cast<void*>(
                    mem_pool->try_allocate_aligned(size * sizeof(int128_t), alignof(int128_t)));
            int128_t value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((int128_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_FLOAT: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(float)));
            float value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((float*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_DOUBLE: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(double)));
            double value = 0;
            std::stringstream ss(_default_value);
            ss >> value;
            for (int i = 0; i < size; ++i) {
                ((double*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(decimal12_t)));
            decimal12_t value = {0, 0};
            value.from_string(_default_value);
            for (int i = 0; i < size; ++i) {
                ((decimal12_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_CHAR: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(Slice)));
            int32_t length = _length;
            char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(size * length));
            memset(string_buffer, 0, size * length);
            for (int i = 0; i < size; ++i) {
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                ((Slice*)_values)[i].size = length;
                ((Slice*)_values)[i].data = string_buffer;
                string_buffer += length;
            }
            break;
        }
        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_OBJECT:
        case OLAP_FIELD_TYPE_HLL:
        case OLAP_FIELD_TYPE_STRING: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(Slice)));
            int32_t length = _default_value.length();
            char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(size * length));
            for (int i = 0; i < size; ++i) {
                memory_copy(string_buffer, _default_value.c_str(), length);
                ((Slice*)_values)[i].size = length;
                ((Slice*)_values)[i].data = string_buffer;
                string_buffer += length;
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATE: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(uint24_t)));
            uint24_t value = timestamp_from_date(_default_value);
            for (int i = 0; i < size; ++i) {
                ((uint24_t*)_values)[i] = value;
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATETIME: {
            _values = reinterpret_cast<void*>(mem_pool->allocate(size * sizeof(uint64_t)));
            uint64_t value = timestamp_from_datetime(_default_value);
            for (int i = 0; i < size; ++i) {
                ((uint64_t*)_values)[i] = value;
            }
            break;
        }
        default:
            break;
        }
        _stats = stats;
        return Status::OK();
    }
    virtual Status seek(PositionProvider* positions) { return Status::OK(); }
    virtual Status skip(uint64_t row_count) { return Status::OK(); }

    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool) {
        column_vector->set_no_nulls(true);
        column_vector->set_col_data(_values);
        _stats->bytes_read += _length * size;
        return Status::OK();
    }

private:
    std::string _default_value;
    void* _values;
    FieldType _type;
    int32_t _length;
};

class NullValueReader : public ColumnReader {
public:
    NullValueReader(uint32_t column_id, uint32_t column_unique_id)
            : ColumnReader(column_id, column_unique_id) {}
    Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size, MemPool* mem_pool,
                OlapReaderStatistics* stats) override {
        _is_null = reinterpret_cast<bool*>(mem_pool->allocate(size));
        memset(_is_null, 1, size);
        _stats = stats;
        return Status::OK();
    }
    virtual Status seek(PositionProvider* positions) override { return Status::OK(); }
    virtual Status skip(uint64_t row_count) override { return Status::OK(); }
    virtual Status next_vector(ColumnVector* column_vector, uint32_t size,
                               MemPool* mem_pool) override {
        column_vector->set_no_nulls(false);
        column_vector->set_is_null(_is_null);
        _stats->bytes_read += size;
        return Status::OK();
    }
};

// 对于Tiny类型, 使用Byte作为存储
class TinyColumnReader : public ColumnReader {
public:
    TinyColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~TinyColumnReader();

    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats);
    virtual Status seek(PositionProvider* positions);
    virtual Status skip(uint64_t row_count);
    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool);

    virtual size_t get_buffer_size() { return sizeof(RunLengthByteReader); }

private:
    bool _eof;
    char* _values;
    RunLengthByteReader* _data_reader;
};

// A wrapper for IntColumnReader, which implements the interface to ColumnReader
template <class T, bool is_sign>
class IntegerColumnReaderWrapper : public ColumnReader {
public:
    IntegerColumnReaderWrapper(uint32_t column_id, uint32_t column_unique_id)
            : ColumnReader(column_id, column_unique_id),
              _reader(column_unique_id),
              _values(nullptr),
              _eof(false) {}

    virtual ~IntegerColumnReaderWrapper() {}

    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats) {
        Status res = ColumnReader::init(streams, size, mem_pool, stats);

        if (res.ok()) {
            res = _reader.init(streams, is_sign);
        }

        _values = reinterpret_cast<T*>(mem_pool->allocate(size * sizeof(T)));

        return res;
    }
    virtual Status seek(PositionProvider* positions) {
        Status res;
        if (nullptr == _present_reader) {
            res = _reader.seek(positions);
            if (!res.ok()) {
                return res;
            }
        } else {
            //all field in the segment can be nullptr, so the data stream is EOF
            res = ColumnReader::seek(positions);
            if (!res.ok()) {
                return res;
            }
            res = _reader.seek(positions);
            if (!res.ok() && Status::OLAPInternalError(OLAP_ERR_COLUMN_STREAM_EOF) != res) {
                LOG(WARNING) << "fail to seek int stream. res = " << res;
                return res;
            }
        }

        return Status::OK();
    }
    virtual Status skip(uint64_t row_count) { return _reader.skip(_count_none_nulls(row_count)); }

    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool) {
        Status res = ColumnReader::next_vector(column_vector, size, mem_pool);
        if (!res.ok()) {
            if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
                _eof = true;
            }
            return res;
        }

        column_vector->set_col_data(_values);
        if (column_vector->no_nulls()) {
            for (uint32_t i = 0; i < size; ++i) {
                int64_t value = 0;
                res = _reader.next(&value);
                if (!res.ok()) {
                    break;
                }
                _values[i] = value;
            }
        } else {
            bool* is_null = column_vector->is_null();
            for (uint32_t i = 0; i < size; ++i) {
                int64_t value = 0;
                if (!is_null[i]) {
                    res = _reader.next(&value);
                    if (!res.ok()) {
                        break;
                    }
                }
                _values[i] = value;
            }
        }
        _stats->bytes_read += sizeof(T) * size;

        if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
            _eof = true;
        }
        return res;
    }

    virtual size_t get_buffer_size() { return sizeof(RunLengthIntegerReader); }

private:
    IntegerColumnReader _reader; // Wrapped real reader
    T* _values;
    bool _eof;
};

// There are two types of strings in OLAP Engine, fixed-length strings and variable-length strings, using two wrappers respectively
// class handles the return format of these two strings
// FixLengthStringColumnReader handles fixed-length strings, the feature is that the part of insufficient length should be filled with 0
template <class ReaderClass>
class FixLengthStringColumnReader : public ColumnReader {
public:
    FixLengthStringColumnReader(uint32_t column_id, uint32_t column_unique_id,
                                uint32_t string_length, uint32_t dictionary_size)
            : ColumnReader(column_id, column_unique_id),
              _eof(false),
              _reader(column_unique_id, dictionary_size),
              _string_length(string_length) {}
    virtual ~FixLengthStringColumnReader() {}

    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats) {
        Status res = ColumnReader::init(streams, size, mem_pool, stats);

        if (res.ok()) {
            res = _reader.init(streams, size, mem_pool);
        }

        return res;
    }

    virtual Status seek(PositionProvider* positions) {
        Status res;
        if (nullptr == _present_reader) {
            res = _reader.seek(positions);
            if (!res.ok()) {
                return res;
            }
        } else {
            //all field in the segment can be nullptr, so the data stream is EOF
            res = ColumnReader::seek(positions);
            if (!res.ok()) {
                return res;
            }
            res = _reader.seek(positions);
            if (!res.ok() && Status::OLAPInternalError(OLAP_ERR_COLUMN_STREAM_EOF) != res) {
                LOG(WARNING) << "fail to read fixed string stream. res = " << res;
                return res;
            }
        }

        return Status::OK();
    }
    virtual Status skip(uint64_t row_count) { return _reader.skip(_count_none_nulls(row_count)); }
    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool) {
        Status res = ColumnReader::next_vector(column_vector, size, mem_pool);
        if (!res.ok()) {
            if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
                _eof = true;
            }
            return res;
        }

        return _reader.next_vector(column_vector, size, mem_pool, &_stats->bytes_read);
    }

    virtual size_t get_buffer_size() { return _reader.get_buffer_size() + _string_length; }

private:
    bool _eof;
    ReaderClass _reader;
    uint32_t _string_length;
};

// VarStringColumnReader handles variable length strings, characterized by using uint16 in the data header to indicate the length
template <class ReaderClass>
class VarStringColumnReader : public ColumnReader {
public:
    VarStringColumnReader(uint32_t column_id, uint32_t column_unique_id, uint32_t max_length,
                          uint32_t dictionary_size)
            : ColumnReader(column_id, column_unique_id),
              _eof(false),
              _reader(column_unique_id, dictionary_size),
              _max_length(max_length) {}
    virtual ~VarStringColumnReader() {}
    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats) {
        Status res = ColumnReader::init(streams, size, mem_pool, stats);
        if (res.ok()) {
            res = _reader.init(streams, size, mem_pool);
        }

        return res;
    }

    virtual Status seek(PositionProvider* position) {
        Status res;
        if (nullptr == _present_reader) {
            res = _reader.seek(position);
            if (!res.ok()) {
                return res;
            }
        } else {
            //all field in the segment can be nullptr, so the data stream is EOF
            res = ColumnReader::seek(position);
            if (!res.ok()) {
                return res;
            }
            res = _reader.seek(position);
            if (!res.ok() && Status::OLAPInternalError(OLAP_ERR_COLUMN_STREAM_EOF) != res) {
                LOG(WARNING) << "fail to seek varchar stream. res = " << res;
                return res;
            }
        }

        return Status::OK();
    }
    virtual Status skip(uint64_t row_count) { return _reader.skip(_count_none_nulls(row_count)); }

    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool) {
        Status res = ColumnReader::next_vector(column_vector, size, mem_pool);
        if (!res.ok()) {
            if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
                _eof = true;
            }
            return res;
        }

        return _reader.next_vector(column_vector, size, mem_pool, &_stats->bytes_read);
    }

    virtual size_t get_buffer_size() { return _reader.get_buffer_size() + _max_length; }

private:
    bool _eof;
    ReaderClass _reader;
    uint32_t _max_length;
};

template <typename FLOAT_TYPE>
class FloatintPointColumnReader : public ColumnReader {
public:
    FloatintPointColumnReader(uint32_t column_id, uint32_t column_unique_id)
            : ColumnReader(column_id, column_unique_id),
              _eof(false),
              _data_stream(nullptr),
              _values(nullptr) {}

    virtual ~FloatintPointColumnReader() {}

    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats) {
        if (nullptr == streams) {
            OLAP_LOG_WARNING("input streams is nullptr");
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        // reset stream and reader
        ColumnReader::init(streams, size, mem_pool, stats);
        _data_stream = extract_stream(_column_unique_id, StreamInfoMessage::DATA, streams);

        if (nullptr == _data_stream) {
            OLAP_LOG_WARNING("specified stream not exist");
            return Status::OLAPInternalError(OLAP_ERR_COLUMN_STREAM_NOT_EXIST);
        }

        _values = reinterpret_cast<FLOAT_TYPE*>(mem_pool->allocate(size * sizeof(FLOAT_TYPE)));
        return Status::OK();
    }
    virtual Status seek(PositionProvider* position) {
        if (nullptr == position) {
            OLAP_LOG_WARNING("input positions is nullptr");
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        if (nullptr == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
        }

        Status res;
        if (nullptr == _present_reader) {
            res = _data_stream->seek(position);
            if (!res.ok()) {
                return res;
            }
        } else {
            //all field in the segment can be nullptr, so the data stream is EOF
            res = ColumnReader::seek(position);
            if (!res.ok()) {
                return res;
            }
            res = _data_stream->seek(position);
            if (!res.ok() && Status::OLAPInternalError(OLAP_ERR_COLUMN_STREAM_EOF) != res) {
                LOG(WARNING) << "fail to seek float stream. res = " << res;
                return res;
            }
        }

        return Status::OK();
    }
    virtual Status skip(uint64_t row_count) {
        if (nullptr == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
        }

        uint64_t skip_values_count = _count_none_nulls(row_count);
        return _data_stream->skip(skip_values_count * sizeof(FLOAT_TYPE));
    }

    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool) {
        if (nullptr == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
        }

        Status res = ColumnReader::next_vector(column_vector, size, mem_pool);
        if (!res.ok()) {
            if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
                _eof = true;
            }
            return res;
        }

        bool* is_null = column_vector->is_null();
        column_vector->set_col_data(_values);
        size_t length = sizeof(FLOAT_TYPE);
        if (column_vector->no_nulls()) {
            for (uint32_t i = 0; i < size; ++i) {
                FLOAT_TYPE value = 0.0;
                res = _data_stream->read(reinterpret_cast<char*>(&value), &length);
                if (!res.ok()) {
                    break;
                }
                _values[i] = value;
            }
        } else {
            for (uint32_t i = 0; i < size; ++i) {
                FLOAT_TYPE value = 0.0;
                if (!is_null[i]) {
                    res = _data_stream->read(reinterpret_cast<char*>(&value), &length);
                    if (!res.ok()) {
                        break;
                    }
                }
                _values[i] = value;
            }
        }
        _stats->bytes_read += sizeof(FLOAT_TYPE) * size;

        if (Status::OLAPInternalError(OLAP_ERR_DATA_EOF) == res) {
            _eof = true;
        }

        return res;
    }

protected:
    bool _eof;
    ReadOnlyFileStream* _data_stream;
    FLOAT_TYPE* _values;
};

class DecimalColumnReader : public ColumnReader {
public:
    DecimalColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~DecimalColumnReader();
    Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size, MemPool* mem_pool,
                OlapReaderStatistics* stats) override;
    virtual Status seek(PositionProvider* positions) override;
    virtual Status skip(uint64_t row_count) override;
    virtual Status next_vector(ColumnVector* column_vector, uint32_t size,
                               MemPool* mem_pool) override;

    virtual size_t get_buffer_size() override { return sizeof(RunLengthByteReader) * 2; }

private:
    bool _eof;
    decimal12_t* _values;
    RunLengthIntegerReader* _int_reader;
    RunLengthIntegerReader* _frac_reader;
};

class LargeIntColumnReader : public ColumnReader {
public:
    LargeIntColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~LargeIntColumnReader();
    virtual Status init(std::map<StreamName, ReadOnlyFileStream*>* streams, int size,
                        MemPool* mem_pool, OlapReaderStatistics* stats);
    virtual Status seek(PositionProvider* positions);
    virtual Status skip(uint64_t row_count);
    virtual Status next_vector(ColumnVector* column_vector, uint32_t size, MemPool* mem_pool);

    virtual size_t get_buffer_size() { return sizeof(RunLengthByteReader) * 2; }

private:
    bool _eof;
    int128_t* _values;
    RunLengthIntegerReader* _high_reader;
    RunLengthIntegerReader* _low_reader;
};

typedef FloatintPointColumnReader<float> FloatColumnReader;
typedef FloatintPointColumnReader<double> DoubleColumnReader;
typedef IntegerColumnReaderWrapper<int64_t, true> DiscreteDoubleColumnReader;

// Use 3 bytes to store the date
// Use IntegerColumnReader, truncated to 3 bytes length when returning data
typedef IntegerColumnReaderWrapper<uint24_t, false> DateColumnReader;

// Internal use LONG implementation
typedef IntegerColumnReaderWrapper<uint64_t, false> DateTimeColumnReader;

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_COLUMN_READER_H
