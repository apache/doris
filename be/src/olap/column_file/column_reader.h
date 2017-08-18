// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_READER_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_READER_H

#include "olap/column_file/byte_buffer.h"
#include "olap/column_file/file_stream.h"
#include "olap/column_file/run_length_byte_reader.h"
#include "olap/column_file/run_length_integer_reader.h"
#include "olap/column_file/stream_name.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"

namespace palo {
namespace column_file {

class StreamName;
class ReadOnlyFileStream;
class BitFieldReader;
class RowIndexEntryMessage;
class ColumnEncodingMessage;

// 解出流
inline ReadOnlyFileStream* extract_stream(uint32_t column_unique_id,
        StreamInfoMessage::Kind kind,
        std::map<StreamName, ReadOnlyFileStream*>* streams) {
    //OLAP_LOG_DEBUG("try to get stream: id=%u, kind=%d", column_unique_id, kind);
    StreamName stream_name(column_unique_id, kind);
    std::map<StreamName, ReadOnlyFileStream*>::iterator it = streams->find(stream_name);

    if (streams->end() != it) {
        return (*it).second;
    }

    return NULL;
}

// Unique id -> PositionProvider
typedef std::unordered_map<uint32_t, PositionProvider> UniqueIdPositionProviderMap;
// Unqiue id -> ColumnEncodingMessage
typedef std::map<uint32_t, ColumnEncodingMessage> UniqueIdEncodingMap;

// Integer和String的读取器。
// 这些读取器虽然也冠有Reader之名，但注意并不从ColumnReader继承
// 因此不考虑空值的情况。

// 对于SHORT/INT/LONG类型的数据，统一使用int64作为存储的数据
// 由于使用变长编码，所以不会造成浪费
// IntegerColumnReader是读取底层int64数据的reader，上层返回时
// 使用IntColumnReaderWrapper转为具体的数据类型
//
// NOTE. 由于RLE读取器只能读int64，这点和java不同，java整形是不考虑符号的
// 那么这东西实际上似乎是�
// ��法支持无符号整形的，需要注意后续是否修改RLEReader
class IntegerColumnReader {
public:
    IntegerColumnReader(uint32_t column_unique_id);
    ~IntegerColumnReader();
    /**
     * 初始化Integer列读取器
     * @param  streams 包含所需要流的map
     * @param  is_sign 所读取的数是否有符号
     * @return         [description]
     */
    OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams, bool is_sign);
    // 将内部指针定位到positions
    OLAPStatus seek(PositionProvider* positions);
    // 将内部指针向后移动row_count行
    OLAPStatus skip(uint64_t row_count);
    // 返回当前行的数据，通过将内部指针移向下一行
    OLAPStatus next(int64_t* value);
    bool eof() {
        return _eof;
    }
private:
    bool _eof;
    uint32_t _column_unique_id;
    RunLengthIntegerReader* _data_reader;
};

// 对于使用Direct方式编码的字符串列的读取器
// Direct方式的String直接读取即可
class StringColumnDirectReader {
public:
    StringColumnDirectReader(uint32_t column_unique_id, uint32_t dictionary_size);
    ~StringColumnDirectReader();

    OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);
    OLAPStatus seek(PositionProvider* positions);
    OLAPStatus skip(uint64_t row_count);
    // 返回当前行的数据，并将内部指针向后移动
    // buffer - 返回数据的缓冲区
    // length - 输入时作为缓存区大小，返回时给出字符串的大小
    OLAPStatus next(char* buffer, uint32_t* length);

    size_t get_buffer_size() {
        return sizeof(RunLengthByteReader);
    }

private:
    bool _eof;
    uint32_t _column_unique_id;
    ReadOnlyFileStream* _data_stream;
    RunLengthIntegerReader* _length_reader;
};

// 对于使用字典编码的字符串列的读取器
// 接口同StringColumnDirectReader
// 读取的流程：
// 1. 读取全部的字典数据，保存在整块buffer中
// 2. 读取length数据，构造偏移字典，偏移是�
// ��每个string的起始，与1组合能够读取数据
// 3. 需要时读取实际保存的数据�
// ��是一个int）。根据这个int找出偏移，再根据偏移读出字典数据
class StringColumnDictionaryReader {
public:
    StringColumnDictionaryReader(uint32_t column_unique_id, uint32_t dictionary_size);
    ~StringColumnDictionaryReader();
    OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);
    OLAPStatus seek(PositionProvider* positions);
    OLAPStatus skip(uint64_t row_count);
    OLAPStatus next(char* buffer, uint32_t* length);

    size_t get_buffer_size() {
        return sizeof(RunLengthByteReader) + _dictionary_size;
    }

private:
    bool _eof;
    uint32_t _dictionary_size;
    uint32_t _column_unique_id;
    char* _read_buffer;
    //uint64_t _dictionary_size;
    //uint64_t* _offset_dictionary;   // 用来查找响应数据的数字对应的offset
    //ByteBuffer* _dictionary_data_buffer;   // 保存dict数据
    std::vector<std::string> _dictionary;
    RunLengthIntegerReader* _data_reader;   // 用来读实际的数据（用一个integer表示）
};

// ColumnReader用于读取一个列, 是其他XXXColumnReader的基类
// ColumnReader通过present的bit field维护了列的NULL特性
class ColumnReader {
public:
    // 工厂方法, 创建ColumnReader, 如果列有子列, 递归创建sub reader
    // 如果需要读取的列在segment_columns中不存在, 则:
    //     1. 如果列允许Null值, 则创建一个NullValueReader
    //     2. 如果列不允许Null值, 但有默认值, 则创建一个DefaultValueReader
    //     3. 否则创建失败
    // Input:
    //     column_id - 需要创建的列在columns中的位置
    //     columns - 表的schema
    //     included - 需要创建的列, 如果某列的unique id在included中则创建
    //     segment_columns - segment中所有column的unique id组成的集合
    //     encodings - 列的编码信息, 使用encodings[_column_unique_id]访问
    static ColumnReader* create(uint32_t column_id,
            const std::vector<FieldInfo>& columns,
            const UniqueIdToColumnIdMap& included,
            UniqueIdToColumnIdMap& segment_included,
            const UniqueIdEncodingMap& encodings);

    ColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~ColumnReader();

    // 使用streams初始化Reader
    // ColumnReader仅初始化一次，每次使用时分配新的对象。
    // Input:
    //     streams - 输入stream
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);

    // 将内部数据attach到cursor, ColumnReader必须将需要返回的数据缓存在内部
    // 的data_buffer, 再通过cursor的attach_by_index返回该数据
    // 即执行cursor->attach_by_index(_column_id, data_buffer);
    // Reader无序attach，因为reader只用来判断当前value是不是为空
    virtual OLAPStatus attach(RowCursor* cursor) = 0;

    // 设置下一个返回的数据的位置
    // positions是各个列需要seek的位置, ColumnReader通过(*positions)[_column_unique_id]
    // 获得本列需要seek的位置
    virtual OLAPStatus seek(PositionProvider* positions);

    // TODO. 这点不是很明白，为什么present不用skip，
    // 如果上层skip过而底层不skip，next判断空不空不是不准了吗
    virtual OLAPStatus skip(uint64_t row_count);

    // next会将变量读取至_value_present
    // 此变量其实本身不会被外部使用，而是配合其他列类型，
    // 来提供 空/非空 指示
    virtual OLAPStatus next();

    // get vector for row batch
    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        return OLAP_SUCCESS;
    }

    uint32_t column_unique_id() {
        return _column_unique_id;
    }

    uint32_t column_id() {
        return _column_id;
    }

    virtual size_t get_buffer_size() {
        return 0;
    }

protected:
    // NOTE. 统计rows中的非空行。这是因为实际存储中，“空行”并不存在，
    // 所以对于可能为空的上层字段（例如integer），调用者希望跳过10行，
    // 但实际上对于
    uint64_t _count_none_nulls(uint64_t rows);

    bool _value_present;
    uint32_t _column_id;        // column在schema内的id
    uint32_t _column_unique_id; // column的唯一id
    BitFieldReader* _present_reader;   // NULLable的字段的NULL值
    std::vector<ColumnReader*> _sub_readers;
};

class DefaultValueReader : public ColumnReader {
public:
    DefaultValueReader(uint32_t column_id, uint32_t column_unique_id, std::string default_value) :
        ColumnReader(column_id, column_unique_id),
        _default_value(default_value) {
    }
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus attach(RowCursor* cursor) {
        cursor->get_mutable_field_by_index(_column_id)->from_string(_default_value.c_str());
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus seek(PositionProvider* positions) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus next() {
        return OLAP_SUCCESS;
    }
private:
    std::string _default_value;
};

class NullValueReader : public ColumnReader {
public:
    NullValueReader(uint32_t column_id, uint32_t column_unique_id) :
        ColumnReader(column_id, column_unique_id) {
    }
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus attach(RowCursor* cursor) {
        OLAPStatus res = cursor->set_null(_column_id);  
        return res;
    }
    virtual OLAPStatus seek(PositionProvider* positions) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        return OLAP_SUCCESS;
    }
    virtual OLAPStatus next() {
        _value_present = true;
        return OLAP_SUCCESS;
    }
};

// 对于Tiny类型, 使用Byte作为存储
class TinyColumnReader : public ColumnReader {
public:
    TinyColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~TinyColumnReader();

    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);
    virtual OLAPStatus attach(RowCursor* cursor) {
        OLAPStatus res;
        if (true == _value_present) {
            res = cursor->set_null(_column_id);
        } else {
            res = cursor->attach_by_index(_column_id, &_value, false);
        }
        return res;
    }
    virtual OLAPStatus seek(PositionProvider* positions);
    virtual OLAPStatus skip(uint64_t row_count);
    virtual OLAPStatus next();

    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        char* return_value = reinterpret_cast<char*>(batch_buf + offset.front());

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            OLAPStatus res = ColumnReader::next();

            if (OLAP_SUCCESS == res) {
                if (false == _value_present) {
                    _data_reader->next(return_value);
                } else {
                    *return_value = 0;
                }

                return_value++;
            }

            if (OLAP_ERR_DATA_EOF == res) {
                _eof = true;
            }
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return sizeof(RunLengthByteReader);
    }

private:
    bool _eof;
    char _value;
    RunLengthByteReader* _data_reader;
};

// IntColumnReader的包裹器, 实现了对ColumnReader的接口
template<class T, bool is_sign>
class IntegerColumnReaderWrapper : public ColumnReader {
public:
    IntegerColumnReaderWrapper(uint32_t column_id, uint32_t column_unique_id) :
        ColumnReader(column_id, column_unique_id),
        _reader(column_unique_id),
        _eof(false) {
    }

    virtual ~IntegerColumnReaderWrapper() {}

    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        OLAPStatus res = ColumnReader::init(streams);

        if (OLAP_SUCCESS == res) {
            res = _reader.init(streams, is_sign);
        }

        return res;
    }
    virtual OLAPStatus attach(RowCursor* cursor) {
        OLAPStatus res;
        if (true == _value_present) {
            res = cursor->set_null(_column_id);
        } else {
            res = cursor->attach_by_index(_column_id, reinterpret_cast<char*>(&_value), false);
        }
        return res;
    }

    virtual OLAPStatus seek(PositionProvider* positions) {
        OLAPStatus res;
        if (NULL == _present_reader) {
            res = _reader.seek(positions);
            if (OLAP_SUCCESS != res) {
                return res;
            }
        } else {
            //all field in the segment can be NULL, so the data stream is EOF
            res = ColumnReader::seek(positions);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            res = _reader.seek(positions);
            if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
                OLAP_LOG_WARNING("fail to seek int stream. [res=%d]", res);
                return res;
            }
        }

        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        return _reader.skip(_count_none_nulls(row_count));
    }
    virtual OLAPStatus next() {
        OLAPStatus res = ColumnReader::next();

        if (OLAP_SUCCESS == res) {
            if (false == _value_present) {
                int64_t value = 0;
                res = _reader.next(&value);
                _value = value;
            } else {
                _value = 0;
            }
        }

        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }

        return res;
    }

    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        T* return_value = reinterpret_cast<T*>(batch_buf + offset.front());

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            OLAPStatus res = ColumnReader::next();

            if (OLAP_SUCCESS == res) {
                if (false == _value_present) {
                    int64_t value = 0;
                    res = _reader.next(&value);
                    *return_value = value;
                } else {
                    *return_value = 0;
                }

                return_value++;
            }

            if (OLAP_ERR_DATA_EOF == res) {
                _eof = true;
            }
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return sizeof(RunLengthIntegerReader);
    }

private:
    IntegerColumnReader _reader;  // 被包裹的真实读取器
    T _value;                     // 当前行读出的值
    bool _eof;
};

// OLAP Engine中有两类字符串，定长字符串和变长字符串，分别使用两个Wrapper
// class 处理对这两种字符串的返回格式
// FixLengthStringColumnReader 处理定长字符串，特点是不足长度的部分要补0
template <class ReaderClass>
class FixLengthStringColumnReader : public ColumnReader {
public:
    FixLengthStringColumnReader(uint32_t column_id,
            uint32_t column_unique_id,
            uint32_t string_length,
            uint32_t dictionary_size) :
            ColumnReader(column_id, column_unique_id),
            _buf(NULL),
            _reader(column_unique_id, dictionary_size),
            _string_length(string_length) {
    }
    virtual ~FixLengthStringColumnReader() {
        SAFE_DELETE_ARRAY(_buf);
    }

    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        _buf = new(std::nothrow) char [_string_length];

        if (NULL == _buf) {
            return OLAP_ERR_MALLOC_ERROR;
        }

        OLAPStatus res = ColumnReader::init(streams);

        if (OLAP_SUCCESS == res) {
            res = _reader.init(streams);
        }

        return res;
    }

    virtual OLAPStatus attach(RowCursor* cursor) {
        OLAPStatus res;
        if (true == _value_present) {
            res = cursor->set_null(_column_id);
        } else {
            res = cursor->attach_by_index(_column_id, _buf, false);
        }
        return res;
    }

    virtual OLAPStatus seek(PositionProvider* positions) {
        OLAPStatus res;
        if (NULL == _present_reader) {
            res = _reader.seek(positions);
            if (OLAP_SUCCESS != res) {
                return res;
            }
        } else {
            //all field in the segment can be NULL, so the data stream is EOF
            res = ColumnReader::seek(positions);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            res = _reader.seek(positions);
            if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
                OLAP_LOG_WARNING("fail to read fixed string stream. [res=%d]", res);
                return res;
            }
        }

        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        return _reader.skip(_count_none_nulls(row_count));
    }
    virtual OLAPStatus next() {
        uint32_t buf_size = _string_length;
        OLAPStatus res = ColumnReader::next();

        if (OLAP_SUCCESS == res) {
            if (false == _value_present) {
                res = _reader.next(_buf, &buf_size);
            } else {
                memset(_buf, 0, buf_size);
            }
        }

        // 将多余的buffer设置为0
        if (OLAP_SUCCESS == res) {
            memset(&_buf[buf_size], 0, _string_length - buf_size);
        }

        return res;
    }

    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        OLAPStatus res = OLAP_SUCCESS;
        char* return_value = reinterpret_cast<char*>(batch_buf + offset.front());

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            res = this->next();

            if (OLAP_SUCCESS == res) {
                memcpy(return_value, _buf, _string_length);
            } else if (OLAP_ERR_COLUMN_STREAM_EOF == res) {
                memcpy(return_value, _buf, _string_length);
                break;
            } else {
                OLAP_LOG_WARNING("fail to get next. [res=%d]", res);
                return res;
            }
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return _reader.get_buffer_size() + _string_length;
    }

private:
    char* _buf;
    ReaderClass _reader;
    uint32_t _string_length;
};

// VarStringColumnReader 处理变长长字符串，特点是在数据头部使用uint16表示长度
template <class ReaderClass>
class VarStringColumnReader : public ColumnReader {
public:
    VarStringColumnReader(
            uint32_t column_id,
            uint32_t column_unique_id,
            uint32_t max_length,
            uint32_t dictionary_size) :
            ColumnReader(column_id, column_unique_id),
            _buf(NULL),
            _reader(column_unique_id, dictionary_size),
            _max_length(max_length),
            _real_length(NULL) {
    }
    virtual ~VarStringColumnReader() {
        SAFE_DELETE_ARRAY(_buf);
    }
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        OLAPStatus res = ColumnReader::init(streams);
        if (OLAP_SUCCESS == res) {
            res = _reader.init(streams);
        }

        _buf = new(std::nothrow) char[_max_length];
        if (NULL == _buf) {
            OLAP_LOG_WARNING("fail to malloc buffer. [size=%u]", _max_length);
            res = OLAP_ERR_MALLOC_ERROR;
        }

        _real_length = reinterpret_cast<VarCharField::LengthValueType*>(_buf);
        return res;
    }

    virtual OLAPStatus attach(RowCursor* cursor) {
        if (true == _value_present) {
            cursor->set_null(_column_id);
        }
        OLAPStatus res = cursor->attach_by_index(_column_id, _buf, false);
        return res;
    }

    virtual OLAPStatus seek(PositionProvider* position) {
        OLAPStatus res;
        if (NULL == _present_reader) {
            res = _reader.seek(position);
            if (OLAP_SUCCESS != res) {
                return res;
            }
        } else {
            //all field in the segment can be NULL, so the data stream is EOF
            res = ColumnReader::seek(position);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            res = _reader.seek(position);
            if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
                OLAP_LOG_WARNING("fail to seek varchar stream. [res=%d]", res);
                return res;
            }
        }

        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        return _reader.skip(_count_none_nulls(row_count));
    }
    virtual OLAPStatus next() {
        uint32_t buf_size = 0;
        *_real_length = 0;

        OLAPStatus res = ColumnReader::next();
        if (OLAP_LIKELY(OLAP_SUCCESS == res)) {
            if (false == _value_present) {
                res = _reader.next(_buf + sizeof(VarCharField::LengthValueType), &buf_size);
                if (OLAP_LIKELY(OLAP_SUCCESS == res)) {
                    *_real_length = static_cast<uint16_t>(buf_size);
                }
            } else {
                *_real_length = 0;
                memset(_buf, 0, sizeof(VarCharField::LengthValueType));
                _buf[sizeof(VarCharField::LengthValueType)] = '\0';
            }
        }

        return res;
    }
    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        OLAPStatus res = OLAP_SUCCESS;
        VarCharField::OffsetValueType* offset_value =
            reinterpret_cast<VarCharField::OffsetValueType*>(batch_buf + offset[0]);
        char* string_value = reinterpret_cast<char*>(batch_buf + offset[1]);
        uint32_t start_offset = offset[1];

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            res = this->next();
            if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_VALUE_NULL != res) {
                OLAP_LOG_WARNING("fail to get next. [res=%d]", res);
                return res;
            }

            *offset_value = start_offset;
            offset_value++;
            uint32_t string_value_len = *_real_length + sizeof(VarCharField::LengthValueType);
            memcpy(string_value, _buf, string_value_len);
            string_value += string_value_len;
            start_offset += string_value_len;
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return _reader.get_buffer_size() + _max_length;
    }

private:
    char* _buf;
    ReaderClass _reader;
    uint32_t _max_length;
    VarCharField::LengthValueType* _real_length;
};

template <typename FLOAT_TYPE>
class FloatintPointColumnReader : public ColumnReader {
public:
    FloatintPointColumnReader(uint32_t column_id, uint32_t column_unique_id) :
            ColumnReader(column_id, column_unique_id),
            _eof(false),
            _data_stream(NULL) {
    }

    virtual ~FloatintPointColumnReader() {}

    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams) {
        if (NULL == streams) {
            OLAP_LOG_WARNING("input streams is NULL");
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        // reset stream and reader
        ColumnReader::init(streams);
        _data_stream = extract_stream(_column_unique_id,
                       StreamInfoMessage::DATA,
                       streams);

        if (NULL == _data_stream) {
            OLAP_LOG_WARNING("specified stream not exist");
            return OLAP_ERR_COLUMN_STREAM_NOT_EXIST;
        }

        return OLAP_SUCCESS;
    }
    virtual OLAPStatus attach(RowCursor* cursor) {        
        OLAPStatus res;
        if (true == _value_present) {
            res = cursor->set_null(_column_id);
        } else {
            res = cursor->attach_by_index(_column_id,  reinterpret_cast<char*>(&_value), false);
        }
        return res;
    }

    virtual OLAPStatus seek(PositionProvider* position) {
        if (NULL == position) {
            OLAP_LOG_WARNING("input positions is NULL");
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        if (NULL == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return OLAP_ERR_NOT_INITED;
        }

        OLAPStatus res;
        if (NULL == _present_reader) {
            res = _data_stream->seek(position);
            if (OLAP_SUCCESS != res) {
                return res;
            }
        } else {
            //all field in the segment can be NULL, so the data stream is EOF
            res = ColumnReader::seek(position);
            if (OLAP_SUCCESS != res) {
                return res;
            }
            res = _data_stream->seek(position);
            if (OLAP_SUCCESS != res && OLAP_ERR_COLUMN_STREAM_EOF != res) {
                OLAP_LOG_WARNING("fail to seek float stream. [res=%d]", res);
                return res;
            }
        }

        return OLAP_SUCCESS;
    }
    virtual OLAPStatus skip(uint64_t row_count) {
        if (NULL == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return OLAP_ERR_NOT_INITED;
        }

        uint64_t skip_values_count = _count_none_nulls(row_count);
        return _data_stream->skip(skip_values_count * sizeof(FLOAT_TYPE));
    }

    virtual OLAPStatus next() {
        if (NULL == _data_stream) {
            OLAP_LOG_WARNING("reader not init.");
            return OLAP_ERR_NOT_INITED;
        }

        OLAPStatus res = ColumnReader::next();

        if (OLAP_SUCCESS == res) {
            if (false == _value_present) {
                size_t length = sizeof(_value);
                res = _data_stream->read(reinterpret_cast<char*>(&_value), &length);
            } else {
                _value = 0.0;
            }
        }

        if (OLAP_ERR_DATA_EOF == res) {
            _eof = true;
        }

        return res;
    }

    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        char* return_value = reinterpret_cast<char*>(batch_buf + offset.front());

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            OLAPStatus res = ColumnReader::next();

            if (OLAP_SUCCESS == res) {
                if (false == _value_present) {
                    size_t length = sizeof(FLOAT_TYPE);
                    res = _data_stream->read(return_value, &length);
                } else {
                    *reinterpret_cast<FLOAT_TYPE*>(return_value) = 0.0;
                }

                return_value += sizeof(FLOAT_TYPE);
            }

            if (OLAP_ERR_DATA_EOF == res) {
                _eof = true;
            }
        }

        return OLAP_SUCCESS;
    }

protected:
    bool _eof;
    ReadOnlyFileStream* _data_stream;
    FLOAT_TYPE _value;
};

class DecimalColumnReader : public ColumnReader {
public:
    DecimalColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~DecimalColumnReader();
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);
    virtual OLAPStatus attach(RowCursor* cursor);
    virtual OLAPStatus seek(PositionProvider* positions);
    virtual OLAPStatus skip(uint64_t row_count);
    virtual OLAPStatus next();
    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        OLAPStatus res = OLAP_SUCCESS;
        DecimalBuf* return_value =
            reinterpret_cast<DecimalBuf*>(batch_buf + offset[0]);

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            res = this->next();

            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to get next.[res=%d]", res);
                return res;
            }

            *return_value = _value;
            return_value++;
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return sizeof(RunLengthByteReader) * 2;
    }

private:
    struct DecimalBuf {
        int64_t _int;
        int32_t _frac;     // 最大64K
    } __attribute__((packed));
    DecimalBuf _value;
    RunLengthIntegerReader* _int_reader;
    RunLengthIntegerReader* _frac_reader;
};

class LargeIntColumnReader : public ColumnReader {
public:
    LargeIntColumnReader(uint32_t column_id, uint32_t column_unique_id);
    virtual ~LargeIntColumnReader();
    virtual OLAPStatus init(std::map<StreamName, ReadOnlyFileStream*>* streams);
    virtual OLAPStatus attach(RowCursor* cursor);
    virtual OLAPStatus seek(PositionProvider* positions);
    virtual OLAPStatus skip(uint64_t row_count);
    virtual OLAPStatus next();
    virtual OLAPStatus next_vector(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t start_row_in_block,
            uint32_t batch_size,
            std::vector<uint32_t>& offset) {
        OLAPStatus res = OLAP_SUCCESS;
        int128_t* return_value =
            reinterpret_cast<int128_t*>(batch_buf + offset[0]);

        for (uint32_t i = start_row_in_block; i < batch_size; i++) {
            res = this->next();

            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to get next.[res=%d]", res);
                return res;
            }

            *return_value = _value;
            ++return_value;
        }

        return OLAP_SUCCESS;
    }

    virtual size_t get_buffer_size() {
        return sizeof(RunLengthByteReader) * 2;
    }

private:
    int128_t _value;
    RunLengthIntegerReader* _high_reader;
    RunLengthIntegerReader* _low_reader;
};

typedef FloatintPointColumnReader<float> FloatColumnReader;
typedef FloatintPointColumnReader<double> DoubleColumnReader;
typedef IntegerColumnReaderWrapper<int64_t, true> DiscreteDoubleColumnReader;

// 使用3个字节存储的日期
// 使用IntegerColumnReader，在返回数据时截断到3字节长度
typedef IntegerColumnReaderWrapper<uint24_t, false> DateColumnReader;

// 内部使用LONG实现
typedef IntegerColumnReaderWrapper<uint64_t, false> DateTimeColumnReader;

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_READER_H
