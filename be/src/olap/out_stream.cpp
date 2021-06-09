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

#include "olap/out_stream.h"

#include "olap/byte_buffer.h"
#include "olap/file_helper.h"
#include "olap/utils.h"
#include "util/mem_util.hpp"
#include "util/monotime.h"

namespace doris {

OutStreamFactory::OutStreamFactory(CompressKind compress_kind, uint32_t stream_buffer_size)
        : _compress_kind(compress_kind), _stream_buffer_size(stream_buffer_size) {
    switch (compress_kind) {
    case COMPRESS_NONE:
        _compressor = NULL;
        break;

#ifdef DORIS_WITH_LZO
    case COMPRESS_LZO:
        _compressor = lzo_compress;
        break;
#endif

    case COMPRESS_LZ4:
        _compressor = lz4_compress;
        break;

    default:
        LOG(FATAL) << "unknown compress kind. kind=" << compress_kind;
    }
}

OutStreamFactory::~OutStreamFactory() {
    for (std::map<StreamName, OutStream*>::iterator it = _streams.begin(); it != _streams.end();
         ++it) {
        SAFE_DELETE(it->second);
    }
}

OutStream* OutStreamFactory::create_stream(uint32_t column_unique_id,
                                           StreamInfoMessage::Kind kind) {
    OutStream* stream = NULL;

    if (StreamInfoMessage::ROW_INDEX == kind || StreamInfoMessage::BLOOM_FILTER == kind) {
        stream = new (std::nothrow) OutStream(_stream_buffer_size, NULL);
    } else {
        stream = new (std::nothrow) OutStream(_stream_buffer_size, _compressor);
    }

    if (NULL == stream) {
        OLAP_LOG_WARNING("fail to allocate OutStream.");
        return NULL;
    }

    StreamName stream_name(column_unique_id, kind);
    _streams[stream_name] = stream;
    return stream;
}

OutStream::OutStream(uint32_t buffer_size, Compressor compressor)
        : _buffer_size(buffer_size),
          _compressor(compressor),
          _is_suppressed(false),
          _current(NULL),
          _compressed(NULL),
          _overflow(NULL),
          _spilled_bytes(0) {}

OutStream::~OutStream() {
    SAFE_DELETE(_current);
    SAFE_DELETE(_compressed);
    SAFE_DELETE(_overflow);

    for (std::vector<StorageByteBuffer*>::iterator it = _output_buffers.begin();
         it != _output_buffers.end(); ++it) {
        SAFE_DELETE(*it);
    }
}

OLAPStatus OutStream::_create_new_input_buffer() {
    SAFE_DELETE(_current);
    _current = StorageByteBuffer::create(_buffer_size + sizeof(StreamHead));

    if (NULL != _current) {
        _current->set_position(sizeof(StreamHead));
        return OLAP_SUCCESS;
    } else {
        return OLAP_ERR_MALLOC_ERROR;
    }
}

OLAPStatus OutStream::_write_head(StorageByteBuffer* buf, uint64_t position,
                                  StreamHead::StreamType type, uint32_t length) {
    if (buf->limit() < sizeof(StreamHead) + length) {
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    StreamHead* head = reinterpret_cast<StreamHead*>(&(buf->array()[position]));
    head->type = type;
    head->length = length;
    head->checksum = 0;
    return OLAP_SUCCESS;
}

OLAPStatus OutStream::_compress(StorageByteBuffer* input, StorageByteBuffer* output,
                                StorageByteBuffer* overflow, bool* smaller) {
    OLAPStatus res = OLAP_SUCCESS;

    res = _compressor(input, overflow, smaller);

    if (OLAP_SUCCESS == res && *smaller) {
        if (output->remaining() >= overflow->position()) {
            memory_copy(&(output->array()[output->position()]), overflow->array(),
                        overflow->position());
            output->set_position(output->position() + overflow->position());
            overflow->set_position(0);
        } else if (0 != output->remaining()) {
            uint64_t to_copy = output->remaining();
            memory_copy(&(output->array()[output->position()]), overflow->array(), to_copy);
            output->set_position(output->limit());

            memmove(overflow->array(), &(overflow->array()[to_copy]),
                    overflow->position() - to_copy);
            overflow->set_position(overflow->position() - to_copy);
        }
    }

    return OLAP_SUCCESS;
}

void OutStream::_output_uncompress() {
    _spilled_bytes += _current->limit();
    _write_head(_current, 0, StreamHead::UNCOMPRESSED, _current->limit() - sizeof(StreamHead));
    _output_buffers.push_back(_current);
    _current = NULL;
}

void OutStream::_output_compressed() {
    _compressed->flip();
    _output_buffers.push_back(_compressed);
    _compressed = _overflow;
    _overflow = NULL;
}

OLAPStatus OutStream::_make_sure_output_buffer() {
    if (NULL == _compressed) {
        _compressed = StorageByteBuffer::create(_buffer_size + sizeof(StreamHead));

        if (NULL == _compressed) {
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    if (NULL == _overflow) {
        _overflow = StorageByteBuffer::create(_buffer_size + sizeof(StreamHead));

        if (NULL == _overflow) {
            return OLAP_ERR_MALLOC_ERROR;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OutStream::_spill() {
    OLAPStatus res = OLAP_SUCCESS;

    if (_current == NULL || _current->position() == sizeof(StreamHead)) {
        return OLAP_SUCCESS;
    }

    // 如果不压缩，直接读取current，注意output之后 current会被清空并设置为NULL
    if (_compressor == NULL) {
        _current->flip();
        _output_uncompress();
    } else {
        // 如果需要压缩，
        // current移动到head后边的位置，留出head的空间
        _current->set_limit(_current->position());
        _current->set_position(sizeof(StreamHead));

        // 分配compress和overflow，这两个buffer大小其实是一样的
        if (OLAP_SUCCESS != (res = _make_sure_output_buffer())) {
            return res;
        }

        // 吧 current解压到compress和overflow
        uint64_t head_pos = _compressed->position();
        _compressed->set_position(head_pos + sizeof(StreamHead));
        bool smaller = false;
        res = _compress(_current, _compressed, _overflow, &smaller);

        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to compress data.");
            return OLAP_ERR_COMPRESS_ERROR;
        }

        if (smaller) {
            // 数据都压缩到_output和_overflow里, 重置_current
            // 注意这种情况下，current并没有被释放，因为实际上输出的compress
            _current->set_position(sizeof(StreamHead));
            _current->set_limit(_current->capacity());

            uint32_t output_bytes = _compressed->position() - head_pos - sizeof(StreamHead);
            output_bytes += _overflow->position();
            _write_head(_compressed, head_pos, StreamHead::COMPRESSED, output_bytes);

            if (_compressed->remaining() < sizeof(StreamHead)) {
                _output_compressed();
            }

            _spilled_bytes += sizeof(StreamHead) + output_bytes;
        } else {
            // 直接将_current输出

            // 如果之前还有_compress, 先输出m_compress
            // 注意此时一定没有_overflow
            _compressed->set_position(head_pos);

            if (head_pos != 0) {
                // 之前_compressed里有数据, 这种情况下先输出compressed,
                // 此时_overflow一定是空的
                _output_compressed();
            }

            _output_uncompress();
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OutStream::write(const char* buffer, uint64_t length) {
    OLAPStatus res = OLAP_SUCCESS;
    uint64_t offset = 0;
    uint64_t remain = length;

    while (remain > 0) {
        // 之所以扔进来，是因为在压缩的情况下，_current只会被创建一次
        // 之后一直在复用，输出的是compress
        // 而在未压缩的情况下，current会被放进列表，而无法复用，原因是
        // 如果复用的话，会修改之前的内容，因此需要重新分配。
        // 只分配一次那么第二块就会挂掉
        if (NULL == _current) {
            res = _create_new_input_buffer();
            if (OLAP_SUCCESS != res) {
                return res;
            }
        }

        uint64_t to_put = std::min(_current->remaining(), remain);

        if (OLAP_LIKELY(0 != to_put)) {
            res = _current->put(&buffer[offset], to_put);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to put buffer.");
                return res;
            }

            offset += to_put;
            remain -= to_put;
        }

        if (_current->remaining() == 0) {
            res = _spill();
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to spill current buffer.");
                return res;
            }
        }
    }

    return OLAP_SUCCESS;
}

void OutStream::get_position(PositionEntryWriter* index_entry) const {
    index_entry->add_position(_spilled_bytes);

    if (NULL != _current) {
        index_entry->add_position(_current->position() - sizeof(StreamHead));
    } else {
        index_entry->add_position(0);
    }
}

uint64_t OutStream::get_stream_length() const {
    uint64_t result = 0;

    for (std::vector<StorageByteBuffer*>::const_iterator it = _output_buffers.begin();
         it != _output_buffers.end(); ++it) {
        result += (*it)->limit();
    }

    return result;
}

uint64_t OutStream::get_total_buffer_size() const {
    uint64_t result = 0;

    for (std::vector<StorageByteBuffer*>::const_iterator it = _output_buffers.begin();
         it != _output_buffers.end(); ++it) {
        result += (*it)->capacity();
    }

    if (_current) {
        result += _current->capacity();
    }

    if (_compressed) {
        result += _compressed->capacity();
    }

    if (_overflow) {
        result += _overflow->capacity();
    }

    return result;
}

OLAPStatus OutStream::write_to_file(FileHandler* file_handle, uint32_t write_mbytes_per_sec) const {
    OLAPStatus res = OLAP_SUCCESS;

    uint64_t total_stream_len = 0;
    OlapStopWatch speed_limit_watch;

    speed_limit_watch.reset();

    for (std::vector<StorageByteBuffer*>::const_iterator it = _output_buffers.begin();
         it != _output_buffers.end(); ++it) {
        VLOG_TRACE << "write stream begin:" << file_handle->tell();

        res = file_handle->write((*it)->array(), (*it)->limit());
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to write stream to fail.");
            return res;
        }

        VLOG_TRACE << "write stream end:" << file_handle->tell();

        total_stream_len += (*it)->limit();
        if (write_mbytes_per_sec > 0) {
            uint64_t delta_time_us = speed_limit_watch.get_elapse_time_us();
            int64_t sleep_time = total_stream_len / write_mbytes_per_sec - delta_time_us;
            if (sleep_time > 0) {
                VLOG_TRACE << "sleep to limit merge speed. time=" << sleep_time
                         << ", bytes=" << total_stream_len;
                SleepFor(MonoDelta::FromMicroseconds(sleep_time));
            }
        }
    }

    return res;
}

OLAPStatus OutStream::flush() {
    OLAPStatus res = OLAP_SUCCESS;

    res = _spill();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to spill stream.");
        return res;
    }

    if (NULL != _compressed && 0 != _compressed->position()) {
        _output_compressed();
        SAFE_DELETE(_compressed);
    }

    SAFE_DELETE(_current);
    SAFE_DELETE(_overflow);

    return res;
}

uint32_t OutStream::crc32(uint32_t checksum) const {
    uint32_t result = CRC32_INIT;

    for (std::vector<StorageByteBuffer*>::const_iterator it = _output_buffers.begin();
         it != _output_buffers.end(); ++it) {
        result = olap_crc32(result, (*it)->array(), (*it)->limit());
    }

    return result;
}

} // namespace doris
