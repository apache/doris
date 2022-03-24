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

#include "olap/file_stream.h"

#include "olap/byte_buffer.h"
#include "olap/out_stream.h"

namespace doris {

ReadOnlyFileStream::ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer,
                                       Decompressor decompressor, uint32_t compress_buffer_size,
                                       OlapReaderStatistics* stats)
        : _file_cursor(handler, 0, 0),
          _compressed_helper(nullptr),
          _uncompressed(nullptr),
          _shared_buffer(shared_buffer),
          _decompressor(decompressor),
          _compress_buffer_size(compress_buffer_size + sizeof(StreamHead)),
          _current_compress_position(std::numeric_limits<uint64_t>::max()),
          _stats(stats) {}

ReadOnlyFileStream::ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer,
                                       uint64_t offset, uint64_t length, Decompressor decompressor,
                                       uint32_t compress_buffer_size, OlapReaderStatistics* stats)
        : _file_cursor(handler, offset, length),
          _compressed_helper(nullptr),
          _uncompressed(nullptr),
          _shared_buffer(shared_buffer),
          _decompressor(decompressor),
          _compress_buffer_size(compress_buffer_size + sizeof(StreamHead)),
          _current_compress_position(std::numeric_limits<uint64_t>::max()),
          _stats(stats) {}

OLAPStatus ReadOnlyFileStream::_assure_data() {
    // if still has data in uncompressed
    if (OLAP_LIKELY(_uncompressed != nullptr && _uncompressed->remaining() > 0)) {
        return OLAP_SUCCESS;
    } else if (_file_cursor.eof()) {
        VLOG_TRACE << "STREAM EOF. length=" << _file_cursor.length()
                   << ", used=" << _file_cursor.position();
        return OLAP_ERR_COLUMN_STREAM_EOF;
    }

    StreamHead header;
    size_t file_cursor_used = _file_cursor.position();
    OLAPStatus res = OLAP_SUCCESS;
    {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        res = _file_cursor.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("read header fail");
            return res;
        }
        res = _fill_compressed(header.length);
        if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("read header fail");
            return res;
        }
        _stats->compressed_bytes_read += sizeof(header) + header.length;
    }

    if (header.type == StreamHead::UNCOMPRESSED) {
        StorageByteBuffer* tmp = _compressed_helper;
        _compressed_helper = *_shared_buffer;
        *_shared_buffer = tmp;
    } else {
        _compressed_helper->set_position(0);
        _compressed_helper->set_limit(_compress_buffer_size);
        {
            SCOPED_RAW_TIMER(&_stats->decompress_ns);
            res = _decompressor(*_shared_buffer, _compressed_helper);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to decompress err=%d", res);
                return res;
            }
        }
    }
    _stats->uncompressed_bytes_read += _compressed_helper->limit();

    _uncompressed = _compressed_helper;
    _current_compress_position = file_cursor_used;
    return res;
}

// 设置读取的位置
OLAPStatus ReadOnlyFileStream::seek(PositionProvider* position) {
    OLAPStatus res = OLAP_SUCCESS;
    // 先seek到解压前的位置，也就是writer中写入的spilled byte
    int64_t compressed_position = position->get_next();
    int64_t uncompressed_bytes = position->get_next();
    if (_current_compress_position == compressed_position && nullptr != _uncompressed) {
        /*
         * 多数情况下不会出现_uncompressed为NULL的情况，
         * 但varchar类型的数据可能会导致查询中出现_uncompressed == nullptr 。
         * 假设查询恰好命中A压缩块的最后一行, 而相临下一个
         * 中压缩块varchar全是空串，会导致_uncompressed == nullptr。
         * 如果后面的segmentreader中还需要再次遍历A压缩块，会出现空指针。
         */
    } else {
        _file_cursor.seek(compressed_position);
        _uncompressed = nullptr;

        res = _assure_data();
        if (OLAP_LIKELY(OLAP_SUCCESS == res)) {
            // assure data will be successful in most case
        } else if (res == OLAP_ERR_COLUMN_STREAM_EOF) {
            VLOG_TRACE << "file stream eof.";
            return res;
        } else {
            OLAP_LOG_WARNING("fail to assure data after seek");
            return res;
        }
    }

    res = _uncompressed->set_position(uncompressed_bytes);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to set position.[res=%d, position=%lu]", res, uncompressed_bytes);
        return res;
    }

    return OLAP_SUCCESS;
}

// 跳过指定的size的流
OLAPStatus ReadOnlyFileStream::skip(uint64_t skip_length) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    uint64_t skip_byte = 0;
    uint64_t byte_to_skip = skip_length;

    // 如果不够跳，则先尝试跳过整个数据块，直到当前的数据
    // 剩下的字节数足够跳过 又或者时EOF
    do {
        skip_byte = std::min(_uncompressed->remaining(), byte_to_skip);
        _uncompressed->set_position(_uncompressed->position() + skip_byte);
        byte_to_skip -= skip_byte;
        // 如果跳到当前的块尽头，那么assure可以换到下一个块
        // 如果当前块就可以满足skip_length,那么_assure_data没任何作用。
        res = _assure_data();
        // while放下面，通常会少判断一次
    } while (byte_to_skip != 0 && res == OLAP_SUCCESS);

    return res;
}

OLAPStatus ReadOnlyFileStream::_fill_compressed(size_t length) {
    if (length > _compress_buffer_size) {
        LOG(WARNING) << "overflow when fill compressed."
                     << ", length=" << length << ", compress_size" << _compress_buffer_size;
        return OLAP_ERR_OUT_OF_BOUND;
    }

    OLAPStatus res = _file_cursor.read((*_shared_buffer)->array(), length);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to fill compressed buffer.");
        return res;
    }

    (*_shared_buffer)->set_position(0);
    (*_shared_buffer)->set_limit(length);
    return res;
}

uint64_t ReadOnlyFileStream::available() {
    return _file_cursor.remain();
}

} // namespace doris
