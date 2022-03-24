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

#include "olap/in_stream.h"

#include "olap/byte_buffer.h"
#include "olap/out_stream.h"

namespace doris {

InStream::InStream(std::vector<StorageByteBuffer*>* inputs, const std::vector<uint64_t>& offsets,
                   uint64_t length, Decompressor decompressor, uint32_t compress_buffer_size)
        : _inputs(*inputs),
          _offsets(offsets),
          _length(length),
          _decompressor(decompressor),
          _compress_buffer_size(compress_buffer_size),
          _current_offset(0),
          _current_range(0),
          _compressed(nullptr),
          _uncompressed(nullptr) {}

InStream::~InStream() {
    SAFE_DELETE(_compressed);
    SAFE_DELETE(_uncompressed);
}

OLAPStatus InStream::_slice(uint64_t chunk_size, StorageByteBuffer** out_slice) {
    uint64_t len = chunk_size;
    uint64_t old_offset = _current_offset;
    StorageByteBuffer* slice = nullptr;

    //如果buffer够读，拿出一个chunksize，并设置position
    if (OLAP_LIKELY(_compressed->remaining() >= len)) {
        slice = StorageByteBuffer::reference_buffer(_compressed, _compressed->position(), len);

        if (OLAP_UNLIKELY(nullptr == slice)) {
            return OLAP_ERR_MALLOC_ERROR;
        }

        _compressed->set_position(_compressed->position() + len);
        *out_slice = slice;
        // 这里之前没有设置_current_offset
        _current_offset += len;
        return OLAP_SUCCESS;
    } else if (_current_range >= _inputs.size() - 1) {
        // 如果buffer用完了
        OLAP_LOG_WARNING("EOF in InStream. [Need=%lu]", chunk_size);
        return OLAP_ERR_OUT_OF_BOUND;
    }

    // 这里并不分配chuck_size, 而是分配一个最大值, 这样利于减少内存碎片
    slice = StorageByteBuffer::create(_compress_buffer_size);

    if (OLAP_UNLIKELY(nullptr == slice)) {
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 当前的compress里的buffer不够了
    _current_offset += _compressed->remaining();
    len -= _compressed->remaining();
    // 先拿一部分出来
    slice->put(&(_compressed->array()[_compressed->position()]), _compressed->remaining());

    // 向后边的buffer移动
    ++_current_range;

    while (len > 0 && _current_range < _inputs.size()) {
        SAFE_DELETE(_compressed);
        // 再取一部分压缩过的buffer
        _compressed = StorageByteBuffer::reference_buffer(_inputs[_current_range],
                                                          _inputs[_current_range]->position(),
                                                          _inputs[_current_range]->remaining());

        if (OLAP_UNLIKELY(nullptr == _compressed)) {
            SAFE_DELETE(slice);
            return OLAP_ERR_MALLOC_ERROR;
        }

        // 如果剩下的大于需要取的部分，拿出来
        // 否则继续这个过程
        if (_compressed->remaining() >= len) {
            slice->put(&(_compressed->array()[_compressed->position()]), len);
            _compressed->set_position(_compressed->position() + len);
            _current_offset += len;
            slice->flip();
            *out_slice = slice;
            return OLAP_SUCCESS;
        } else {
            _current_offset += _compressed->remaining();
            len -= _compressed->remaining();
            slice->put(&(_compressed->array()[_compressed->position()]), _compressed->remaining());
        }

        ++_current_range;
    }

    // 到这里就意味着上边的循环里没有取到足够的buf
    // 回退到进来之前的状态
    _seek(old_offset);
    OLAP_LOG_WARNING("EOF in InStream. [Need=%lu]", chunk_size);
    return OLAP_ERR_OUT_OF_BOUND;
}

OLAPStatus InStream::_assure_data() {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_LIKELY(_uncompressed != nullptr && _uncompressed->remaining() > 0)) {
        return OLAP_SUCCESS;
    } else if (OLAP_UNLIKELY((_uncompressed == nullptr || _uncompressed->remaining() == 0) &&
                             (_current_offset == _length))) {
        return OLAP_ERR_COLUMN_STREAM_EOF;
    }

    // read head and data
    SAFE_DELETE(_uncompressed);

    // 到这里说明当前uncompress没有什么可以读了，input拿数据
    // 如果没有compress。或者compress耗尽，用_seek向后一个buff移动
    if (_compressed == nullptr || _compressed->remaining() == 0) {
        res = _seek(_current_offset);
        if (OLAP_SUCCESS != res) {
            return res;
        }
    }

    // 取了compress之后，看下是不是能拿出一个head的长度，如果可以
    // 则解析head，从outstream代码看header不会分开写在两个压缩块中
    if (OLAP_LIKELY(_compressed->remaining() >= sizeof(StreamHead))) {
        // 如果可以，从compress中拿出一个head，head是未压缩的。
        StreamHead head;
        _compressed->get((char*)&head, sizeof(head));

        if (head.length > _compress_buffer_size) {
            OLAP_LOG_WARNING("chunk size is larger than buffer size. [chunk=%u buffer_size=%u]",
                             head.length, _compress_buffer_size);
            return OLAP_ERR_COLUMN_READ_STREAM;
        }

        // 向后移动整体偏移
        _current_offset += sizeof(StreamHead);
        StorageByteBuffer* slice = nullptr;

        // 根据head取一块buf，这里应该要调整_current_offset
        res = _slice(head.length, &slice);
        if (OLAP_LIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("fail to slice data from stream.");
            return OLAP_ERR_COLUMN_READ_STREAM;
        }

        // 如果没压缩，就直接读这块
        // 否则需要解压下
        if (head.type == StreamHead::UNCOMPRESSED) {
            _uncompressed = slice;
        } else {
            _uncompressed = StorageByteBuffer::create(_compress_buffer_size);

            if (OLAP_UNLIKELY(nullptr == _uncompressed)) {
                res = OLAP_ERR_MALLOC_ERROR;
            } else {
                res = _decompressor(slice, _uncompressed);
            }

            // 一定要释放掉slice
            SAFE_DELETE(slice);
        }
    } else {
        OLAP_LOG_WARNING(
                "compressed remaining size less than stream head size. "
                "[compressed_remaining_size=%lu stream_head_size=%lu]",
                _compressed->remaining(), sizeof(StreamHead));
        return OLAP_ERR_COLUMN_READ_STREAM;
    }

    return res;
}

uint64_t InStream::available() {
    OLAPStatus res;
    res = _assure_data();

    if (res != OLAP_SUCCESS) {
        return 0;
    }

    return _uncompressed->remaining();
}

// seek的是解压前的数据。
OLAPStatus InStream::_seek(uint64_t position) {
    for (uint32_t i = 0; i < _inputs.size(); i++) {
        if (_offsets[i] <= position && position - _offsets[i] < _inputs[i]->remaining()) {
            // don't need to malloc _compressed if current range don't be changed.
            if (!(_current_range == i && nullptr != _compressed)) {
                _current_range = i;
                SAFE_DELETE(_compressed);
                _compressed =
                        StorageByteBuffer::reference_buffer(_inputs[i], 0, _inputs[i]->remaining());
            }

            uint64_t pos = _inputs[i]->position() + position - _offsets[i];
            _compressed->set_position(pos);
            _current_offset = position;
            return OLAP_SUCCESS;
        }
    }

    SAFE_DELETE(_compressed);

    if (!_inputs.empty() && position == _offsets.back() + _inputs.back()->remaining()) {
        _current_range = _inputs.size() - 1;
        _compressed = StorageByteBuffer::reference_buffer(_inputs[_current_range], 0,
                                                          _inputs[_current_range]->limit());
        _current_offset = position;
        return OLAP_SUCCESS;
    }

    return OLAP_ERR_OUT_OF_BOUND;
}

OLAPStatus InStream::seek(PositionProvider* position) {
    OLAPStatus res = OLAP_SUCCESS;

    // 先seek到解压前的位置，也就是writer中写入的spilled byte
    res = _seek(position->get_next());
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to seek.[res=%d]", res);
        return res;
    }

    // 再定位到已经解压的内存中的位置
    long uncompressed_bytes = position->get_next();

    if (uncompressed_bytes != 0) {
        SAFE_DELETE(_uncompressed);
        res = _assure_data();

        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to assure data.[res=%d]", res);
            return res;
        }

        res = _uncompressed->set_position(uncompressed_bytes);

        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to set position.[res=%d, position=%lu]", res,
                             _uncompressed->position() + uncompressed_bytes);
            return res;
        }
    } else if (_uncompressed != nullptr) {
        // mark the uncompressed buffer as done
        res = _uncompressed->set_position(_uncompressed->limit());

        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to set position.[res=%d, position=%lu]", res,
                             _uncompressed->limit());
            return res;
        }
    }

    return OLAP_SUCCESS;
}

// skip的是解压后的数据
OLAPStatus InStream::skip(uint64_t skip_length) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    uint64_t skip_byte = 0;
    uint64_t byte_to_skip = skip_length;

    // 如果不够跳，则先尝试跳过整个数据块，直到当前的数据块剩下的字节数足够跳过
    // 又或者是EOF
    do {
        skip_byte = std::min(_uncompressed->remaining(), byte_to_skip);
        _uncompressed->set_position(_uncompressed->position() + skip_byte);

        byte_to_skip -= skip_byte;
        // 如果跳到当前块的尽头，那么assure可以换到下一个块。
        // 如果当前块就可以满足skip_length，那么_assure_data没任何作用。
        res = _assure_data();
        // while 放下边，通常会少一次判断
    } while (byte_to_skip != 0 && res == OLAP_SUCCESS);

    return res;
}

} // namespace doris
