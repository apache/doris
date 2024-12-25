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

#include "vec/exec/format/parquet/reference_counted_reader.h"

#include <sstream>
#include <stdexcept>

namespace doris {
namespace vectorized {

ReferenceCountedReader::ReferenceCountedReader(const io::PrefetchRange& range,
                                               io::FileReaderSPtr file_reader,
                                               ChunkReader::Statistics& statistics)
        : _range(range),
          _file_reader(std::move(file_reader)),
          _statistics(statistics),
          _data(nullptr),
          _reference_count(1) {
    if (_range.end_offset - _range.start_offset > MAX_ARRAY_SIZE) {
        throw std::invalid_argument("Cannot read range bigger than " +
                                    std::to_string(MAX_ARRAY_SIZE) + " but got " +
                                    std::to_string(_range.end_offset - _range.start_offset));
    }
}

void ReferenceCountedReader::addReference() {
    if (_reference_count <= 0) {
        throw std::runtime_error("Chunk reader is already closed");
    }
    _reference_count++;
}

int64_t ReferenceCountedReader::getDiskOffset() {
    return _range.start_offset;
}

Status ReferenceCountedReader::read(const io::IOContext* io_ctx, std::shared_ptr<Slice>* result) {
    if (_reference_count <= 0) {
        throw std::runtime_error("Chunk reader is already closed");
    }

    auto range_size = _range.end_offset - _range.start_offset;
    if (_data == nullptr) { // need read new range to cache.

        //        _cache_statistics.cache_refresh_count++;
        //        _cache_statistics.read_to_cache_bytes += range_size;
        //        SCOPED_RAW_TIMER(&_cache_statistics.read_to_cache_time);

        //        Slice slice = {_data.get(), range_size};
        _data = std::make_unique<char[]>(range_size);
        *result = std::make_shared<Slice>(_data.get(), range_size);
        size_t bytes_read;
        RETURN_IF_ERROR(
                _file_reader->read_at(_range.start_offset, *(*result), &bytes_read, io_ctx));
        _statistics.merged_io++;
        _statistics.merged_bytes += bytes_read;

        if (bytes_read != range_size) [[unlikely]] {
            return Status::InternalError(
                    "const io::IOContext* io_ctx use file reader read bytes {} not eq expect size "
                    "{}",
                    bytes_read, range_size);
        }
    }
    *result = std::make_shared<Slice>(_data.get(), range_size);
    return Status::OK();
}

//Status ReferenceCountedReader::read(size_t offset, Slice result, size_t* bytes_read,
//                                    const io::IOContext* io_ctx) {
//    if (_reference_count <= 0) {
//        throw std::runtime_error("Chunk reader is already closed");
//    }
//
//    auto request_size = result.size;
//
//    if (_data == nullptr) { // need read new range to cache.
//        auto range_size = _range.end_offset - _range.start_offset;
//
////        _cache_statistics.cache_refresh_count++;
////        _cache_statistics.read_to_cache_bytes += range_size;
////        SCOPED_RAW_TIMER(&_cache_statistics.read_to_cache_time);
//
//        Slice cache_slice = {_data.get(), range_size};
//        RETURN_IF_ERROR(
//                _file_reader->read_at(_range.start_offset, cache_slice, bytes_read, io_ctx));
//
//        if (*bytes_read != range_size) [[unlikely]] {
//            return Status::InternalError(
//                    "RangeCacheFileReader use inner reader read bytes {} not eq expect size {}",
//                    *bytes_read, range_size);
//        }
//    }
//
//    int64_t buffer_offset = offset - _range.start_offset;
//    memcpy(result.data, _data.get() + buffer_offset, request_size);
//    *bytes_read = request_size;
//
//    return Status::OK();
//}

void ReferenceCountedReader::free() {
    if (_reference_count <= 0) {
        throw std::runtime_error("Reference count is already 0");
    }

    _reference_count--;
    if (_reference_count == 0) {
        _data.reset();
    }
}

size_t ReferenceCountedReader::size() {
    return _range.end_offset - _range.start_offset;
}

size_t ReferenceCountedReader::file_size() {
    return _file_reader->size();
}

std::string ReferenceCountedReader::toString() const {
    std::ostringstream oss;
    oss << "ReferenceCountedReader{range=" << /* range_.toString() */
            ", referenceCount=" << _reference_count << "}";
    return oss.str();
}

// 这个函数需要根据实际的I/O实现来完成
//Status ReferenceCountedReader::readFully(size_t offset, Slice result, size_t* bytes_read, const doris::io::IOContext* io_ctx) {
//    return _file_reader->read_at(offset, result, bytes_read, io_ctx)
//}

} // namespace vectorized
} // namespace doris
