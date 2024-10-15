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

#include "io/fs/buffered_reader.h"

#include <bvar/reducer.h>
#include <bvar/window.h>
#include <string.h>

#include <algorithm>
#include <chrono>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace doris {
namespace io {
struct IOContext;

// add bvar to capture the download bytes per second by buffered reader
bvar::Adder<uint64_t> g_bytes_downloaded("buffered_reader", "bytes_downloaded");
bvar::PerSecond<bvar::Adder<uint64_t>> g_bytes_downloaded_per_second("buffered_reader",
                                                                     "bytes_downloaded_per_second",
                                                                     &g_bytes_downloaded, 60);

Status MergeRangeFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                          const IOContext* io_ctx) {
    _statistics.request_io++;
    *bytes_read = 0;
    if (result.size == 0) {
        return Status::OK();
    }
    const int range_index = _search_read_range(offset, offset + result.size);
    if (range_index < 0) {
        SCOPED_RAW_TIMER(&_statistics.read_time);
        Status st = _reader->read_at(offset, result, bytes_read, io_ctx);
        _statistics.merged_io++;
        _statistics.request_bytes += *bytes_read;
        _statistics.merged_bytes += *bytes_read;
        return st;
    }
    if (offset + result.size > _random_access_ranges[range_index].end_offset) {
        // return _reader->read_at(offset, result, bytes_read, io_ctx);
        return Status::IOError("Range in RandomAccessReader should be read sequentially");
    }

    size_t has_read = 0;
    RangeCachedData& cached_data = _range_cached_data[range_index];
    cached_data.has_read = true;
    if (cached_data.contains(offset)) {
        // has cached data in box
        _read_in_box(cached_data, offset, result, &has_read);
        _statistics.request_bytes += has_read;
        if (has_read == result.size) {
            // all data is read in cache
            *bytes_read = has_read;
            return Status::OK();
        }
    } else if (!cached_data.empty()) {
        // the data in range may be skipped or ignored
        for (int16 box_index : cached_data.ref_box) {
            _dec_box_ref(box_index);
        }
        cached_data.reset();
    }

    size_t to_read = result.size - has_read;
    if (to_read >= SMALL_IO || to_read >= _remaining) {
        SCOPED_RAW_TIMER(&_statistics.read_time);
        size_t read_size = 0;
        RETURN_IF_ERROR(_reader->read_at(offset + has_read, Slice(result.data + has_read, to_read),
                                         &read_size, io_ctx));
        *bytes_read = has_read + read_size;
        _statistics.merged_io++;
        _statistics.request_bytes += read_size;
        _statistics.merged_bytes += read_size;
        return Status::OK();
    }

    // merge small IO
    size_t merge_start = offset + has_read;
    const size_t merge_end = merge_start + READ_SLICE_SIZE;
    // <slice_size, is_content>
    std::vector<std::pair<size_t, bool>> merged_slice;
    size_t content_size = 0;
    size_t hollow_size = 0;
    if (merge_start > _random_access_ranges[range_index].end_offset) {
        return Status::IOError("Fail to merge small IO");
    }
    int merge_index = range_index;
    while (merge_start < merge_end && merge_index < _random_access_ranges.size()) {
        size_t content_max = _remaining - content_size;
        if (content_max == 0) {
            break;
        }
        if (merge_index != range_index && _range_cached_data[merge_index].has_read) {
            // don't read or merge twice
            break;
        }
        if (_random_access_ranges[merge_index].end_offset > merge_end) {
            size_t add_content = std::min(merge_end - merge_start, content_max);
            content_size += add_content;
            merge_start += add_content;
            merged_slice.emplace_back(add_content, true);
            break;
        }
        size_t add_content =
                std::min(_random_access_ranges[merge_index].end_offset - merge_start, content_max);
        content_size += add_content;
        merge_start += add_content;
        merged_slice.emplace_back(add_content, true);
        if (merge_start != _random_access_ranges[merge_index].end_offset) {
            break;
        }
        if (merge_index < _random_access_ranges.size() - 1 && merge_start < merge_end) {
            size_t gap = _random_access_ranges[merge_index + 1].start_offset -
                         _random_access_ranges[merge_index].end_offset;
            if ((content_size + hollow_size) > SMALL_IO && gap >= SMALL_IO) {
                // too large gap
                break;
            }
            if (gap < merge_end - merge_start && content_size < _remaining &&
                !_range_cached_data[merge_index + 1].has_read) {
                hollow_size += gap;
                merge_start = _random_access_ranges[merge_index + 1].start_offset;
                merged_slice.emplace_back(gap, false);
            } else {
                // there's no enough memory to read hollow data
                break;
            }
        }
        merge_index++;
    }
    content_size = 0;
    hollow_size = 0;
    std::vector<std::pair<double, size_t>> ratio_and_size;
    // Calculate the read amplified ratio for each merge operation and the size of the merged data.
    // Find the largest size of the merged data whose amplified ratio is less than config::max_amplified_read_ratio
    for (const std::pair<size_t, bool>& slice : merged_slice) {
        if (slice.second) {
            content_size += slice.first;
            if (slice.first > 0) {
                ratio_and_size.emplace_back((double)hollow_size / content_size,
                                            content_size + hollow_size);
            }
        } else {
            hollow_size += slice.first;
        }
    }
    size_t best_merged_size = 0;
    for (int i = 0; i < ratio_and_size.size(); ++i) {
        const std::pair<double, size_t>& rs = ratio_and_size[i];
        size_t equivalent_size = rs.second / (i + 1);
        if (rs.second > best_merged_size) {
            if (rs.first <= _max_amplified_ratio ||
                (_max_amplified_ratio < 1 && equivalent_size <= _equivalent_io_size)) {
                best_merged_size = rs.second;
            }
        }
    }

    if (best_merged_size == to_read) {
        // read directly to avoid copy operation
        SCOPED_RAW_TIMER(&_statistics.read_time);
        size_t read_size = 0;
        RETURN_IF_ERROR(_reader->read_at(offset + has_read, Slice(result.data + has_read, to_read),
                                         &read_size, io_ctx));
        *bytes_read = has_read + read_size;
        _statistics.merged_io++;
        _statistics.request_bytes += read_size;
        _statistics.merged_bytes += read_size;
        return Status::OK();
    }

    merge_start = offset + has_read;
    size_t merge_read_size = 0;
    RETURN_IF_ERROR(
            _fill_box(range_index, merge_start, best_merged_size, &merge_read_size, io_ctx));
    if (cached_data.start_offset != merge_start) {
        return Status::IOError("Wrong start offset in merged IO");
    }

    // read from cached data
    size_t box_read_size = 0;
    _read_in_box(cached_data, merge_start, Slice(result.data + has_read, to_read), &box_read_size);
    *bytes_read = has_read + box_read_size;
    _statistics.request_bytes += box_read_size;
    if (*bytes_read < result.size && box_read_size < merge_read_size) {
        return Status::IOError("Can't read enough bytes in merged IO");
    }
    return Status::OK();
}

int MergeRangeFileReader::_search_read_range(size_t start_offset, size_t end_offset) {
    if (_random_access_ranges.empty()) {
        return -1;
    }
    int left = 0, right = _random_access_ranges.size() - 1;
    do {
        int mid = left + (right - left) / 2;
        const PrefetchRange& range = _random_access_ranges[mid];
        if (range.start_offset <= start_offset && start_offset < range.end_offset) {
            if (range.start_offset <= end_offset && end_offset <= range.end_offset) {
                return mid;
            } else {
                return -1;
            }
        } else if (range.start_offset > start_offset) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    } while (left <= right);
    return -1;
}

void MergeRangeFileReader::_clean_cached_data(RangeCachedData& cached_data) {
    if (!cached_data.empty()) {
        for (int i = 0; i < cached_data.ref_box.size(); ++i) {
            DCHECK_GT(cached_data.box_end_offset[i], cached_data.box_start_offset[i]);
            int16 box_index = cached_data.ref_box[i];
            DCHECK_GT(_box_ref[box_index], 0);
            _box_ref[box_index]--;
        }
    }
    cached_data.reset();
}

void MergeRangeFileReader::_dec_box_ref(int16 box_index) {
    if (--_box_ref[box_index] == 0) {
        _remaining += BOX_SIZE;
    }
    if (box_index == _last_box_ref) {
        _last_box_ref = -1;
        _last_box_usage = 0;
    }
}

void MergeRangeFileReader::_read_in_box(RangeCachedData& cached_data, size_t offset, Slice result,
                                        size_t* bytes_read) {
    SCOPED_RAW_TIMER(&_statistics.copy_time);
    auto handle_in_box = [&](size_t remaining, char* copy_out) {
        size_t to_handle = remaining;
        int cleaned_box = 0;
        for (int i = 0; i < cached_data.ref_box.size() && remaining > 0; ++i) {
            int16 box_index = cached_data.ref_box[i];
            size_t box_to_handle = std::min(remaining, (size_t)(cached_data.box_end_offset[i] -
                                                                cached_data.box_start_offset[i]));
            if (copy_out != nullptr) {
            }
            if (copy_out != nullptr) {
                memcpy(copy_out + to_handle - remaining,
                       _boxes[box_index] + cached_data.box_start_offset[i], box_to_handle);
            }
            remaining -= box_to_handle;
            cached_data.box_start_offset[i] += box_to_handle;
            if (cached_data.box_start_offset[i] == cached_data.box_end_offset[i]) {
                cleaned_box++;
                _dec_box_ref(box_index);
            }
        }
        DCHECK_EQ(remaining, 0);
        if (cleaned_box > 0) {
            cached_data.ref_box.erase(cached_data.ref_box.begin(),
                                      cached_data.ref_box.begin() + cleaned_box);
            cached_data.box_start_offset.erase(cached_data.box_start_offset.begin(),
                                               cached_data.box_start_offset.begin() + cleaned_box);
            cached_data.box_end_offset.erase(cached_data.box_end_offset.begin(),
                                             cached_data.box_end_offset.begin() + cleaned_box);
        }
        cached_data.start_offset += to_handle;
        if (cached_data.start_offset == cached_data.end_offset) {
            _clean_cached_data(cached_data);
        }
    };

    if (offset > cached_data.start_offset) {
        // the data in range may be skipped
        size_t to_skip = offset - cached_data.start_offset;
        handle_in_box(to_skip, nullptr);
    }

    size_t to_read = std::min(cached_data.end_offset - cached_data.start_offset, result.size);
    handle_in_box(to_read, result.data);
    *bytes_read = to_read;
}

Status MergeRangeFileReader::_fill_box(int range_index, size_t start_offset, size_t to_read,
                                       size_t* bytes_read, const IOContext* io_ctx) {
    if (_read_slice == nullptr) {
        _read_slice = new char[READ_SLICE_SIZE];
    }
    *bytes_read = 0;
    {
        SCOPED_RAW_TIMER(&_statistics.read_time);
        RETURN_IF_ERROR(
                _reader->read_at(start_offset, Slice(_read_slice, to_read), bytes_read, io_ctx));
        _statistics.merged_io++;
        _statistics.merged_bytes += *bytes_read;
    }

    SCOPED_RAW_TIMER(&_statistics.copy_time);
    size_t copy_start = start_offset;
    const size_t copy_end = start_offset + *bytes_read;
    // copy data into small boxes
    // tuple(box_index, box_start_offset, file_start_offset, file_end_offset)
    std::vector<std::tuple<int16, uint32, size_t, size_t>> filled_boxes;

    auto fill_box = [&](int16 fill_box_ref, uint32 box_usage, size_t box_copy_end) {
        size_t copy_size = std::min(box_copy_end - copy_start, BOX_SIZE - box_usage);
        memcpy(_boxes[fill_box_ref] + box_usage, _read_slice + copy_start - start_offset,
               copy_size);
        filled_boxes.emplace_back(fill_box_ref, box_usage, copy_start, copy_start + copy_size);
        copy_start += copy_size;
        _last_box_ref = fill_box_ref;
        _last_box_usage = box_usage + copy_size;
        _box_ref[fill_box_ref]++;
        if (box_usage == 0) {
            _remaining -= BOX_SIZE;
        }
    };

    for (int fill_range_index = range_index;
         fill_range_index < _random_access_ranges.size() && copy_start < copy_end;
         ++fill_range_index) {
        RangeCachedData& fill_range_cache = _range_cached_data[fill_range_index];
        DCHECK(fill_range_cache.empty());
        fill_range_cache.reset();
        const PrefetchRange& fill_range = _random_access_ranges[fill_range_index];
        if (fill_range.start_offset > copy_start) {
            // don't copy hollow data
            size_t hollow_size = fill_range.start_offset - copy_start;
            DCHECK_GT(copy_end - copy_start, hollow_size);
            copy_start += hollow_size;
        }

        const size_t range_copy_end = std::min(copy_end, fill_range.end_offset);
        // reuse the remaining capacity of last box
        if (_last_box_ref >= 0 && _last_box_usage < BOX_SIZE) {
            fill_box(_last_box_ref, _last_box_usage, range_copy_end);
        }
        // reuse the former released box
        for (int16 i = 0; i < _boxes.size() && copy_start < range_copy_end; ++i) {
            if (_box_ref[i] == 0) {
                fill_box(i, 0, range_copy_end);
            }
        }
        // apply for new box to copy data
        while (copy_start < range_copy_end && _boxes.size() < NUM_BOX) {
            _boxes.emplace_back(new char[BOX_SIZE]);
            _box_ref.emplace_back(0);
            fill_box(_boxes.size() - 1, 0, range_copy_end);
        }
        DCHECK_EQ(copy_start, range_copy_end);

        if (!filled_boxes.empty()) {
            fill_range_cache.start_offset = std::get<2>(filled_boxes[0]);
            fill_range_cache.end_offset = std::get<3>(filled_boxes.back());
            for (auto& tuple : filled_boxes) {
                fill_range_cache.ref_box.emplace_back(std::get<0>(tuple));
                fill_range_cache.box_start_offset.emplace_back(std::get<1>(tuple));
                fill_range_cache.box_end_offset.emplace_back(
                        std::get<1>(tuple) + std::get<3>(tuple) - std::get<2>(tuple));
            }
            filled_boxes.clear();
        }
    }
    return Status::OK();
}

// there exists occasions where the buffer is already closed but
// some prior tasks are still queued in thread pool, so we have to check whether
// the buffer is closed each time the condition variable is notified.
void PrefetchBuffer::reset_offset(size_t offset) {
    {
        std::unique_lock lck {_lock};
        if (!_prefetched.wait_for(
                    lck, std::chrono::milliseconds(config::buffered_reader_read_timeout_ms),
                    [this]() { return _buffer_status != BufferStatus::PENDING; })) {
            _prefetch_status = Status::TimedOut("time out when reset prefetch buffer");
            return;
        }
        if (UNLIKELY(_buffer_status == BufferStatus::CLOSED)) {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::RESET;
        _offset = offset;
        _prefetched.notify_all();
    }
    if (UNLIKELY(offset >= _file_range.end_offset)) {
        _len = 0;
        _exceed = true;
        return;
    } else {
        _exceed = false;
    }
    _prefetch_status = ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buffer_ptr = shared_from_this()]() { buffer_ptr->prefetch_buffer(); });
}

// only this function would run concurrently in another thread
void PrefetchBuffer::prefetch_buffer() {
    {
        std::unique_lock lck {_lock};
        if (!_prefetched.wait_for(
                    lck, std::chrono::milliseconds(config::buffered_reader_read_timeout_ms),
                    [this]() {
                        return _buffer_status == BufferStatus::RESET ||
                               _buffer_status == BufferStatus::CLOSED;
                    })) {
            _prefetch_status = Status::TimedOut("time out when invoking prefetch buffer");
            return;
        }
        // in case buffer is already closed
        if (UNLIKELY(_buffer_status == BufferStatus::CLOSED)) {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::PENDING;
        _prefetched.notify_all();
    }

    int read_range_index = search_read_range(_offset);
    size_t buf_size;
    if (read_range_index == -1) {
        buf_size =
                _file_range.end_offset - _offset > _size ? _size : _file_range.end_offset - _offset;
    } else {
        buf_size = merge_small_ranges(_offset, read_range_index);
    }

    _len = 0;
    Status s;

    {
        SCOPED_RAW_TIMER(&_statis.read_time);
        s = _reader->read_at(_offset, Slice {_buf.get(), buf_size}, &_len, _io_ctx);
    }
    if (UNLIKELY(s.ok() && buf_size != _len)) {
        // This indicates that the data size returned by S3 object storage is smaller than what we requested,
        // which seems to be a violation of the S3 protocol since our request range was valid.
        // We currently consider this situation a bug and will treat this task as a failure.
        s = Status::InternalError("Data size returned by S3 is smaller than requested");
        LOG(WARNING) << "Data size returned by S3 is smaller than requested" << _reader->path()
                     << " request bytes " << buf_size << " returned size " << _len;
    }
    g_bytes_downloaded << _len;
    _statis.prefetch_request_io += 1;
    _statis.prefetch_request_bytes += _len;
    std::unique_lock lck {_lock};
    if (!_prefetched.wait_for(lck,
                              std::chrono::milliseconds(config::buffered_reader_read_timeout_ms),
                              [this]() { return _buffer_status == BufferStatus::PENDING; })) {
        _prefetch_status = Status::TimedOut("time out when invoking prefetch buffer");
        return;
    }
    if (!s.ok() && _offset < _reader->size()) {
        // We should print the error msg since this buffer might not be accessed by the consumer
        // which would result in the status being missed
        LOG_WARNING("prefetch path {} failed, offset {}, error {}", _reader->path().native(),
                    _offset, s.to_string());
        _prefetch_status = std::move(s);
    }
    _buffer_status = BufferStatus::PREFETCHED;
    _prefetched.notify_all();
    // eof would come up with len == 0, it would be handled by read_buffer
}

int PrefetchBuffer::search_read_range(size_t off) const {
    if (_random_access_ranges == nullptr || _random_access_ranges->empty()) {
        return -1;
    }
    const std::vector<PrefetchRange>& random_access_ranges = *_random_access_ranges;
    int left = 0, right = random_access_ranges.size() - 1;
    do {
        int mid = left + (right - left) / 2;
        const PrefetchRange& range = random_access_ranges[mid];
        if (range.start_offset <= off && range.end_offset > off) {
            return mid;
        } else if (range.start_offset > off) {
            right = mid;
        } else {
            left = mid + 1;
        }
    } while (left < right);
    if (random_access_ranges[right].start_offset > off) {
        return right;
    } else {
        return -1;
    }
}

size_t PrefetchBuffer::merge_small_ranges(size_t off, int range_index) const {
    if (_random_access_ranges == nullptr || _random_access_ranges->empty()) {
        return _size;
    }
    int64 remaining = _size;
    const std::vector<PrefetchRange>& random_access_ranges = *_random_access_ranges;
    while (remaining > 0 && range_index < random_access_ranges.size()) {
        const PrefetchRange& range = random_access_ranges[range_index];
        if (range.start_offset <= off && range.end_offset > off) {
            remaining -= range.end_offset - off;
            off = range.end_offset;
            range_index++;
        } else if (range.start_offset > off) {
            // merge small range
            size_t hollow = range.start_offset - off;
            if (hollow < remaining) {
                remaining -= hollow;
                off = range.start_offset;
            } else {
                break;
            }
        } else {
            DCHECK(false);
        }
    }
    if (remaining < 0 || remaining == _size) {
        remaining = 0;
    }
    return _size - remaining;
}

Status PrefetchBuffer::read_buffer(size_t off, const char* out, size_t buf_len,
                                   size_t* bytes_read) {
    if (UNLIKELY(off >= _file_range.end_offset)) {
        // Reader can read out of [start_offset, end_offset) by synchronous method.
        return _reader->read_at(off, Slice {out, buf_len}, bytes_read, _io_ctx);
    }
    if (_exceed) {
        reset_offset((off / _size) * _size);
        return read_buffer(off, out, buf_len, bytes_read);
    }
    auto start = std::chrono::steady_clock::now();
    // The baseline time is calculated by dividing the size of each buffer by MB/s.
    // If it exceeds this value, it is considered a slow I/O operation.
    constexpr auto read_time_baseline = std::chrono::seconds(s_max_pre_buffer_size / 1024 / 1024);
    {
        std::unique_lock lck {_lock};
        // buffer must be prefetched or it's closed
        if (!_prefetched.wait_for(
                    lck, std::chrono::milliseconds(config::buffered_reader_read_timeout_ms),
                    [this]() {
                        return _buffer_status == BufferStatus::PREFETCHED ||
                               _buffer_status == BufferStatus::CLOSED;
                    })) {
            _prefetch_status = Status::TimedOut("time out when read prefetch buffer");
            return _prefetch_status;
        }
        if (UNLIKELY(BufferStatus::CLOSED == _buffer_status)) {
            return Status::OK();
        }
    }
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start);
    if (duration > read_time_baseline) [[unlikely]] {
        LOG_WARNING("The prefetch io is too slow");
    }
    RETURN_IF_ERROR(_prefetch_status);
    // there is only parquet would do not sequence read
    // it would read the end of the file first
    if (UNLIKELY(!contains(off))) {
        reset_offset((off / _size) * _size);
        return read_buffer(off, out, buf_len, bytes_read);
    }
    if (UNLIKELY(0 == _len || _offset + _len < off)) {
        return Status::OK();
    }

    {
        LIMIT_REMOTE_SCAN_IO(bytes_read);
        // [0]: maximum len trying to read, [1] maximum length buffer can provide, [2] actual len buffer has
        size_t read_len = std::min({buf_len, _offset + _size - off, _offset + _len - off});
        {
            SCOPED_RAW_TIMER(&_statis.copy_time);
            memcpy((void*)out, _buf.get() + (off - _offset), read_len);
        }
        *bytes_read = read_len;
        _statis.request_io += 1;
        _statis.request_bytes += read_len;
    }
    if (off + *bytes_read == _offset + _len) {
        reset_offset(_offset + _whole_buffer_size);
    }
    return Status::OK();
}

void PrefetchBuffer::close() {
    std::unique_lock lck {_lock};
    // in case _reader still tries to write to the buf after we close the buffer
    if (!_prefetched.wait_for(lck,
                              std::chrono::milliseconds(config::buffered_reader_read_timeout_ms),
                              [this]() { return _buffer_status != BufferStatus::PENDING; })) {
        _prefetch_status = Status::TimedOut("time out when close prefetch buffer");
        return;
    }
    _buffer_status = BufferStatus::CLOSED;
    _prefetched.notify_all();
}

void PrefetchBuffer::_collect_profile_before_close() {
    if (_sync_profile != nullptr) {
        _sync_profile(*this);
    }
}

// buffered reader
PrefetchBufferedReader::PrefetchBufferedReader(RuntimeProfile* profile, io::FileReaderSPtr reader,
                                               PrefetchRange file_range, const IOContext* io_ctx,
                                               int64_t buffer_size)
        : _reader(std::move(reader)), _file_range(file_range), _io_ctx(io_ctx) {
    if (buffer_size == -1L) {
        buffer_size = config::remote_storage_read_buffer_mb * 1024 * 1024;
    }
    _size = _reader->size();
    _whole_pre_buffer_size = buffer_size;
    _file_range.end_offset = std::min(_file_range.end_offset, _size);
    int buffer_num = buffer_size > s_max_pre_buffer_size ? buffer_size / s_max_pre_buffer_size : 1;
    std::function<void(PrefetchBuffer&)> sync_buffer = nullptr;
    if (profile != nullptr) {
        const char* prefetch_buffered_reader = "PrefetchBufferedReader";
        ADD_TIMER(profile, prefetch_buffered_reader);
        auto copy_time = ADD_CHILD_TIMER(profile, "CopyTime", prefetch_buffered_reader);
        auto read_time = ADD_CHILD_TIMER(profile, "ReadTime", prefetch_buffered_reader);
        auto prefetch_request_io =
                ADD_CHILD_COUNTER(profile, "PreRequestIO", TUnit::UNIT, prefetch_buffered_reader);
        auto prefetch_request_bytes = ADD_CHILD_COUNTER(profile, "PreRequestBytes", TUnit::BYTES,
                                                        prefetch_buffered_reader);
        auto request_io =
                ADD_CHILD_COUNTER(profile, "RequestIO", TUnit::UNIT, prefetch_buffered_reader);
        auto request_bytes =
                ADD_CHILD_COUNTER(profile, "RequestBytes", TUnit::BYTES, prefetch_buffered_reader);
        sync_buffer = [=](PrefetchBuffer& buf) {
            COUNTER_UPDATE(copy_time, buf._statis.copy_time);
            COUNTER_UPDATE(read_time, buf._statis.read_time);
            COUNTER_UPDATE(prefetch_request_io, buf._statis.prefetch_request_io);
            COUNTER_UPDATE(prefetch_request_bytes, buf._statis.prefetch_request_bytes);
            COUNTER_UPDATE(request_io, buf._statis.request_io);
            COUNTER_UPDATE(request_bytes, buf._statis.request_bytes);
        };
    }
    // set the _cur_offset of this reader as same as the inner reader's,
    // to make sure the buffer reader will start to read at right position.
    for (int i = 0; i < buffer_num; i++) {
        _pre_buffers.emplace_back(std::make_shared<PrefetchBuffer>(
                _file_range, s_max_pre_buffer_size, _whole_pre_buffer_size, _reader.get(), _io_ctx,
                sync_buffer));
    }
}

PrefetchBufferedReader::~PrefetchBufferedReader() {
    /// Better not to call virtual functions in a destructor.
    static_cast<void>(_close_internal());
}

Status PrefetchBufferedReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    if (!_initialized) {
        reset_all_buffer(offset);
        _initialized = true;
    }
    if (UNLIKELY(result.get_size() == 0 || offset >= size())) {
        *bytes_read = 0;
        return Status::OK();
    }
    size_t nbytes = result.get_size();
    int actual_bytes_read = 0;
    while (actual_bytes_read < nbytes && offset < size()) {
        size_t read_num = 0;
        auto buffer_pos = get_buffer_pos(offset);
        RETURN_IF_ERROR(
                _pre_buffers[buffer_pos]->read_buffer(offset, result.get_data() + actual_bytes_read,
                                                      nbytes - actual_bytes_read, &read_num));
        actual_bytes_read += read_num;
        offset += read_num;
    }
    *bytes_read = actual_bytes_read;
    return Status::OK();
}

Status PrefetchBufferedReader::close() {
    return _close_internal();
}

Status PrefetchBufferedReader::_close_internal() {
    if (!_closed) {
        _closed = true;
        std::for_each(_pre_buffers.begin(), _pre_buffers.end(),
                      [](std::shared_ptr<PrefetchBuffer>& buffer) { buffer->close(); });
        return _reader->close();
    }

    return Status::OK();
}

void PrefetchBufferedReader::_collect_profile_before_close() {
    std::for_each(_pre_buffers.begin(), _pre_buffers.end(),
                  [](std::shared_ptr<PrefetchBuffer>& buffer) {
                      buffer->collect_profile_before_close();
                  });
    if (_reader != nullptr) {
        _reader->collect_profile_before_close();
    }
}

// InMemoryFileReader
InMemoryFileReader::InMemoryFileReader(io::FileReaderSPtr reader) : _reader(std::move(reader)) {
    _size = _reader->size();
}

InMemoryFileReader::~InMemoryFileReader() {
    static_cast<void>(_close_internal());
}

Status InMemoryFileReader::close() {
    return _close_internal();
}

Status InMemoryFileReader::_close_internal() {
    if (!_closed) {
        _closed = true;
        return _reader->close();
    }
    return Status::OK();
}

Status InMemoryFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                        const IOContext* io_ctx) {
    if (_data == nullptr) {
        _data = std::make_unique_for_overwrite<char[]>(_size);

        size_t file_size = 0;
        RETURN_IF_ERROR(_reader->read_at(0, Slice(_data.get(), _size), &file_size, io_ctx));
        DCHECK_EQ(file_size, _size);
    }
    if (UNLIKELY(offset > _size)) {
        return Status::IOError("Out of bounds access");
    }
    *bytes_read = std::min(result.size, _size - offset);
    memcpy(result.data, _data.get() + offset, *bytes_read);
    return Status::OK();
}

void InMemoryFileReader::_collect_profile_before_close() {
    if (_reader != nullptr) {
        _reader->collect_profile_before_close();
    }
}

// BufferedFileStreamReader
BufferedFileStreamReader::BufferedFileStreamReader(io::FileReaderSPtr file, uint64_t offset,
                                                   uint64_t length, size_t max_buf_size)
        : _file(file),
          _file_start_offset(offset),
          _file_end_offset(offset + length),
          _max_buf_size(max_buf_size) {}

Status BufferedFileStreamReader::read_bytes(const uint8_t** buf, uint64_t offset,
                                            const size_t bytes_to_read, const IOContext* io_ctx) {
    if (offset < _file_start_offset || offset >= _file_end_offset) {
        return Status::IOError("Out-of-bounds Access");
    }
    int64_t end_offset = offset + bytes_to_read;
    if (_buf_start_offset <= offset && _buf_end_offset >= end_offset) {
        *buf = _buf.get() + offset - _buf_start_offset;
        return Status::OK();
    }
    size_t buf_size = std::max(_max_buf_size, bytes_to_read);
    if (_buf_size < buf_size) {
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[buf_size]);
        if (offset >= _buf_start_offset && offset < _buf_end_offset) {
            memcpy(new_buf.get(), _buf.get() + offset - _buf_start_offset,
                   _buf_end_offset - offset);
        }
        _buf = std::move(new_buf);
        _buf_size = buf_size;
    } else if (offset > _buf_start_offset && offset < _buf_end_offset) {
        memmove(_buf.get(), _buf.get() + offset - _buf_start_offset, _buf_end_offset - offset);
    }
    if (offset < _buf_start_offset || offset >= _buf_end_offset) {
        _buf_end_offset = offset;
    }
    _buf_start_offset = offset;
    int64_t buf_remaining = _buf_end_offset - _buf_start_offset;
    int64_t to_read = std::min(_buf_size - buf_remaining, _file_end_offset - _buf_end_offset);
    int64_t has_read = 0;
    SCOPED_RAW_TIMER(&_statistics.read_time);
    while (has_read < to_read) {
        size_t loop_read = 0;
        Slice result(_buf.get() + buf_remaining + has_read, to_read - has_read);
        RETURN_IF_ERROR(_file->read_at(_buf_end_offset + has_read, result, &loop_read, io_ctx));
        _statistics.read_calls++;
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != to_read) {
        return Status::Corruption("Try to read {} bytes, but received {} bytes", to_read, has_read);
    }
    _statistics.read_bytes += to_read;
    _buf_end_offset += to_read;
    *buf = _buf.get();
    return Status::OK();
}

Status BufferedFileStreamReader::read_bytes(Slice& slice, uint64_t offset,
                                            const IOContext* io_ctx) {
    return read_bytes((const uint8_t**)&slice.data, offset, slice.size, io_ctx);
}

Result<io::FileReaderSPtr> DelegateReader::create_file_reader(
        RuntimeProfile* profile, const FileSystemProperties& system_properties,
        const FileDescription& file_description, const io::FileReaderOptions& reader_options,
        AccessMode access_mode, const IOContext* io_ctx, const PrefetchRange file_range) {
    return FileFactory::create_file_reader(system_properties, file_description, reader_options,
                                           profile)
            .transform([&](auto&& reader) -> io::FileReaderSPtr {
                if (reader->size() < config::in_memory_file_size &&
                    typeid_cast<io::S3FileReader*>(reader.get())) {
                    return std::make_shared<InMemoryFileReader>(std::move(reader));
                }

                if (access_mode == AccessMode::SEQUENTIAL) {
                    bool is_thread_safe = false;
                    if (typeid_cast<io::S3FileReader*>(reader.get())) {
                        is_thread_safe = true;
                    } else if (auto* cached_reader =
                                       typeid_cast<io::CachedRemoteFileReader*>(reader.get());
                               cached_reader &&
                               typeid_cast<io::S3FileReader*>(cached_reader->get_remote_reader())) {
                        is_thread_safe = true;
                    }
                    if (is_thread_safe) {
                        // PrefetchBufferedReader needs thread-safe reader to prefetch data concurrently.
                        return std::make_shared<io::PrefetchBufferedReader>(
                                profile, std::move(reader), file_range, io_ctx);
                    }
                }

                return reader;
            });
}
} // namespace io
} // namespace doris
