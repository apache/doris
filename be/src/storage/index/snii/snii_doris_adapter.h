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

#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/io_common.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "util/slice.h"

namespace doris {
class ThreadPool;
} // namespace doris

namespace doris::segment_v2::snii_doris {

class DorisSniiFileWriter final : public ::doris::snii::io::FileWriter {
public:
    explicit DorisSniiFileWriter(io::FileWriter* writer) : _writer(writer) {}

    Status append(::doris::snii::Slice data) override;
    Status finalize() override;
    uint64_t bytes_written() const override;

private:
    io::FileWriter* _writer = nullptr;
};

class DorisSniiFileReader final : public ::doris::snii::io::FileReader {
public:
    class ScopedIOContext {
    public:
        explicit ScopedIOContext(const io::IOContext* io_ctx);
        ~ScopedIOContext();

        ScopedIOContext(const ScopedIOContext&) = delete;
        ScopedIOContext& operator=(const ScopedIOContext&) = delete;

    private:
        const io::IOContext* _previous = nullptr;
        io::IOContext _io_ctx;
    };

    explicit DorisSniiFileReader(io::FileReaderSPtr reader, const io::IOContext* io_ctx = nullptr);

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override;
    Status read_batch(const std::vector<::doris::snii::io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override;
    uint64_t size() const override;

    // Test-only: inject (or clear with nullptr) the thread pool used to fan out
    // batch segment reads. nullptr (default) routes to the BE buffered-reader
    // prefetch pool when ExecEnv is ready, otherwise a serial fallback.
    static void set_io_thread_pool_for_test(ThreadPool* pool);

private:
    static io::IOContext _make_index_io_context(const io::IOContext* io_ctx);
    Status _check_read_range(uint64_t offset, size_t len) const;
    Status _read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out,
                    const io::IOContext* io_ctx) const;
    const io::IOContext* _current_io_ctx() const;
    void _record_read_stats(int64_t request_bytes, int64_t read_bytes, int64_t range_read_count,
                            int64_t serial_read_rounds) const;

    // Selects the executor for parallel batch segment reads: the test seam if
    // set, else the BE prefetch pool when ExecEnv is ready, else nullptr (the
    // caller then reads segments serially).
    static ThreadPool* _select_io_pool();
    // Number of dependent serial dispatch waves for `seg_count` physical
    // segments given the per-call concurrency cap. This is the F19 metric:
    // a coalesced batch of <= cap segments is a single concurrent round.
    static size_t _compute_num_waves(size_t seg_count);
    // Sums every counter of `src` into `dst`. Per-segment reads accumulate file
    // cache statistics into private slots (so disjoint physical reads never race
    // on the shared sink); this folds them back. MUST stay in sync with
    // io::FileCacheStatistics (be/src/io/io_common.h) when fields are added.
    static void _merge_file_cache_statistics(io::FileCacheStatistics* dst,
                                             const io::FileCacheStatistics& src);

    io::FileReaderSPtr _reader;
    io::IOContext _default_io_ctx;
    static thread_local const io::IOContext* _scoped_io_ctx;
    // Test seam for the batch-read executor; see set_io_thread_pool_for_test.
    static ThreadPool* _io_pool_for_test;
};

} // namespace doris::segment_v2::snii_doris
