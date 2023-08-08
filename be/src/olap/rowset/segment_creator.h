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

#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_writer_context.h"
#include "util/spinlock.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

struct SegmentStatistics;
class BetaRowsetWriter;

class FileWriterCreator {
public:
    virtual ~FileWriterCreator() = default;

    virtual Status create(uint32_t segment_id, io::FileWriterPtr& file_writer) = 0;
};

template <class T>
class FileWriterCreatorT : public FileWriterCreator {
public:
    explicit FileWriterCreatorT(T* t) : _t(t) {}

    Status create(uint32_t segment_id, io::FileWriterPtr& file_writer) override {
        return _t->create_file_writer(segment_id, file_writer);
    }

private:
    T* _t;
};

class SegmentCollector {
public:
    virtual ~SegmentCollector() = default;

    virtual Status add(uint32_t segment_id, SegmentStatistics& segstat) = 0;
};

template <class T>
class SegmentCollectorT : public SegmentCollector {
public:
    explicit SegmentCollectorT(T* t) : _t(t) {}

    Status add(uint32_t segment_id, SegmentStatistics& segstat) override {
        return _t->add_segment(segment_id, segstat);
    }

private:
    T* _t;
};

class SegmentFlusher {
public:
    SegmentFlusher();

    ~SegmentFlusher();

    Status init(const RowsetWriterContext& rowset_writer_context);

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              int64_t* flush_size = nullptr,
                              TabletSchemaSPtr flush_schema = nullptr);

    int64_t num_rows_written() const { return _num_rows_written; }

    int64_t num_rows_filtered() const { return _num_rows_filtered; }

    Status close();

public:
    class Writer {
        friend class SegmentFlusher;

    public:
        ~Writer();

        Status add_rows(const vectorized::Block* block, size_t row_offset, size_t input_row_num) {
            return _flusher->_add_rows(_writer, block, row_offset, input_row_num);
        }

        Status flush();

        int64_t max_row_to_add(size_t row_avg_size_in_bytes);

    private:
        Writer(SegmentFlusher* flusher, std::unique_ptr<segment_v2::SegmentWriter>& segment_writer);

        SegmentFlusher* _flusher;
        std::unique_ptr<segment_v2::SegmentWriter> _writer;
    };

    Status create_writer(std::unique_ptr<SegmentFlusher::Writer>& writer, uint32_t segment_id);

private:
    Status _add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                     const vectorized::Block* block, size_t row_offset, size_t row_num);
    Status _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                  int32_t segment_id, bool no_compression = false,
                                  TabletSchemaSPtr flush_schema = nullptr);
    Status _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                 int64_t* flush_size = nullptr);

private:
    RowsetWriterContext _context;

    mutable SpinLock _lock; // protect following vectors.
    std::vector<io::FileWriterPtr> _file_writers;

    // written rows by add_block/add_row
    std::atomic<int64_t> _num_rows_written = 0;
    std::atomic<int64_t> _num_rows_filtered = 0;
};

class SegmentCreator {
public:
    SegmentCreator() = default;

    ~SegmentCreator() = default;

    Status init(const RowsetWriterContext& rowset_writer_context);

    void set_segment_start_id(uint32_t start_id) { _next_segment_id = start_id; }

    Status add_block(const vectorized::Block* block);

    Status flush();

    int32_t allocate_segment_id() { return _next_segment_id.fetch_add(1); }

    int32_t next_segment_id() const { return _next_segment_id.load(); }

    int64_t num_rows_written() const { return _segment_flusher.num_rows_written(); }

    int64_t num_rows_filtered() const { return _segment_flusher.num_rows_filtered(); }

    // Flush a block into a single segment, with pre-allocated segment_id.
    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              int64_t* flush_size = nullptr,
                              TabletSchemaSPtr flush_schema = nullptr);

    // Flush a block into a single segment, without pre-allocated segment_id.
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block) {
        return flush_single_block(block, allocate_segment_id());
    }

    Status close();

private:
    std::atomic<int32_t> _next_segment_id = 0;
    SegmentFlusher _segment_flusher;
    std::unique_ptr<SegmentFlusher::Writer> _flush_writer;
};

} // namespace doris
