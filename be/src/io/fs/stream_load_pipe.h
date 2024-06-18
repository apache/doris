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

#include <gen_cpp/internal_service.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/message_body_sink.h"
#include "util/byte_buffer.h"
#include "util/slice.h"

namespace doris {
namespace io {
struct IOContext;

static inline constexpr size_t kMaxPipeBufferedBytes = 4 * 1024 * 1024;

class StreamLoadPipe : public MessageBodySink, public FileReader {
public:
    StreamLoadPipe(size_t max_buffered_bytes = kMaxPipeBufferedBytes,
                   size_t min_chunk_size = 64 * 1024, int64_t total_length = -1,
                   bool use_proto = false);
    ~StreamLoadPipe() override;

    Status append_and_flush(const char* data, size_t size, size_t proto_byte_size = 0);

    Status append(std::unique_ptr<PDataRow>&& row);
    Status append(const char* data, size_t size) override;
    Status append(const ByteBufferPtr& buf) override;

    const Path& path() const override { return _path; }

    size_t size() const override { return 0; }

    // called when consumer finished
    Status close() override {
        if (!(_finished || _cancelled)) {
            cancel("closed");
        }
        return Status::OK();
    }

    bool closed() const override { return _cancelled; }

    // called when producer finished
    virtual Status finish() override;

    // called when producer/consumer failed
    virtual void cancel(const std::string& reason) override;

    Status read_one_message(std::unique_ptr<uint8_t[]>* data, size_t* length);

    size_t get_queue_size() { return _buf_queue.size(); }

    // used for pipeline load, which use TUniqueId(lo: query_id.lo + fragment_id, hi: query_id.hi) as pipe_id
    static TUniqueId calculate_pipe_id(const UniqueId& query_id, int32_t fragment_id);

    size_t max_capacity() const { return _max_buffered_bytes; }

    size_t current_capacity();

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    // read the next buffer from _buf_queue
    Status _read_next_buffer(std::unique_ptr<uint8_t[]>* data, size_t* length);

    Status _append(const ByteBufferPtr& buf, size_t proto_byte_size = 0);

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes;
    size_t _proto_buffered_bytes;
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;
    // The total amount of data expected to be read.
    // In some scenarios, such as loading json format data through stream load,
    // the data needs to be completely read before it can be parsed,
    // so the total size of the data needs to be known.
    // The default is -1, which means that the data arrives in a stream
    // and the length is unknown.
    // size_t is unsigned, so use int64_t
    int64_t _total_length = -1;
    bool _use_proto = false;
    std::deque<ByteBufferPtr> _buf_queue;
    std::deque<std::unique_ptr<PDataRow>> _data_row_ptrs;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;

    ByteBufferPtr _write_buf;

    // no use, only for compatibility with the `Path` interface
    Path _path = "";
};
} // namespace io
} // namespace doris
