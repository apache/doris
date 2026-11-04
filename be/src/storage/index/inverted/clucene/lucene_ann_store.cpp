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

#include "storage/index/inverted/clucene/lucene_ann_store.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/debug/error.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <fmt/format.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "common/logging.h"
#include "storage/index/index_common.h"

namespace doris::segment_v2 {

namespace {

// CLucene takes every length as an int32_t, so a longer read or write goes in pieces of this size.
constexpr size_t kMaxCluceneBytesPerCall = static_cast<size_t>(std::numeric_limits<int32_t>::max());

class LuceneAnnInput final : public AnnIndexInput {
public:
    LuceneAnnInput(std::string name, std::unique_ptr<lucene::store::IndexInput> input)
            : _name(std::move(name)), _input(std::move(input)) {}

    ~LuceneAnnInput() override { _input->close(); }

    uint64_t length() const override { return static_cast<uint64_t>(_input->length()); }

    Status read_at(uint64_t offset, void* out, size_t size, const io::IOContext* io_ctx) override {
        auto* bytes = static_cast<uint8_t*>(out);
        ErrorContext error_context;
        // Bound for this read only. A compound input hands the context to the reader's shared file
        // stream around each refill, so it must not keep a context that outlives its query.
        _input->setIoContext(io_ctx);
        try {
            // A seek inside the buffer only moves the cursor, and one at its end only marks it
            // empty, so a file read front to back through read_at refills exactly where readBytes
            // alone would. The buffer also enforces the length: reading past it throws.
            _input->seek(static_cast<int64_t>(offset));
            for (size_t done = 0; done < size;) {
                const size_t piece = std::min(size - done, kMaxCluceneBytesPerCall);
                _input->readBytes(bytes + done, static_cast<int32_t>(piece));
                done += piece;
            }
        } catch (const CLuceneError& e) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = fmt::format("Failed to read {} bytes at offset {} of {}: {}",
                                                size, offset, _name, e.what());
        }
        FINALLY({ _input->setIoContext(nullptr); })
        return Status::OK();
    }

private:
    std::string _name;
    std::unique_ptr<lucene::store::IndexInput> _input;
};

class LuceneAnnOutput final : public AnnIndexOutput {
public:
    LuceneAnnOutput(std::string name, std::unique_ptr<lucene::store::IndexOutput> output)
            : _name(std::move(name)), _output(std::move(output)) {}

    // Still open only when the serialization failed, and that failure is already on its way back.
    // A destructor must not turn the I/O error of this cleanup into std::terminate.
    ~LuceneAnnOutput() override {
        if (Status status = LuceneAnnOutput::close(); !status.ok()) {
            LOG(WARNING) << "Failed to close an ANN output left open by a failed write: "
                         << status.msg();
        }
    }

    Status append(const void* data, size_t size) override {
        DCHECK(!_closed) << _name;
        const auto* bytes = static_cast<const uint8_t*>(data);
        try {
            for (size_t done = 0; done < size;) {
                const size_t piece = std::min(size - done, kMaxCluceneBytesPerCall);
                _output->writeBytes(bytes + done, static_cast<int32_t>(piece));
                done += piece;
            }
        } catch (const CLuceneError& e) {
            return clucene_error_status(fmt::format("Failed to write {}: {}", _name, e.what()));
        }
        return Status::OK();
    }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        // Marked before the attempt: a failed close is not retried here. The CLucene output still
        // runs its own cleanup when it is deleted.
        _closed = true;
        try {
            _output->close();
        } catch (const CLuceneError& e) {
            return clucene_error_status(fmt::format("Failed to close {}: {}", _name, e.what()));
        }
        return Status::OK();
    }

private:
    std::string _name;
    std::unique_ptr<lucene::store::IndexOutput> _output;
    bool _closed = false;
};

} // namespace

LuceneAnnSource::LuceneAnnSource(std::shared_ptr<lucene::store::Directory> dir)
        : _dir(std::move(dir)) {}

Result<std::unique_ptr<AnnIndexInput>> LuceneAnnSource::open_input(std::string_view name) {
    std::string file_name(name);
    std::unique_ptr<lucene::store::IndexInput> input;
    try {
        input.reset(_dir->openInput(file_name.c_str()));
    } catch (const CLuceneError& e) {
        return ResultError(clucene_error_status(
                fmt::format("Failed to open index file: {}, error: {}", file_name, e.what())));
    }
    return std::make_unique<LuceneAnnInput>(std::move(file_name), std::move(input));
}

std::string LuceneAnnSource::debug_string() const {
    return _dir->toString();
}

LuceneAnnSink::LuceneAnnSink(std::shared_ptr<lucene::store::Directory> dir)
        : _dir(std::move(dir)) {}

Result<std::unique_ptr<AnnIndexOutput>> LuceneAnnSink::create_output(std::string_view name) {
    std::string file_name(name);
    std::unique_ptr<lucene::store::IndexOutput> output;
    try {
        output.reset(_dir->createOutput(file_name.c_str()));
    } catch (const CLuceneError& e) {
        return ResultError(clucene_error_status(fmt::format(
                "Failed to create {} in {}: {}", file_name, _dir->toString(), e.what())));
    }
    return std::make_unique<LuceneAnnOutput>(std::move(file_name), std::move(output));
}

std::string LuceneAnnSink::debug_string() const {
    return _dir->toString();
}

} // namespace doris::segment_v2
