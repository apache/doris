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

// A CLucene directory for tests of the ANN storage adapters (lucene_ann_store.h): a RAMDirectory
// whose files can fail the way a disk does, or log the reads they send to their "file".

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace doris::segment_v2 {

// The production value of inverted_index_read_buffer_size, the buffer of every logging input.
inline constexpr int32_t kScriptedReadBufferSize = 4096;

struct ScriptedReadLog {
    struct Read {
        int64_t offset;
        int32_t length;
        const void* io_ctx;
    };
    std::vector<Read> reads;
    bool fail = false;
};

// A buffered input over bytes the test owns. It logs every read it sends to its file, with the IO
// context bound at the time: the reads FSIndexInput counts as range reads.
class LoggingInput final : public lucene::store::BufferedIndexInput {
public:
    LoggingInput(const std::vector<uint8_t>* bytes, ScriptedReadLog* log)
            : BufferedIndexInput(kScriptedReadBufferSize), _bytes(bytes), _log(log) {}
    LoggingInput(const LoggingInput& other) = default;

    lucene::store::IndexInput* clone() const override { return _CLNEW LoggingInput(*this); }
    int64_t length() const override { return static_cast<int64_t>(_bytes->size()); }
    const char* getDirectoryType() const override { return "ScriptedLuceneDirectory"; }
    const char* getObjectName() const override { return "LoggingInput"; }
    void setIoContext(const void* io_ctx) override { _io_ctx = io_ctx; }
    const void* getIoContext() override { return _io_ctx; }

protected:
    void readInternal(uint8_t* b, const int32_t len) override {
        if (_log->fail) {
            _CLTHROWA(CL_ERR_IO, "injected read failure");
        }
        const int64_t offset = getFilePointer();
        std::memcpy(b, _bytes->data() + offset, static_cast<size_t>(len));
        _log->reads.push_back({.offset = offset, .length = len, .io_ctx = _io_ctx});
    }
    void seekInternal(const int64_t /*pos*/) override {}

private:
    const std::vector<uint8_t>* _bytes;
    ScriptedReadLog* _log;
    const void* _io_ctx = nullptr;
};

// An output whose every flush fails, the way a full disk fails FSIndexOutput. Up to its buffer
// (BufferedIndexOutput::BUFFER_SIZE) appends succeed, so the failure comes from close().
class FailingOutput final : public lucene::store::BufferedIndexOutput {
public:
    // ~BufferedIndexOutput closes an open output through flushBuffer, which is pure virtual once
    // this subobject is gone; closing here first frees the base's buffer, so it does not.
    ~FailingOutput() override {
        try {
            FailingOutput::close();
        } catch (const CLuceneError&) {
            // The flush failure the test injected; nothing is left to clean up after it.
        }
    }
    int64_t length() const override { return 0; }

protected:
    void flushBuffer(const uint8_t* /*b*/, const int32_t size) override {
        if (size > 0) {
            _CLTHROWA(CL_ERR_IO, "injected flush failure");
        }
    }
};

// A RAMDirectory with scripted files: a name in logged_files reads its bytes through a
// LoggingInput, a name in broken_outputs writes through a FailingOutput, and creating any file
// fails while fail_create is set.
class ScriptedLuceneDirectory final : public lucene::store::RAMDirectory {
public:
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                   int32_t buffer_size) override {
        const auto logged = logged_files.find(name);
        if (logged != logged_files.end()) {
            ret = _CLNEW LoggingInput(&logged->second, &log);
            return true;
        }
        return RAMDirectory::openInput(name, ret, error, buffer_size);
    }

    lucene::store::IndexOutput* createOutput(const char* name) override {
        if (fail_create) {
            _CLTHROWA(CL_ERR_IO, "injected create failure");
        }
        if (broken_outputs.contains(name)) {
            return _CLNEW FailingOutput();
        }
        return RAMDirectory::createOutput(name);
    }

    std::map<std::string, std::vector<uint8_t>> logged_files;
    ScriptedReadLog log;
    std::set<std::string> broken_outputs;
    bool fail_create = false;
};

} // namespace doris::segment_v2
