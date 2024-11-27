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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/SharedHeader.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/util/Equators.h>
#include <CLucene/util/VoidMap.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

class CLuceneError;

namespace lucene::store {
class RAMDirectory;
} // namespace lucene::store

namespace doris {
class TabletIndex;
namespace segment_v2 {

class ReaderFileEntry : LUCENE_BASE {
public:
    std::string file_name {};
    int64_t offset;
    int64_t length;
    ReaderFileEntry() {
        //file_name = nullptr;
        offset = 0;
        length = 0;
    }
    ~ReaderFileEntry() override = default;
};

using EntriesType =
        lucene::util::CLHashMap<char*, ReaderFileEntry*, lucene::util::Compare::Char,
                                lucene::util::Equals::Char, lucene::util::Deletor::acArray,
                                lucene::util::Deletor::Object<ReaderFileEntry>>;
class CLUCENE_EXPORT DorisCompoundReader : public lucene::store::Directory {
private:
    lucene::store::RAMDirectory* _ram_dir = nullptr;
    CL_NS(store)::IndexInput* _stream = nullptr;
    EntriesType* _entries = nullptr;
    std::mutex _this_lock;
    bool _closed = false;
    int32_t _read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE;

protected:
    /** Removes an existing file in the directory-> */
    bool doDeleteFile(const char* name) override;

public:
    explicit DorisCompoundReader(
            CL_NS(store)::IndexInput* stream, EntriesType* entries_clone,
            int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE)
            : _stream(stream),
              _entries(_CLNEW EntriesType(true, true)),
              _read_buffer_size(read_buffer_size) {
        for (auto& e : *entries_clone) {
            auto* origin_entry = e.second;
            auto* entry = _CLNEW ReaderFileEntry();
            char* aid = strdup(e.first);
            entry->file_name = origin_entry->file_name;
            entry->offset = origin_entry->offset;
            entry->length = origin_entry->length;
            _entries->put(aid, entry);
        }
    };
    DorisCompoundReader(CL_NS(store)::IndexInput* stream,
                        int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    ~DorisCompoundReader() override;
    void copyFile(const char* file, int64_t file_length, uint8_t* buffer, int64_t buffer_length);
    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    bool openInput(const char* name, std::unique_ptr<lucene::store::IndexInput>& ret,
                   CLuceneError& err, int32_t bufferSize = -1);
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    std::string getPath() const;
    static const char* getClassName();
    const char* getObjectName() const override;
    CL_NS(store)::IndexInput* getDorisIndexInput();
};

} // namespace segment_v2
} // namespace doris
