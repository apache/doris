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

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

class CLuceneError;

namespace lucene::store {
class RAMDirectory;
} // namespace lucene::store

namespace doris::segment_v2 {

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

// Modern C++ using std::unordered_map with smart pointers for automatic memory management
using EntriesType = std::unordered_map<std::string, std::unique_ptr<ReaderFileEntry>>;

class CLUCENE_EXPORT DorisCompoundReader : public lucene::store::Directory {
private:
    std::unique_ptr<lucene::store::RAMDirectory> _ram_dir;
    CL_NS(store)::IndexInput* _stream = nullptr;
    // The life cycle of _entries should be consistent with that of the DorisCompoundReader.
    std::unique_ptr<EntriesType> _entries;
    std::mutex _this_lock;
    bool _closed = false;
    int32_t _read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE;
    void _copyFile(const char* file, int32_t file_length, uint8_t* buffer, int32_t buffer_length);

protected:
    /** Removes an existing file in the directory-> */
    bool doDeleteFile(const char* name) override;

public:
    DorisCompoundReader(CL_NS(store)::IndexInput* stream, const EntriesType& entries_clone,
                        int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE,
                        const io::IOContext* io_ctx = nullptr);
    DorisCompoundReader(CL_NS(store)::IndexInput* stream,
                        int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE,
                        const io::IOContext* io_ctx = nullptr);
    ~DorisCompoundReader() override;
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

private:
    void initialize(const io::IOContext* io_ctx);
};

} // namespace doris::segment_v2
