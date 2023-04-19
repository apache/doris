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
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "CLucene/SharedHeader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "util/lock.h"

class CLuceneError;

namespace lucene {
namespace store {
class LockFactory;
} // namespace store
} // namespace lucene

namespace doris {

namespace segment_v2 {

class DorisCompoundFileWriter : LUCENE_BASE {
public:
    DorisCompoundFileWriter(CL_NS(store)::Directory* dir);
    ~DorisCompoundFileWriter() override = default;
    /** Returns the directory of the compound file. */
    CL_NS(store)::Directory* getDirectory();
    void writeCompoundFile();
    void copyFile(const char* fileName, lucene::store::IndexOutput* output, uint8_t* buffer,
                  int64_t bufferLength);

private:
    CL_NS(store)::Directory* directory;
};

class CLUCENE_EXPORT DorisCompoundDirectory : public lucene::store::Directory {
private:
    int filemode;

    doris::Mutex _this_lock;

protected:
    DorisCompoundDirectory();
    virtual void init(const io::FileSystemSPtr& fs, const char* path,
                      lucene::store::LockFactory* lock_factory = nullptr,
                      const io::FileSystemSPtr& compound_fs = nullptr,
                      const char* cfs_path = nullptr);
    void priv_getFN(char* buffer, const char* name) const;

private:
    io::FileSystemSPtr fs;
    io::FileSystemSPtr compound_fs;
    std::string directory;
    std::string cfs_directory;
    void create();
    static bool disableLocks;
    bool useCompoundFileWriter {false};

protected:
    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

public:
    class FSIndexOutput;
    class FSIndexInput;

    friend class DorisCompoundDirectory::FSIndexOutput;
    friend class DorisCompoundDirectory::FSIndexInput;

    const io::FileSystemSPtr& getFileSystem() { return fs; }
    const io::FileSystemSPtr& getCompoundFileSystem() { return compound_fs; }
    ~DorisCompoundDirectory() override;

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    const char* getCfsDirName() const;
    static DorisCompoundDirectory* getDirectory(const io::FileSystemSPtr& fs, const char* file,
                                                lucene::store::LockFactory* lock_factory = nullptr,
                                                const io::FileSystemSPtr& cfs_fs = nullptr,
                                                const char* cfs_file = nullptr);

    static DorisCompoundDirectory* getDirectory(const io::FileSystemSPtr& fs, const char* file,
                                                bool use_compound_file_writer,
                                                const io::FileSystemSPtr& cfs_fs = nullptr,
                                                const char* cfs_file = nullptr);

    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    static const char* getClassName();
    const char* getObjectName() const override;
    bool deleteDirectory();
};

class DorisCompoundDirectory::FSIndexInput : public lucene::store::BufferedIndexInput {
    class SharedHandle : LUCENE_REFBASE {
    public:
        io::FileReaderSPtr _reader;
        uint64_t _length;
        int64_t _fpos;
        doris::Mutex* _shared_lock;
        char path[4096];
        SharedHandle(const char* path);
        ~SharedHandle() override;
    };

    SharedHandle* _handle;
    int64_t _pos;

    FSIndexInput(SharedHandle* handle, int32_t buffer_size) : BufferedIndexInput(buffer_size) {
        this->_pos = 0;
        this->_handle = handle;
    }

protected:
    FSIndexInput(const FSIndexInput& clone);

public:
    static bool open(const io::FileSystemSPtr& fs, const char* path, IndexInput*& ret,
                     CLuceneError& error, int32_t bufferSize = -1);
    ~FSIndexInput() override;

    IndexInput* clone() const override;
    void close() override;
    int64_t length() const override { return _handle->_length; }

    const char* getDirectoryType() const override { return DorisCompoundDirectory::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }
    static const char* getClassName() { return "FSIndexInput"; }

    doris::Mutex _this_lock;

protected:
    // Random-access methods
    void seekInternal(const int64_t position) override;
    // IndexInput methods
    void readInternal(uint8_t* b, const int32_t len) override;
};

} // namespace segment_v2
} // namespace doris
