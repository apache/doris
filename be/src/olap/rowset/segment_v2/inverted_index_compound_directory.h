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
#include <CLucene/store/_RAMDirectory.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "CLucene/SharedHeader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"

class CLuceneError;

namespace lucene::store {
class LockFactory;
} // namespace lucene::store

namespace doris::segment_v2 {

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
    class FileInfo {
    public:
        std::string filename;
        int32_t filesize;
    };

    void sort_files(std::vector<FileInfo>& file_infos);

    CL_NS(store)::Directory* directory = nullptr;
};

class CLUCENE_EXPORT DorisCompoundDirectory : public lucene::store::Directory {
private:
    int filemode;

protected:
    mutable std::mutex _this_lock;
    io::FileSystemSPtr fs;
    io::FileSystemSPtr compound_fs;
    std::string directory;
    std::string cfs_directory;
    bool useCompoundFileWriter {false};

    void priv_getFN(char* buffer, const char* name) const;
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
    virtual bool deleteDirectory();

    DorisCompoundDirectory();

    virtual void init(const io::FileSystemSPtr& fs, const char* path, bool use_compound_file_writer,
                      lucene::store::LockFactory* lock_factory = nullptr,
                      const io::FileSystemSPtr& compound_fs = nullptr,
                      const char* cfs_path = nullptr);
};

class CLUCENE_EXPORT DorisRAMCompoundDirectory : public DorisCompoundDirectory {
protected:
    using FileMap =
            lucene::util::CLHashMap<char*, lucene::store::RAMFile*, lucene::util::Compare::Char,
                                    lucene::util::Equals::Char, lucene::util::Deletor::acArray,
                                    lucene::util::Deletor::Object<lucene::store::RAMFile>>;

    // unlike the java Hashtable, FileMap is not synchronized, and all access must be protected by a lock
    FileMap* filesMap;
    void init(const io::FileSystemSPtr& fs, const char* path, bool use_compound_file_writer,
              lucene::store::LockFactory* lock_factory = nullptr,
              const io::FileSystemSPtr& compound_fs = nullptr,
              const char* cfs_path = nullptr) override;

public:
    int64_t sizeInBytes;

    /// Returns a null terminated array of strings, one for each file in the directory.
    bool list(std::vector<std::string>* names) const override;

    /** Constructs an empty {@link Directory}. */
    DorisRAMCompoundDirectory();

    ///Destructor - only call this if you are sure the directory
    ///is not being used anymore. Otherwise use the ref-counting
    ///facilities of dir->close
    ~DorisRAMCompoundDirectory() override;

    bool doDeleteFile(const char* name) override;

    bool deleteDirectory() override;

    /// Returns true iff the named file exists in this directory.
    bool fileExists(const char* name) const override;

    /// Returns the time the named file was last modified.
    int64_t fileModified(const char* name) const override;

    /// Returns the length in bytes of a file in the directory.
    int64_t fileLength(const char* name) const override;

    /// Removes an existing file in the directory.
    void renameFile(const char* from, const char* to) override;

    /** Set the modified time of an existing file to now. */
    void touchFile(const char* name) override;

    /// Creates a new, empty file in the directory with the given name.
    ///	Returns a stream writing this file.
    lucene::store::IndexOutput* createOutput(const char* name) override;

    /// Returns a stream reading an existing file.
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                   int32_t bufferSize = -1) override;

    void close() override;

    std::string toString() const override;

    static const char* getClassName();
    const char* getObjectName() const override;
};

class DorisCompoundDirectory::FSIndexInput : public lucene::store::BufferedIndexInput {
    class SharedHandle : LUCENE_REFBASE {
    public:
        io::FileReaderSPtr _reader;
        uint64_t _length;
        int64_t _fpos;
        std::mutex* _shared_lock = nullptr;
        char path[4096];
        SharedHandle(const char* path);
        ~SharedHandle() override;
    };

    SharedHandle* _handle = nullptr;
    int64_t _pos;
    io::IOContext _io_ctx;

    FSIndexInput(SharedHandle* handle, int32_t buffer_size) : BufferedIndexInput(buffer_size) {
        this->_pos = 0;
        this->_handle = handle;
        this->_io_ctx.reader_type = ReaderType::READER_QUERY;
        this->_io_ctx.is_index_data = false;
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

    void setIdxFileCache(bool index) override { _io_ctx.is_index_data = index; }

    std::mutex _this_lock;

protected:
    // Random-access methods
    void seekInternal(const int64_t position) override;
    // IndexInput methods
    void readInternal(uint8_t* b, const int32_t len) override;
};

/**
 * Factory function to create DorisCompoundDirectory
 */
class DorisCompoundDirectoryFactory {
public:
    static DorisCompoundDirectory* getDirectory(const io::FileSystemSPtr& fs, const char* file,
                                                bool use_compound_file_writer = false,
                                                bool can_use_ram_dir = false,
                                                lucene::store::LockFactory* lock_factory = nullptr,
                                                const io::FileSystemSPtr& cfs_fs = nullptr,
                                                const char* cfs_file = nullptr);
};
} // namespace doris::segment_v2