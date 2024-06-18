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

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

#include "CLucene/SharedHeader.h"
#include "CLucene/_SharedHeader.h"
#include "common/status.h"
#include "inverted_index_desc.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"
#include "util/slice.h"

#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <CLucene/LuceneThreads.h>
#include <CLucene/clucene-config.h>
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/store/LockFactory.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <utility>

#define CL_MAX_PATH 4096
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

#define LOG_AND_THROW_IF_ERROR(status, msg)                                  \
    do {                                                                     \
        auto _status_result = (status);                                      \
        if (!_status_result.ok()) {                                          \
            auto err = std::string(msg) + ": " + _status_result.to_string(); \
            LOG(WARNING) << err;                                             \
            _CLTHROWA(CL_ERR_IO, err.c_str());                               \
        }                                                                    \
    } while (0)

namespace doris::segment_v2 {

const char* const DorisFSDirectory::WRITE_LOCK_FILE = "write.lock";
const char* const DorisFSDirectory::COMPOUND_FILE_EXTENSION = ".idx";

class DorisFSDirectory::FSIndexOutput : public lucene::store::BufferedIndexOutput {
private:
    io::FileWriterPtr _writer;

protected:
    void flushBuffer(const uint8_t* b, const int32_t size) override;

public:
    FSIndexOutput() = default;
    void init(const io::FileSystemSPtr& fs, const char* path);
    ~FSIndexOutput() override;
    void close() override;
    int64_t length() const override;
};

bool DorisFSDirectory::FSIndexInput::open(const io::FileSystemSPtr& fs, const char* path,
                                          IndexInput*& ret, CLuceneError& error,
                                          int32_t buffer_size) {
    CND_PRECONDITION(path != nullptr, "path is NULL");

    if (buffer_size == -1) {
        buffer_size = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    }
    auto h = std::make_shared<SharedHandle>(path);

    io::FileReaderOptions reader_options;
    reader_options.cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                          : io::FileCachePolicy::NO_CACHE;
    reader_options.is_doris_table = true;
    if (!fs->open_file(path, &h->_reader, &reader_options).ok()) {
        error.set(CL_ERR_IO, "open file error");
    }

    //Check if a valid handle was retrieved
    if (h->_reader) {
        //Store the file length
        h->_length = h->_reader->size();
        h->_fpos = 0;
        ret = _CLNEW FSIndexInput(std::move(h), buffer_size);
        return true;

    } else {
        int err = errno;
        if (err == ENOENT) {
            error.set(CL_ERR_IO, "File does not exist");
        } else if (err == EACCES) {
            error.set(CL_ERR_IO, "File Access denied");
        } else if (err == EMFILE) {
            error.set(CL_ERR_IO, "Too many open files");
        } else {
            error.set(CL_ERR_IO, "Could not open file");
        }
    }
    //delete h->_shared_lock;
    //_CLDECDELETE(h)
    return false;
}

DorisFSDirectory::FSIndexInput::FSIndexInput(const FSIndexInput& other)
        : BufferedIndexInput(other) {
    if (other._handle == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "other handle is null");
    }

    std::lock_guard<std::mutex> wlock(other._handle->_shared_lock);
    _handle = other._handle;
    _pos = other._handle->_fpos; //note where we are currently...
    _io_ctx = other._io_ctx;
}

DorisFSDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    _length = 0;
    _fpos = 0;
    strcpy(this->path, path);
    //_shared_lock = new std::mutex();
}

DorisFSDirectory::FSIndexInput::SharedHandle::~SharedHandle() {
    if (_reader) {
        if (_reader->close().ok()) {
            _reader = nullptr;
        }
    }
}

DorisFSDirectory::FSIndexInput::~FSIndexInput() {
    FSIndexInput::close();
}

lucene::store::IndexInput* DorisFSDirectory::FSIndexInput::clone() const {
    return _CLNEW DorisFSDirectory::FSIndexInput(*this);
}
void DorisFSDirectory::FSIndexInput::close() {
    BufferedIndexInput::close();
    /*if (_handle != nullptr) {
        std::mutex* lock = _handle->_shared_lock;
        bool ref = false;
        {
            std::lock_guard<std::mutex> wlock(*lock);
            //determine if we are about to delete the handle...
            ref = (_LUCENE_ATOMIC_INT_GET(_handle->__cl_refcount) > 1);
            //decdelete (deletes if refcount is down to 0
            _CLDECDELETE(_handle);
        }

        //if _handle is not ref by other FSIndexInput, try to release mutex lock, or it will be leaked.
        if (!ref) {
            delete lock;
        }
    }*/
}

void DorisFSDirectory::FSIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < _handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void DorisFSDirectory::FSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(_handle != nullptr, "shared file handle has closed");
    CND_PRECONDITION(_handle->_reader != nullptr, "file is not open");
    std::lock_guard<std::mutex> wlock(_handle->_shared_lock);

    int64_t position = getFilePointer();
    if (_pos != position) {
        _pos = position;
    }

    if (_handle->_fpos != _pos) {
        _handle->_fpos = _pos;
    }

    Slice result {b, (size_t)len};
    size_t bytes_read = 0;
    if (!_handle->_reader->read_at(_pos, result, &bytes_read, &_io_ctx).ok()) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    bufferLength = len;
    if (bytes_read != len) {
        _CLTHROWA(CL_ERR_IO, "read error");
    }
    _pos += bufferLength;
    _handle->_fpos = _pos;
}

void DorisFSDirectory::FSIndexOutput::init(const io::FileSystemSPtr& fs, const char* path) {
    Status status = fs->create_file(path, &_writer);
    DBUG_EXECUTE_IF(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_"
            "init",
            {
                status = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "debug point: test throw error in fsindexoutput init mock error");
            })
    if (!status.ok()) {
        _writer.reset(nullptr);
        auto err = "Create compound file error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
}

DorisFSDirectory::FSIndexOutput::~FSIndexOutput() {
    if (_writer) {
        try {
            FSIndexOutput::close();
            DBUG_EXECUTE_IF(
                    "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_"
                    "destructor",
                    {
                        _CLTHROWA(CL_ERR_IO,
                                  "debug point: test throw error in fsindexoutput destructor");
                    })
        } catch (CLuceneError& err) {
            //ignore errors...
            LOG(WARNING) << "FSIndexOutput deconstruct error: " << err.what();
        }
    }
}

void DorisFSDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (_writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_"
                "flushBuffer",
                {
                    if (_writer->path().filename() == "_0.tii" ||
                        _writer->path().filename() == "_0.tis") {
                        return;
                    }
                })
        Status st = _writer->append(data);
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer", {
                    st = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "flush buffer mock error");
                })
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
            _CLTHROWA(CL_ERR_IO, "writer append data when flushBuffer error");
        }
    } else {
        if (_writer == nullptr) {
            LOG(WARNING) << "File writer is nullptr in DorisFSDirectory::FSIndexOutput, "
                            "ignore flush.";
        } else if (b == nullptr) {
            LOG(WARNING) << "buffer is nullptr when flushBuffer in "
                            "DorisFSDirectory::FSIndexOutput";
        }
    }
}

void DorisFSDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_"
                "close",
                {
                    _CLTHROWA(CL_ERR_IO,
                              "debug point: test throw error in bufferedindexoutput close");
                })
    } catch (CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close error: " << err.what();
        if (err.number() == CL_ERR_IO) {
            LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close IO error: "
                         << err.what();
        }
        _writer.reset(nullptr);
        _CLTHROWA(err.number(), err.what());
    }
    if (_writer) {
        auto ret = _writer->close();
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error",
                        { ret = Status::Error<INTERNAL_ERROR>("writer close status error"); })
        if (!ret.ok()) {
            LOG(WARNING) << "FSIndexOutput close, file writer close error: " << ret.to_string();
            _writer.reset(nullptr);
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
    }
    _writer.reset(nullptr);
}

int64_t DorisFSDirectory::FSIndexOutput::length() const {
    CND_PRECONDITION(_writer != nullptr, "file is not open");
    return _writer->bytes_appended();
}

DorisFSDirectory::DorisFSDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void DorisFSDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
                            bool use_compound_file_writer, lucene::store::LockFactory* lock_factory,
                            const io::FileSystemSPtr& cfs, const char* cfs_path) {
    fs = _fs;
    directory = _path;
    useCompoundFileWriter = use_compound_file_writer;

    if (cfs == nullptr) {
        compound_fs = fs;
    } else {
        compound_fs = cfs;
    }
    if (cfs_path != nullptr) {
        cfs_directory = cfs_path;
    } else {
        cfs_directory = _path;
    }

    if (lock_factory == nullptr) {
        lock_factory = _CLNEW lucene::store::NoLockFactory();
    }

    lucene::store::Directory::setLockFactory(lock_factory);

    // It's fail checking directory existence in S3.
    if (fs->type() == io::FileSystemType::S3) {
        return;
    }
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(directory, &exists),
                           "Doris compound directory init IO error");
    if (!exists) {
        auto e = "Doris compound directory init error: " + directory + " is not a directory";
        LOG(WARNING) << e;
        _CLTHROWA(CL_ERR_IO, e.c_str());
    }
}

void DorisFSDirectory::priv_getFN(char* buffer, const char* name) const {
    buffer[0] = 0;
    strcpy(buffer, directory.c_str());
    strcat(buffer, PATH_DELIMITERA);
    strcat(buffer, name);
}

DorisFSDirectory::~DorisFSDirectory() = default;

const char* DorisFSDirectory::getClassName() {
    return "DorisFSDirectory";
}
const char* DorisFSDirectory::getObjectName() const {
    return getClassName();
}

bool DorisFSDirectory::list(std::vector<std::string>* names) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    std::vector<io::FileInfo> files;
    bool exists;
    LOG_AND_THROW_IF_ERROR(fs->list(fl, true, &files, &exists), "List file IO error");
    for (auto& file : files) {
        names->push_back(file.file_name);
    }
    return true;
}

bool DorisFSDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "File exists IO error");
    return exists;
}

const char* DorisFSDirectory::getCfsDirName() const {
    return cfs_directory.c_str();
}

const std::string& DorisFSDirectory::getDirName() const {
    return directory;
}

int64_t DorisFSDirectory::fileModified(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    struct stat buf;
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    if (stat(buffer, &buf) == -1) {
        return 0;
    } else {
        return buf.st_mtime;
    }
}

void DorisFSDirectory::touchFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    snprintf(buffer, CL_MAX_DIR, "%s%s%s", directory.c_str(), PATH_DELIMITERA, name);

    io::FileWriterPtr tmp_writer;
    LOG_AND_THROW_IF_ERROR(fs->create_file(buffer, &tmp_writer), "Touch file IO error");
}

int64_t DorisFSDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    int64_t size = -1;
    LOG_AND_THROW_IF_ERROR(fs->file_size(buffer, &size), "Get file size IO error");
    return size;
}

bool DorisFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                 CLuceneError& error, int32_t bufferSize) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    return FSIndexInput::open(fs, fl, ret, error, bufferSize);
}

void DorisFSDirectory::close() {}

bool DorisFSDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    LOG_AND_THROW_IF_ERROR(fs->delete_file(fl), "Delete file IO error");
    return true;
}

bool DorisFSDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    LOG_AND_THROW_IF_ERROR(fs->delete_directory(fl),
                           fmt::format("Delete directory {} IO error", fl));
    return true;
}

void DorisFSDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::lock_guard<std::mutex> wlock(_this_lock);
    char old[CL_MAX_DIR];
    priv_getFN(old, from);

    char nu[CL_MAX_DIR];
    priv_getFN(nu, to);

    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(nu, &exists), "File exists IO error");
    if (exists) {
        LOG_AND_THROW_IF_ERROR(fs->delete_directory(nu), fmt::format("Delete {} IO error", nu));
    }
    LOG_AND_THROW_IF_ERROR(fs->rename(old, nu), fmt::format("Rename {} to {} IO error", old, nu));
}

lucene::store::IndexOutput* DorisFSDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "Create output file exists IO error");
    if (exists) {
        LOG_AND_THROW_IF_ERROR(fs->delete_file(fl),
                               fmt::format("Create output delete file {} IO error", fl));
        LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "Create output file exists IO error");
        assert(!exists);
    }
    auto* ret = _CLNEW FSIndexOutput();
    try {
        ret->init(fs, fl);
    } catch (CLuceneError& err) {
        ret->close();
        _CLDELETE(ret)
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

std::string DorisFSDirectory::toString() const {
    return std::string("DorisFSDirectory@") + this->directory;
}

DorisRAMFSDirectory::DorisRAMFSDirectory() {
    filesMap = _CLNEW FileMap(true, true);
    this->sizeInBytes = 0;
}

DorisRAMFSDirectory::~DorisRAMFSDirectory() {
    std::lock_guard<std::mutex> wlock(_this_lock);
    filesMap->clear();
    _CLDELETE(lockFactory);
    _CLDELETE(filesMap);
}

void DorisRAMFSDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
                               bool use_compound_file_writer,
                               lucene::store::LockFactory* lock_factory,
                               const io::FileSystemSPtr& cfs, const char* cfs_path) {
    fs = _fs;
    directory = _path;
    useCompoundFileWriter = use_compound_file_writer;

    if (cfs == nullptr) {
        compound_fs = fs;
    } else {
        compound_fs = cfs;
    }
    if (cfs_path != nullptr) {
        cfs_directory = cfs_path;
    } else {
        cfs_directory = _path;
    }

    lucene::store::Directory::setLockFactory(_CLNEW lucene::store::SingleInstanceLockFactory());
}

bool DorisRAMFSDirectory::list(std::vector<std::string>* names) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->begin();
    while (itr != filesMap->end()) {
        names->emplace_back(itr->first);
        ++itr;
    }
    return true;
}

bool DorisRAMFSDirectory::fileExists(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    return filesMap->exists((char*)name);
}

int64_t DorisRAMFSDirectory::fileModified(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    return f->getLastModified();
}

void DorisRAMFSDirectory::touchFile(const char* name) {
    lucene::store::RAMFile* file = nullptr;
    {
        std::lock_guard<std::mutex> wlock(_this_lock);
        file = filesMap->get((char*)name);
    }
    const uint64_t ts1 = file->getLastModified();
    uint64_t ts2 = lucene::util::Misc::currentTimeMillis();

    //make sure that the time has actually changed
    while (ts1 == ts2) {
        _LUCENE_SLEEP(1);
        ts2 = lucene::util::Misc::currentTimeMillis();
    };

    file->setLastModified(ts2);
}

int64_t DorisRAMFSDirectory::fileLength(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    return f->getLength();
}

bool DorisRAMFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                    CLuceneError& error, int32_t bufferSize) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* file = filesMap->get((char*)name);
    if (file == nullptr) {
        error.set(CL_ERR_IO,
                  "[DorisRAMCompoundDirectory::open] The requested file does not exist.");
        return false;
    }
    ret = _CLNEW lucene::store::RAMInputStream(file);
    return true;
}

void DorisRAMFSDirectory::close() {
    DorisFSDirectory::close();
}

bool DorisRAMFSDirectory::doDeleteFile(const char* name) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->find((char*)name);
    if (itr != filesMap->end()) {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr->second->sizeInBytes;
        filesMap->removeitr(itr);
        return true;
    } else {
        return false;
    }
}

bool DorisRAMFSDirectory::deleteDirectory() {
    // do nothing, RAM dir do not have actual files
    return true;
}

void DorisRAMFSDirectory::renameFile(const char* from, const char* to) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->find((char*)from);

    /* DSR:CL_BUG_LEAK:
    ** If a file named $to already existed, its old value was leaked.
    ** My inclination would be to prevent this implicit deletion with an
    ** exception, but it happens routinely in CLucene's internals (e.g., during
    ** IndexWriter.addIndexes with the file named 'segments'). */
    if (filesMap->exists((char*)to)) {
        auto itr1 = filesMap->find((char*)to);
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr1->second->sizeInBytes;
        filesMap->removeitr(itr1);
    }
    if (itr == filesMap->end()) {
        char tmp[1024];
        snprintf(tmp, 1024, "cannot rename %s, file does not exist", from);
        _CLTHROWT(CL_ERR_IO, tmp);
    }
    DCHECK(itr != filesMap->end());
    auto* file = itr->second;
    filesMap->removeitr(itr, false, true);
    filesMap->put(strdup(to), file);
}

lucene::store::IndexOutput* DorisRAMFSDirectory::createOutput(const char* name) {
    /* Check the $filesMap VoidMap to see if there was a previous file named
    ** $name.  If so, delete the old RAMFile object, but reuse the existing
    ** char buffer ($n) that holds the filename.  If not, duplicate the
    ** supplied filename buffer ($name) and pass ownership of that memory ($n)
    ** to $files. */
    std::lock_guard<std::mutex> wlock(_this_lock);

    // get the actual pointer to the output name
    char* n = nullptr;
    auto itr = filesMap->find(const_cast<char*>(name));
    if (itr != filesMap->end()) {
        n = itr->first;
        lucene::store::RAMFile* rf = itr->second;
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= rf->sizeInBytes;
        _CLDELETE(rf);
    } else {
        n = STRDUP_AtoA(name);
    }

    auto* file = _CLNEW lucene::store::RAMFile();
    (*filesMap)[n] = file;

    return _CLNEW lucene::store::RAMOutputStream(file);
}

std::string DorisRAMFSDirectory::toString() const {
    return std::string("DorisRAMFSDirectory@") + this->directory;
}

const char* DorisRAMFSDirectory::getClassName() {
    return "DorisRAMFSDirectory";
}

const char* DorisRAMFSDirectory::getObjectName() const {
    return getClassName();
}

DorisFSDirectory* DorisFSDirectoryFactory::getDirectory(
        const io::FileSystemSPtr& _fs, const char* _file, bool use_compound_file_writer,
        bool can_use_ram_dir, lucene::store::LockFactory* lock_factory,
        const io::FileSystemSPtr& _cfs, const char* _cfs_file) {
    const char* cfs_file = _cfs_file;
    if (cfs_file == nullptr) {
        cfs_file = _file;
    }
    DorisFSDirectory* dir = nullptr;
    if (!_file || !*_file) {
        _CLTHROWA(CL_ERR_IO, "Invalid directory");
    }

    const char* file = _file;

    // Write by RAM directory
    // 1. only write separated index files, which is can_use_ram_dir = true.
    // 2. config::inverted_index_ram_dir_enable = true
    if (config::inverted_index_ram_dir_enable && can_use_ram_dir) {
        dir = _CLNEW DorisRAMFSDirectory();
    } else {
        bool exists = false;
        LOG_AND_THROW_IF_ERROR(_fs->exists(file, &exists), "Get directory exists IO error");
        if (!exists) {
            LOG_AND_THROW_IF_ERROR(_fs->create_directory(file),
                                   "Get directory create directory IO error");
        }
        dir = _CLNEW DorisFSDirectory();
    }
    dir->init(_fs, file, use_compound_file_writer, lock_factory, _cfs, cfs_file);

    return dir;
}

} // namespace doris::segment_v2
