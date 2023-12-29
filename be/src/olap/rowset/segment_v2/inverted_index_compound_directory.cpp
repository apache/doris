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

#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"

#include "CLucene/SharedHeader.h"
#include "CLucene/_SharedHeader.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
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

#define LOG_AND_THROW_IF_ERROR(status, msg)                      \
    if (!status.ok()) {                                          \
        auto err = std::string(msg) + ": " + status.to_string(); \
        LOG(WARNING) << err;                                     \
        _CLTHROWA(CL_ERR_IO, err.c_str());                       \
    }
namespace doris::segment_v2 {

const char* WRITE_LOCK_FILE = "write.lock";
const char* COMPOUND_FILE_EXTENSION = ".idx";
const int64_t MAX_HEADER_DATA_SIZE = 1024 * 128; // 128k

DorisCompoundFileWriter::DorisCompoundFileWriter(CL_NS(store)::Directory* dir) {
    if (dir == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "directory cannot be null");
    }

    directory = dir;
}

CL_NS(store)::Directory* DorisCompoundFileWriter::getDirectory() {
    return directory;
}

void DorisCompoundFileWriter::sort_files(std::vector<FileInfo>& file_infos) {
    auto file_priority = [](const std::string& filename) {
        if (filename.find("segments") != std::string::npos) return 1;
        if (filename.find("fnm") != std::string::npos) return 2;
        if (filename.find("tii") != std::string::npos) return 3;
        return 4; // Other files
    };

    std::sort(file_infos.begin(), file_infos.end(), [&](const FileInfo& a, const FileInfo& b) {
        int32_t priority_a = file_priority(a.filename);
        int32_t priority_b = file_priority(b.filename);
        if (priority_a != priority_b) return priority_a < priority_b;
        return a.filesize < b.filesize;
    });
}

void DorisCompoundFileWriter::writeCompoundFile() {
    // list files in current dir
    std::vector<std::string> files;
    directory->list(&files);
    // remove write.lock file
    auto it = std::find(files.begin(), files.end(), WRITE_LOCK_FILE);
    if (it != files.end()) {
        files.erase(it);
    }

    std::vector<FileInfo> sorted_files;
    for (auto file : files) {
        FileInfo file_info;
        file_info.filename = file;
        file_info.filesize = directory->fileLength(file.c_str());
        sorted_files.emplace_back(std::move(file_info));
    }
    sort_files(sorted_files);

    int32_t file_count = sorted_files.size();

    io::Path cfs_path(((DorisCompoundDirectory*)directory)->getCfsDirName());
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = std::string(cfs_path.stem().c_str()) + COMPOUND_FILE_EXTENSION;
    // write file entries to ram directory to get header length
    lucene::store::RAMDirectory ram_dir;
    auto* out_idx = ram_dir.createOutput(idx_name.c_str());
    if (out_idx == nullptr) {
        LOG(WARNING) << "Write compound file error: RAMDirectory output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create RAMDirectory output error");
    }

    std::unique_ptr<lucene::store::IndexOutput> ram_output(out_idx);
    ram_output->writeVInt(file_count);
    // write file entries in ram directory
    // number of files, which data are in header
    int header_file_count = 0;
    int64_t header_file_length = 0;
    const int64_t buffer_length = 16384;
    uint8_t ram_buffer[buffer_length];
    for (auto file : sorted_files) {
        ram_output->writeString(file.filename); // file name
        ram_output->writeLong(0);               // data offset
        ram_output->writeLong(file.filesize);   // file length
        header_file_length += file.filesize;
        if (header_file_length <= MAX_HEADER_DATA_SIZE) {
            copyFile(file.filename.c_str(), ram_output.get(), ram_buffer, buffer_length);
            header_file_count++;
        }
    }
    auto header_len = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.deleteFile(idx_name.c_str());
    ram_dir.close();

    auto compound_fs = ((DorisCompoundDirectory*)directory)->getCompoundFileSystem();
    auto* out_dir = DorisCompoundDirectoryFactory::getDirectory(compound_fs, idx_path.c_str());

    auto* out = out_dir->createOutput(idx_name.c_str());
    if (out == nullptr) {
        LOG(WARNING) << "Write compound file error: CompoundDirectory output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create CompoundDirectory output error");
    }
    std::unique_ptr<lucene::store::IndexOutput> output(out);
    output->writeVInt(file_count);
    // write file entries
    int64_t data_offset = header_len;
    uint8_t header_buffer[buffer_length];
    for (int i = 0; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        output->writeString(file.filename); // FileName
        // DataOffset
        if (i < header_file_count) {
            // file data write in header, so we set its offset to -1.
            output->writeLong(-1);
        } else {
            output->writeLong(data_offset);
        }
        output->writeLong(file.filesize); // FileLength
        if (i < header_file_count) {
            // append data
            copyFile(file.filename.c_str(), output.get(), header_buffer, buffer_length);
        } else {
            data_offset += file.filesize;
        }
    }
    // write rest files' data
    uint8_t data_buffer[buffer_length];
    for (int i = header_file_count; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        copyFile(file.filename.c_str(), output.get(), data_buffer, buffer_length);
    }
    out_dir->close();
    // NOTE: need to decrease ref count, but not to delete here,
    // because index cache may get the same directory from DIRECTORIES
    _CLDECDELETE(out_dir)
    output->close();
}

void DorisCompoundFileWriter::copyFile(const char* fileName, lucene::store::IndexOutput* output,
                                       uint8_t* buffer, int64_t bufferLength) {
    lucene::store::IndexInput* tmp = nullptr;
    CLuceneError err;
    if (!directory->openInput(fileName, tmp, err)) {
        throw err;
    }

    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, length), remainder);
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, fileName, (int)length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    input->close();
}

class DorisCompoundDirectory::FSIndexOutput : public lucene::store::BufferedIndexOutput {
private:
    io::FileWriterPtr _writer;

protected:
    void flushBuffer(const uint8_t* b, const int32_t size) override;

public:
    FSIndexOutput() = default;
    void init(const io::FileSystemSPtr& fileSystem, const char* path);
    ~FSIndexOutput() override;
    void close() override;
    int64_t length() const override;
};

bool DorisCompoundDirectory::FSIndexInput::open(const io::FileSystemSPtr& fs, const char* path,
                                                IndexInput*& ret, CLuceneError& error,
                                                int32_t buffer_size) {
    CND_PRECONDITION(path != nullptr, "path is NULL");

    if (buffer_size == -1) {
        buffer_size = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    }
    auto* h = _CLNEW SharedHandle(path);

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
        ret = _CLNEW FSIndexInput(h, buffer_size);
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
    delete h->_shared_lock;
    _CLDECDELETE(h)
    return false;
}

DorisCompoundDirectory::FSIndexInput::FSIndexInput(const FSIndexInput& other)
        : BufferedIndexInput(other) {
    if (other._handle == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "other handle is null");
    }

    std::lock_guard<std::mutex> wlock(*other._handle->_shared_lock);
    _handle = _CL_POINTER(other._handle);
    _pos = other._handle->_fpos; //note where we are currently...
    _io_ctx = other._io_ctx;
}

DorisCompoundDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    _length = 0;
    _fpos = 0;
    strcpy(this->path, path);
    _shared_lock = new std::mutex();
}

DorisCompoundDirectory::FSIndexInput::SharedHandle::~SharedHandle() {
    if (_reader) {
        if (_reader->close().ok()) {
            _reader = nullptr;
        }
    }
}

DorisCompoundDirectory::FSIndexInput::~FSIndexInput() {
    FSIndexInput::close();
}

lucene::store::IndexInput* DorisCompoundDirectory::FSIndexInput::clone() const {
    return _CLNEW DorisCompoundDirectory::FSIndexInput(*this);
}
void DorisCompoundDirectory::FSIndexInput::close() {
    BufferedIndexInput::close();
    if (_handle != nullptr) {
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
    }
}

void DorisCompoundDirectory::FSIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < _handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void DorisCompoundDirectory::FSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(_handle != nullptr, "shared file handle has closed");
    CND_PRECONDITION(_handle->_reader != nullptr, "file is not open");
    std::lock_guard<std::mutex> wlock(*_handle->_shared_lock);

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

void DorisCompoundDirectory::FSIndexOutput::init(const io::FileSystemSPtr& fileSystem,
                                                 const char* path) {
    Status status = fileSystem->create_file(path, &_writer);
    if (!status.ok()) {
        _writer.reset(nullptr);
        auto err = "Create compound file error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
}

DorisCompoundDirectory::FSIndexOutput::~FSIndexOutput() {
    if (_writer) {
        try {
            FSIndexOutput::close();
            DBUG_EXECUTE_IF(
                    "DorisCompoundDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_"
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

void DorisCompoundDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (_writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        DBUG_EXECUTE_IF(
                "DorisCompoundDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_"
                "flushBuffer",
                {
                    if (_writer->path().filename() == "_0.tii" ||
                        _writer->path().filename() == "_0.tis") {
                        return;
                    }
                })
        Status st = _writer->append(data);
        DBUG_EXECUTE_IF(
                "DorisCompoundDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer",
                {
                    st = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "flush buffer mock error");
                })
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
            _CLTHROWA(CL_ERR_IO, "writer append data when flushBuffer error");
        }
    } else {
        if (_writer == nullptr) {
            LOG(WARNING) << "File writer is nullptr in DorisCompoundDirectory::FSIndexOutput, "
                            "ignore flush.";
        } else if (b == nullptr) {
            LOG(WARNING) << "buffer is nullptr when flushBuffer in "
                            "DorisCompoundDirectory::FSIndexOutput";
        }
    }
}

void DorisCompoundDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
        DBUG_EXECUTE_IF(
                "DorisCompoundDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_"
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
        Status ret = _writer->finalize();
        DBUG_EXECUTE_IF("DorisCompoundDirectory::FSIndexOutput._set_writer_finalize_status_error",
                        { ret = Status::Error<INTERNAL_ERROR>("writer finalize status error"); })
        if (!ret.ok()) {
            LOG(WARNING) << "FSIndexOutput close, file writer finalize error: " << ret.to_string();
            _writer.reset(nullptr);
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
        ret = _writer->close();
        DBUG_EXECUTE_IF("DorisCompoundDirectory::FSIndexOutput._set_writer_close_status_error",
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

int64_t DorisCompoundDirectory::FSIndexOutput::length() const {
    CND_PRECONDITION(_writer != nullptr, "file is not open");
    int64_t ret;
    if (!_writer->fs()->file_size(_writer->path(), &ret).ok()) {
        return -1;
    }
    return ret;
}

DorisCompoundDirectory::DorisCompoundDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void DorisCompoundDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
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

    if (lock_factory == nullptr) {
        lock_factory = _CLNEW lucene::store::NoLockFactory();
    }

    lucene::store::Directory::setLockFactory(lock_factory);

    // It's fail checking directory existence in S3.
    if (fs->type() == io::FileSystemType::S3) {
        return;
    }
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(directory, &exists), "Doris compound directory init IO error")
    if (!exists) {
        auto e = "Doris compound directory init error: " + directory + " is not a directory";
        LOG(WARNING) << e;
        _CLTHROWA(CL_ERR_IO, e.c_str());
    }
}

void DorisCompoundDirectory::priv_getFN(char* buffer, const char* name) const {
    buffer[0] = 0;
    strcpy(buffer, directory.c_str());
    strcat(buffer, PATH_DELIMITERA);
    strcat(buffer, name);
}

DorisCompoundDirectory::~DorisCompoundDirectory() = default;

const char* DorisCompoundDirectory::getClassName() {
    return "DorisCompoundDirectory";
}
const char* DorisCompoundDirectory::getObjectName() const {
    return getClassName();
}

bool DorisCompoundDirectory::list(std::vector<std::string>* names) const {
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

bool DorisCompoundDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "File exists IO error")
    return exists;
}

const char* DorisCompoundDirectory::getCfsDirName() const {
    return cfs_directory.c_str();
}

int64_t DorisCompoundDirectory::fileModified(const char* name) const {
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

void DorisCompoundDirectory::touchFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    snprintf(buffer, CL_MAX_DIR, "%s%s%s", directory.c_str(), PATH_DELIMITERA, name);

    io::FileWriterPtr tmp_writer;
    LOG_AND_THROW_IF_ERROR(fs->create_file(buffer, &tmp_writer), "Touch file IO error")
}

int64_t DorisCompoundDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    int64_t size = -1;
    LOG_AND_THROW_IF_ERROR(fs->file_size(buffer, &size), "Get file size IO error");
    return size;
}

bool DorisCompoundDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                       CLuceneError& error, int32_t bufferSize) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    return FSIndexInput::open(fs, fl, ret, error, bufferSize);
}

void DorisCompoundDirectory::close() {
    if (useCompoundFileWriter) {
        auto* cfsWriter = _CLNEW DorisCompoundFileWriter(this);
        // write compound file
        cfsWriter->writeCompoundFile();
        // delete index path, which contains separated inverted index files
        deleteDirectory();
        _CLDELETE(cfsWriter)
    }
}

bool DorisCompoundDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    LOG_AND_THROW_IF_ERROR(fs->delete_file(fl), "Delete file IO error")
    return true;
}

bool DorisCompoundDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    LOG_AND_THROW_IF_ERROR(fs->delete_directory(fl),
                           fmt::format("Delete directory {} IO error", fl))
    return true;
}

void DorisCompoundDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::lock_guard<std::mutex> wlock(_this_lock);
    char old[CL_MAX_DIR];
    priv_getFN(old, from);

    char nu[CL_MAX_DIR];
    priv_getFN(nu, to);

    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(nu, &exists), "File exists IO error")
    if (exists) {
        LOG_AND_THROW_IF_ERROR(fs->delete_directory(nu), fmt::format("Delete {} IO error", nu))
    }
    LOG_AND_THROW_IF_ERROR(fs->rename_dir(old, nu),
                           fmt::format("Rename {} to {} IO error", old, nu))
}

lucene::store::IndexOutput* DorisCompoundDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "Create output file exists IO error")
    if (exists) {
        LOG_AND_THROW_IF_ERROR(fs->delete_file(fl),
                               fmt::format("Create output delete file {} IO error", fl))
        LOG_AND_THROW_IF_ERROR(fs->exists(fl, &exists), "Create output file exists IO error")
        assert(!exists);
    }
    auto* ret = _CLNEW FSIndexOutput();
    try {
        ret->init(fs, fl);
    } catch (CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

std::string DorisCompoundDirectory::toString() const {
    return std::string("DorisCompoundDirectory@") + this->directory;
}

/**
 * DorisRAMCompoundDirectory
 */
DorisRAMCompoundDirectory::DorisRAMCompoundDirectory() {
    filesMap = _CLNEW FileMap(true, true);
    this->sizeInBytes = 0;
}

DorisRAMCompoundDirectory::~DorisRAMCompoundDirectory() {
    _CLDELETE(lockFactory);
    _CLDELETE(filesMap);
}

void DorisRAMCompoundDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
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

bool DorisRAMCompoundDirectory::list(std::vector<std::string>* names) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->begin();
    while (itr != filesMap->end()) {
        names->emplace_back(itr->first);
        ++itr;
    }
    return true;
}

bool DorisRAMCompoundDirectory::fileExists(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    return filesMap->exists((char*)name);
}

int64_t DorisRAMCompoundDirectory::fileModified(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    return f->getLastModified();
}

void DorisRAMCompoundDirectory::touchFile(const char* name) {
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

int64_t DorisRAMCompoundDirectory::fileLength(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    return f->getLength();
}

bool DorisRAMCompoundDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
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

void DorisRAMCompoundDirectory::close() {
    // write compound file
    DorisCompoundDirectory::close();

    std::lock_guard<std::mutex> wlock(_this_lock);
    filesMap->clear();
    _CLDELETE(filesMap);
}

bool DorisRAMCompoundDirectory::doDeleteFile(const char* name) {
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

bool DorisRAMCompoundDirectory::deleteDirectory() {
    // do nothing, RAM dir do not have actual files
    return true;
}

void DorisRAMCompoundDirectory::renameFile(const char* from, const char* to) {
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

lucene::store::IndexOutput* DorisRAMCompoundDirectory::createOutput(const char* name) {
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

std::string DorisRAMCompoundDirectory::toString() const {
    return std::string("DorisRAMCompoundDirectory@") + this->directory;
}

const char* DorisRAMCompoundDirectory::getClassName() {
    return "DorisRAMCompoundDirectory";
}

const char* DorisRAMCompoundDirectory::getObjectName() const {
    return getClassName();
}

/**
 * DorisCompoundDirectoryFactory
 */
DorisCompoundDirectory* DorisCompoundDirectoryFactory::getDirectory(
        const io::FileSystemSPtr& _fs, const char* _file, bool use_compound_file_writer,
        bool can_use_ram_dir, lucene::store::LockFactory* lock_factory,
        const io::FileSystemSPtr& _cfs, const char* _cfs_file) {
    const char* cfs_file = _cfs_file;
    if (cfs_file == nullptr) {
        cfs_file = _file;
    }
    DorisCompoundDirectory* dir = nullptr;
    if (!_file || !*_file) {
        _CLTHROWA(CL_ERR_IO, "Invalid directory");
    }

    const char* file = _file;

    // Write by RAM directory
    // 1. only write separated index files, which is can_use_ram_dir = true.
    // 2. config::inverted_index_ram_dir_enable = true
    if (config::inverted_index_ram_dir_enable && can_use_ram_dir) {
        dir = _CLNEW DorisRAMCompoundDirectory();
    } else {
        bool exists = false;
        LOG_AND_THROW_IF_ERROR(_fs->exists(file, &exists), "Get directory exists IO error")
        if (!exists) {
            LOG_AND_THROW_IF_ERROR(_fs->create_directory(file),
                                   "Get directory create directory IO error")
        }
        dir = _CLNEW DorisCompoundDirectory();
    }
    dir->init(_fs, file, use_compound_file_writer, lock_factory, _cfs, cfs_file);

    return dir;
}

} // namespace doris::segment_v2
