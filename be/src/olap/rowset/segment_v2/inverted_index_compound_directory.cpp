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
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
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
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>

#define CL_MAX_PATH 4096
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

namespace doris {
namespace segment_v2 {

const char* WRITE_LOCK_FILE = "write.lock";
const char* COMPOUND_FILE_EXTENSION = ".idx";
const int64_t MAX_HEADER_DATA_SIZE = 1024 * 128; // 128k

bool DorisCompoundDirectory::disableLocks = false;

DorisCompoundFileWriter::DorisCompoundFileWriter(CL_NS(store)::Directory* dir) {
    if (dir == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "directory cannot be null");
    }

    directory = dir;
}

CL_NS(store)::Directory* DorisCompoundFileWriter::getDirectory() {
    return directory;
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
    // sort file list by file length
    std::vector<std::pair<std::string, int64_t>> sorted_files;
    for (auto file : files) {
        sorted_files.push_back(std::make_pair(
                file, ((DorisCompoundDirectory*)directory)->fileLength(file.c_str())));
    }
    std::sort(sorted_files.begin(), sorted_files.end(),
              [](const std::pair<std::string, int64_t>& a,
                 const std::pair<std::string, int64_t>& b) { return (a.second < b.second); });

    int32_t file_count = sorted_files.size();

    io::Path cfs_path(((DorisCompoundDirectory*)directory)->getCfsDirName());
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = std::string(cfs_path.stem().c_str()) + COMPOUND_FILE_EXTENSION;
    // write file entries to ram directory to get header length
    lucene::store::RAMDirectory ram_dir;
    auto out_idx = ram_dir.createOutput(idx_name.c_str());
    if (out_idx == nullptr) {
        LOG(WARNING) << "Write compound file error: RAMDirectory output is nullptr.";
        return;
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
        ram_output->writeString(file.first); // file name
        ram_output->writeLong(0);            // data offset
        ram_output->writeLong(file.second);  // file length
        header_file_length += file.second;
        if (header_file_length <= MAX_HEADER_DATA_SIZE) {
            copyFile(file.first.c_str(), ram_output.get(), ram_buffer, buffer_length);
            header_file_count++;
        }
    }
    auto header_len = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.deleteFile(idx_name.c_str());
    ram_dir.close();

    auto compound_fs = ((DorisCompoundDirectory*)directory)->getCompoundFileSystem();
    auto out_dir = DorisCompoundDirectory::getDirectory(compound_fs, idx_path.c_str(), false);

    auto out = out_dir->createOutput(idx_name.c_str());
    if (out == nullptr) {
        LOG(WARNING) << "Write compound file error: CompoundDirectory output is nullptr.";
        return;
    }
    std::unique_ptr<lucene::store::IndexOutput> output(out);
    output->writeVInt(file_count);
    // write file entries
    int64_t data_offset = header_len;
    uint8_t header_buffer[buffer_length];
    for (int i = 0; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        output->writeString(file.first); // FileName
        // DataOffset
        if (i < header_file_count) {
            // file data write in header, so we set its offset to -1.
            output->writeLong(-1);
        } else {
            output->writeLong(data_offset);
        }
        output->writeLong(file.second); // FileLength
        if (i < header_file_count) {
            // append data
            copyFile(file.first.c_str(), output.get(), header_buffer, buffer_length);
        } else {
            data_offset += file.second;
        }
    }
    // write rest files' data
    uint8_t data_buffer[buffer_length];
    for (int i = header_file_count; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        copyFile(file.first.c_str(), output.get(), data_buffer, buffer_length);
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
    io::FileWriterPtr writer;

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
    SharedHandle* h = _CLNEW SharedHandle(path);

    if (!fs->open_file(path, &h->_reader).ok()) {
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

    std::lock_guard<doris::Mutex> wlock(*other._handle->_shared_lock);
    _handle = _CL_POINTER(other._handle);
    _pos = other._handle->_fpos; //note where we are currently...
}

DorisCompoundDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    _length = 0;
    _fpos = 0;
    strcpy(this->path, path);
    _shared_lock = new doris::Mutex();
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
        doris::Mutex* lock = _handle->_shared_lock;
        bool ref = false;
        {
            std::lock_guard<doris::Mutex> wlock(*lock);
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
    std::lock_guard<doris::Mutex> wlock(*_handle->_shared_lock);

    if (_handle->_fpos != _pos) {
        _handle->_fpos = _pos;
    }

    Slice result {b, (size_t)len};
    size_t bytes_read = 0;
    if (!_handle->_reader->read_at(_pos, result, &bytes_read).ok()) {
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
    Status status = fileSystem->create_file(path, &writer);
    if (!status.ok()) {
        writer.reset(nullptr);
        auto err = "Create compound file error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
}

DorisCompoundDirectory::FSIndexOutput::~FSIndexOutput() {
    if (writer) {
        try {
            FSIndexOutput::close();
        } catch (CLuceneError& err) {
            //ignore errors...
            LOG(WARNING) << "FSIndexOutput deconstruct error: " << err.what();
        }
    }
}

void DorisCompoundDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        Status st = writer->append(data);
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore flush.";
    }
}

void DorisCompoundDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
    } catch (CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close error: " << err.what();
        if (err.number() != CL_ERR_IO) {
            LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close IO error: "
                         << err.what();
            throw;
        }
    }
    if (writer) {
        Status ret = writer->finalize();
        if (ret != Status::OK()) {
            LOG(WARNING) << "FSIndexOutput close, file writer finalize error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
        ret = writer->close();
        if (ret != Status::OK()) {
            LOG(WARNING) << "FSIndexOutput close, file writer close error: " << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
    }
    writer = nullptr;
}

int64_t DorisCompoundDirectory::FSIndexOutput::length() const {
    CND_PRECONDITION(writer != nullptr, "file is not open");
    int64_t ret;
    if (!writer->fs()->file_size(writer->path(), &ret).ok()) {
        return -1;
    }
    return ret;
}

DorisCompoundDirectory::DorisCompoundDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void DorisCompoundDirectory::init(const io::FileSystemSPtr& _fs, const char* _path,
                                  lucene::store::LockFactory* lock_factory,
                                  const io::FileSystemSPtr& cfs, const char* cfs_path) {
    fs = _fs;
    directory = _path;

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
    bool doClearLockID = false;

    if (lock_factory == nullptr) {
        if (disableLocks) {
            lock_factory = lucene::store::NoLockFactory::getNoLockFactory();
        } else {
            lock_factory = _CLNEW lucene::store::FSLockFactory(directory.c_str(), this->filemode);
            doClearLockID = true;
        }
    }

    setLockFactory(lock_factory);

    if (doClearLockID) {
        lockFactory->setLockPrefix(nullptr);
    }

    // It's meaningless checking directory existence in S3.
    if (fs->type() == io::FileSystemType::S3) {
        return;
    }
    bool exists = false;
    Status status = fs->exists(directory, &exists);
    if (!status.ok()) {
        auto err = "File system error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA_DEL(CL_ERR_IO, err.c_str());
    }
    if (!exists) {
        auto e = "Doris compound directory init error: " + directory + " is not a directory";
        LOG(WARNING) << e;
        _CLTHROWA_DEL(CL_ERR_IO, e.c_str());
    }
}

void DorisCompoundDirectory::create() {
    std::lock_guard<doris::Mutex> wlock(_this_lock);

    //clear old files
    std::vector<std::string> files;
    lucene::util::Misc::listFiles(directory.c_str(), files, false);
    std::vector<std::string>::iterator itr = files.begin();
    while (itr != files.end()) {
        if (CL_NS(index)::IndexReader::isLuceneFile(itr->c_str())) {
            if (unlink((directory + PATH_DELIMITERA + *itr).c_str()) == -1) {
                _CLTHROWA(CL_ERR_IO, "Couldn't delete file ");
            }
        }
        itr++;
    }
    lockFactory->clearLock(CL_NS(index)::IndexWriter::WRITE_LOCK_NAME);
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
    RETURN_IF_ERROR(fs->list(fl, true, &files, &exists));
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
    fs->exists(fl, &exists);
    return exists;
}

const char* DorisCompoundDirectory::getCfsDirName() const {
    return cfs_directory.c_str();
}

DorisCompoundDirectory* DorisCompoundDirectory::getDirectory(const io::FileSystemSPtr& fs,
                                                             const char* file,
                                                             bool use_compound_file_writer,
                                                             const io::FileSystemSPtr& cfs_fs,
                                                             const char* cfs_file) {
    DorisCompoundDirectory* dir =
            getDirectory(fs, file, (lucene::store::LockFactory*)nullptr, cfs_fs, cfs_file);
    dir->useCompoundFileWriter = use_compound_file_writer;
    return dir;
}

//static
DorisCompoundDirectory* DorisCompoundDirectory::getDirectory(
        const io::FileSystemSPtr& _fs, const char* _file, lucene::store::LockFactory* lock_factory,
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

    bool exists = false;
    _fs->exists(file, &exists);
    if (!exists) {
        mkdir(file, 0777);
    }

    dir = _CLNEW DorisCompoundDirectory();
    dir->init(_fs, file, lock_factory, _cfs, cfs_file);

    return dir;
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
    if (!fs->create_file(buffer, &tmp_writer).ok()) {
        _CLTHROWA(CL_ERR_IO, "IO Error while touching file");
    }
}

int64_t DorisCompoundDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    priv_getFN(buffer, name);
    int64_t size = -1;
    RETURN_IF_ERROR(fs->file_size(buffer, &size));
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
        DorisCompoundFileWriter* cfsWriter = _CLNEW DorisCompoundFileWriter(this);
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
    return fs->delete_file(fl).ok();
}

bool DorisCompoundDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, "");
    Status status = fs->delete_directory(fl);
    if (!status.ok()) {
        char* err = _CL_NEWARRAY(
                char, 16 + status.to_string().length() + 1); //16: len of "couldn't delete "
        strcpy(err, "couldn't delete directory: ");
        strcat(err, status.to_string().c_str());
        _CLTHROWA_DEL(CL_ERR_IO, err);
    }
    return true;
}

void DorisCompoundDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::lock_guard<doris::Mutex> wlock(_this_lock);
    char old[CL_MAX_DIR];
    priv_getFN(old, from);

    char nu[CL_MAX_DIR];
    priv_getFN(nu, to);

    bool exists = false;
    fs->exists(nu, &exists);
    if (exists) {
        if (!fs->delete_directory(nu).ok()) {
            char* err = _CL_NEWARRAY(char, 16 + strlen(to) + 1); //16: len of "couldn't delete "
            strcpy(err, "couldn't delete ");
            strcat(err, to);
            _CLTHROWA_DEL(CL_ERR_IO, err);
        }
    }
    if (rename(old, nu) != 0) {
        char buffer[20 + CL_MAX_PATH + CL_MAX_PATH];
        strcpy(buffer, "couldn't rename ");
        strcat(buffer, from);
        strcat(buffer, " to ");
        strcat(buffer, nu);
        _CLTHROWA(CL_ERR_IO, buffer);
    }
}

lucene::store::IndexOutput* DorisCompoundDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char fl[CL_MAX_DIR];
    priv_getFN(fl, name);
    bool exists = false;
    auto status = fs->exists(fl, &exists);
    if (!status.ok()) {
        LOG(WARNING) << "Doris compound directory create output error: " << status.to_string();
        return nullptr;
    }
    if (exists) {
        if (!fs->delete_file(fl).ok()) {
            char tmp[1024];
            strcpy(tmp, "Cannot overwrite: ");
            strcat(tmp, name);
            _CLTHROWA(CL_ERR_IO, tmp);
        }
        fs->exists(fl, &exists);
        assert(!exists);
    }
    auto ret = _CLNEW FSIndexOutput();
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

} // namespace segment_v2
} // namespace doris
