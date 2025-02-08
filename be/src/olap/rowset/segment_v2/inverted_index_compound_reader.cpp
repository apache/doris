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

#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

#include <CLucene/clucene-config.h>
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>

#include <memory>
#include <utility>

#include "CLucene/SharedHeader.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io
} // namespace doris

#define BUFFER_LENGTH 16384
#define CL_MAX_PATH 4096

#define STRDUP_WtoA(x) CL_NS(util)::Misc::_wideToChar(x)
#define STRDUP_TtoA STRDUP_WtoA

using FileWriterPtr = std::unique_ptr<doris::io::FileWriter>;

namespace doris {
namespace segment_v2 {

/** Implementation of an IndexInput that reads from a portion of the
 *  compound file.
 */
class CSIndexInput : public lucene::store::BufferedIndexInput {
private:
    CL_NS(store)::IndexInput* base;
    std::string file_name;
    int64_t fileOffset;
    int64_t _length;
    const io::IOContext* _io_ctx = nullptr;

protected:
    void readInternal(uint8_t* /*b*/, const int32_t /*len*/) override;
    void seekInternal(const int64_t /*pos*/) override {}

public:
    CSIndexInput(CL_NS(store)::IndexInput* base, const std::string& file_name,
                 const int64_t fileOffset, const int64_t length,
                 const int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    CSIndexInput(const CSIndexInput& clone);
    ~CSIndexInput() override;
    void close() override;
    lucene::store::IndexInput* clone() const override;
    int64_t length() const override { return _length; }
    const char* getDirectoryType() const override { return DorisCompoundReader::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }
    static const char* getClassName() { return "CSIndexInput"; }
    void setIoContext(const void* io_ctx) override;
};

CSIndexInput::CSIndexInput(CL_NS(store)::IndexInput* base, const std::string& file_name,
                           const int64_t fileOffset, const int64_t length,
                           const int32_t read_buffer_size)
        : BufferedIndexInput(read_buffer_size) {
    this->base = base;
    this->file_name = file_name;
    this->fileOffset = fileOffset;
    this->_length = length;
}

void CSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    std::lock_guard wlock(((DorisFSDirectory::FSIndexInput*)base)->_this_lock);

    int64_t start = getFilePointer();
    if (start + len > _length) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }

    if (_io_ctx) {
        base->setIoContext(_io_ctx);
    }

    DBUG_EXECUTE_IF("CSIndexInput.readInternal", {
        for (const auto& entry : InvertedIndexDescriptor::index_file_info_map) {
            if (file_name.find(entry.first) != std::string::npos) {
                if (!static_cast<const io::IOContext*>(base->getIoContext())->is_index_data) {
                    _CLTHROWA(CL_ERR_IO,
                              "The 'is_index_data' flag should be true for inverted index meta "
                              "files.");
                }
            }
        }
        for (const auto& entry : InvertedIndexDescriptor::normal_file_info_map) {
            if (file_name.find(entry.first) != std::string::npos) {
                if (static_cast<const io::IOContext*>(base->getIoContext())->is_index_data) {
                    _CLTHROWA(CL_ERR_IO,
                              "The 'is_index_data' flag should be false for non-meta inverted "
                              "index files.");
                }
            }
        }
    });

    base->seek(fileOffset + start);
    bool read_from_buffer = true;
    base->readBytes(b, len, read_from_buffer);

    if (_io_ctx) {
        base->setIoContext(nullptr);
    }
}

CSIndexInput::~CSIndexInput() = default;

lucene::store::IndexInput* CSIndexInput::clone() const {
    return _CLNEW CSIndexInput(*this);
}

CSIndexInput::CSIndexInput(const CSIndexInput& clone) : BufferedIndexInput(clone) {
    this->base = clone.base;
    this->file_name = clone.file_name;
    this->fileOffset = clone.fileOffset;
    this->_length = clone._length;
}

void CSIndexInput::close() {}

void CSIndexInput::setIoContext(const void* io_ctx) {
    _io_ctx = static_cast<const io::IOContext*>(io_ctx);
}

DorisCompoundReader::DorisCompoundReader(CL_NS(store)::IndexInput* stream,
                                         EntriesType* entries_clone, int32_t read_buffer_size,
                                         const io::IOContext* io_ctx)
        : _stream(stream),
          _entries(_CLNEW EntriesType(true, true)),
          _read_buffer_size(read_buffer_size) {
    // After stream clone, the io_ctx needs to be reconfigured.
    initialize(io_ctx);

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

DorisCompoundReader::DorisCompoundReader(CL_NS(store)::IndexInput* stream, int32_t read_buffer_size,
                                         const io::IOContext* io_ctx)
        : _ram_dir(new lucene::store::RAMDirectory()),
          _stream(stream),
          _entries(_CLNEW EntriesType(true, true)),
          _read_buffer_size(read_buffer_size) {
    // After stream clone, the io_ctx needs to be reconfigured.
    initialize(io_ctx);

    try {
        int32_t count = _stream->readVInt();
        ReaderFileEntry* entry = nullptr;
        TCHAR tid[CL_MAX_PATH];
        uint8_t buffer[BUFFER_LENGTH];
        for (int32_t i = 0; i < count; i++) {
            entry = _CLNEW ReaderFileEntry();
            stream->readString(tid, CL_MAX_PATH);
            char* aid = STRDUP_TtoA(tid);
            entry->file_name = aid;
            entry->offset = stream->readLong();
            entry->length = stream->readLong();
            DBUG_EXECUTE_IF("construct_DorisCompoundReader_failed", {
                CLuceneError err;
                err.set(CL_ERR_IO, "construct_DorisCompoundReader_failed");
                throw err;
            })
            _entries->put(aid, entry);
            // read header file data
            if (entry->offset < 0) {
                copyFile(entry->file_name.c_str(), entry->length, buffer, BUFFER_LENGTH);
            }
        }
    } catch (...) {
        try {
            if (_stream != nullptr) {
                _stream->close();
                _CLDELETE(_stream)
            }
            if (_entries != nullptr) {
                _entries->clear();
                _CLDELETE(_entries);
            }
            if (_ram_dir) {
                _ram_dir->close();
                _CLDELETE(_ram_dir)
            }
        } catch (CLuceneError& err) {
            if (err.number() != CL_ERR_IO) {
                throw err;
            }
        }
        throw;
    }
}

void DorisCompoundReader::copyFile(const char* file, int64_t file_length, uint8_t* buffer,
                                   int64_t buffer_length) {
    std::unique_ptr<lucene::store::IndexOutput> output(_ram_dir->createOutput(file));
    int64_t start_ptr = output->getFilePointer();
    int64_t remainder = file_length;
    int64_t chunk = buffer_length;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, file_length), remainder);
        _stream->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, file, (int)file_length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != file_length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)file_length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    output->close();
}

DorisCompoundReader::~DorisCompoundReader() {
    if (!_closed) {
        try {
            close();
        } catch (CLuceneError& err) {
            LOG(ERROR) << "DorisCompoundReader finalize error:" << err.what();
        }
    }
    _CLDELETE(_entries)
}

const char* DorisCompoundReader::getClassName() {
    return "DorisCompoundReader";
}
const char* DorisCompoundReader::getObjectName() const {
    return getClassName();
}

bool DorisCompoundReader::list(std::vector<std::string>* names) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "DorisCompoundReader is already closed");
    }
    for (EntriesType::const_iterator i = _entries->begin(); i != _entries->end(); i++) {
        names->push_back(i->first);
    }
    return true;
}

bool DorisCompoundReader::fileExists(const char* name) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "DorisCompoundReader is already closed");
    }
    return _entries->exists((char*)name);
}

int64_t DorisCompoundReader::fileModified(const char* name) const {
    return 0;
}

int64_t DorisCompoundReader::fileLength(const char* name) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "DorisCompoundReader is already closed");
    }
    ReaderFileEntry* e = _entries->get((char*)name);
    if (e == nullptr) {
        char buf[CL_MAX_PATH + 30];
        strcpy(buf, "File ");
        strncat(buf, name, CL_MAX_PATH);
        strcat(buf, " does not exist");
        _CLTHROWA(CL_ERR_IO, buf);
    }
    return e->length;
}

bool DorisCompoundReader::openInput(const char* name,
                                    std::unique_ptr<lucene::store::IndexInput>& ret,
                                    CLuceneError& error, int32_t bufferSize) {
    if (_closed || _entries == nullptr) {
        error.set(CL_ERR_IO, "DorisCompoundReader is already closed");
        return false;
    }
    lucene::store::IndexInput* tmp;
    bool success = openInput(name, tmp, error, bufferSize);
    if (success) {
        ret.reset(tmp);
    }
    return success;
}

bool DorisCompoundReader::openInput(const char* name, lucene::store::IndexInput*& ret,
                                    CLuceneError& error, int32_t bufferSize) {
    if (_stream == nullptr) {
        error.set(CL_ERR_IO, "Stream closed");
        return false;
    }

    const ReaderFileEntry* entry = _entries->get((char*)name);
    if (entry == nullptr) {
        char buf[CL_MAX_PATH + 26];
        snprintf(buf, CL_MAX_PATH + 26, "No sub-file with id %s found", name);
        error.set(CL_ERR_IO, buf);
        return false;
    }

    // If file is in RAM, just return.
    if (_ram_dir && _ram_dir->fileExists(name)) {
        return _ram_dir->openInput(name, ret, error, bufferSize);
    }

    if (bufferSize < 1) {
        bufferSize = _read_buffer_size;
    }

    ret = _CLNEW CSIndexInput(_stream, entry->file_name, entry->offset, entry->length, bufferSize);
    return true;
}

void DorisCompoundReader::close() {
    std::lock_guard<std::mutex> wlock(_this_lock);
    if (_stream != nullptr) {
        _stream->close();
        _CLDELETE(_stream)
    }
    if (_entries != nullptr) {
        // The life cycle of _entries should be consistent with that of the DorisCompoundReader.
        // DO NOT DELETE _entries here, it will be deleted in the destructor
        // When directory is closed, all _entries are cleared. But the directory may be called in other places.
        // If we delete the _entries object here, it will cause core dump.
        _entries->clear();
    }
    if (_ram_dir) {
        _ram_dir->close();
        _CLDELETE(_ram_dir)
    }
    _closed = true;
}

bool DorisCompoundReader::doDeleteFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::doDeleteFile");
}

void DorisCompoundReader::renameFile(const char* /*from*/, const char* /*to*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::renameFile");
}

void DorisCompoundReader::touchFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::touchFile");
}

lucene::store::IndexOutput* DorisCompoundReader::createOutput(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::createOutput");
}

std::string DorisCompoundReader::toString() const {
    return std::string("DorisCompoundReader@");
}

CL_NS(store)::IndexInput* DorisCompoundReader::getDorisIndexInput() {
    return _stream;
}

void DorisCompoundReader::initialize(const io::IOContext* io_ctx) {
    _stream->setIoContext(io_ctx);
    _stream->setIdxFileCache(true);
}

} // namespace segment_v2
} // namespace doris
