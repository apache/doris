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

#include "exec/sink/writer/paimon/doris_paimon_file_system.h"

#ifdef USE_PAIMON_CPP
#include <paimon/macros.h>

#include <algorithm>
#include <limits>
#include <map>
#include <string_view>

#include "common/exception.h"
#include "exec/sink/writer/paimon/paimon_resource_context.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/thread_context.h"

namespace doris {
namespace {
using PStatus = paimon::Status;
template <typename T>
using PResult = paimon::Result<T>;

PStatus io_status(const Status& status) {
    if (status.ok()) return PStatus::OK();
    if (status.is<ErrorCode::NOT_FOUND>()) return PStatus::NotExist(status.to_string());
    if (status.is<ErrorCode::ALREADY_EXIST>()) return PStatus::Exist(status.to_string());
    if (status.is<ErrorCode::CANCELLED>()) return PStatus::Cancelled(status.to_string());
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || status.is<ErrorCode::MEM_ALLOC_FAILED>() ||
        status.is<ErrorCode::BUFFER_ALLOCATION_FAILED>() ||
        status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>() ||
        status.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>() ||
        status.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
        return PStatus::OutOfMemory("Doris Paimon IO allocation failed");
    }
    if (status.is<ErrorCode::INVALID_ARGUMENT>()) return PStatus::Invalid(status.to_string());
    if (status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>())
        return PStatus::NotImplemented(status.to_string());
    return PStatus::IOError(status.to_string());
}

// SDK calls can arrive on its own threads. Readers/writers retain both the filesystem and
// ResourceContext until the last stream is released, including destructor-time IO draining.
template <typename F>
auto io_call(const std::shared_ptr<ResourceContext>& context, F&& f) -> decltype(f()) {
    try {
        return with_paimon_resource_context(context, std::forward<F>(f));
    } catch (const doris::Exception& e) {
        return io_status(e.to_status());
    } catch (const std::bad_alloc&) {
        return PStatus::OutOfMemory("Doris Paimon IO allocation failed");
    } catch (const std::exception&) {
        return PStatus::IOError("Exception in Doris Paimon IO");
    } catch (...) {
        return PStatus::IOError("Unknown exception in Doris Paimon IO");
    }
}

std::string trim_root(std::string path) {
    while (path.size() > 1 && path.back() == '/') path.pop_back();
    return path;
}

std::string child_path(const std::string& parent, const std::string& child) {
    return parent + (parent.back() == '/' ? "" : "/") + child;
}

// Reject ambiguous path components rather than letting an SDK path escape the table's
// storage mapping. This is a namespace guard, not a local-filesystem symlink sandbox.
bool safe_suffix(std::string_view suffix) {
    while (!suffix.empty()) {
        auto slash = suffix.find('/');
        auto part = suffix.substr(0, slash);
        if (part == "." || part == "..") return false;
        if (slash == std::string_view::npos) break;
        suffix.remove_prefix(slash + 1);
    }
    return true;
}

PResult<std::map<std::string, io::FileInfo>> list_children(const io::FileSystemSPtr& fs,
                                                           const std::string& path) {
    std::vector<io::FileInfo> files;
    bool exists = false;
    PAIMON_RETURN_NOT_OK(io_status(fs->list(path, false, &files, &exists)));
    std::map<std::string, io::FileInfo> children;
    for (auto& file : files) {
        if (file.file_name.empty()) continue; // object-store directory marker
        if (file.file_name.front() == '/' || !safe_suffix(file.file_name)) {
            return PStatus::Invalid("Invalid entry in Doris directory listing");
        }
        auto slash = file.file_name.find('/');
        auto name = file.file_name.substr(0, slash);
        if (slash != std::string::npos)
            children[name] = {name, 0, false};
        else if (!children.contains(name))
            children[name] = std::move(file);
    }
    return children;
}

class Input final : public paimon::InputStream {
public:
    Input(io::FileReaderSPtr reader, io::FileSystemSPtr fs, std::string uri,
          std::shared_ptr<ResourceContext> context)
            : _reader(std::move(reader)),
              _fs(std::move(fs)),
              _uri(std::move(uri)),
              _context(std::move(context)) {}
    ~Input() override {
        (void)Close();
        (void)io_call(_context, [&] {
            _reader.reset();
            _fs.reset();
            return PStatus::OK();
        });
    }
    PStatus Close() override {
        if (_closed) return _close_status;
        _closed = true;
        _close_status = io_call(_context, [&] { return io_status(_reader->close()); });
        return _close_status;
    }
    PStatus Seek(int64_t offset, paimon::SeekOrigin origin) override {
        if (_closed) return PStatus::Invalid("Input stream is closed");
        int64_t base = 0;
        if (origin == paimon::FS_SEEK_CUR)
            base = _pos;
        else if (origin == paimon::FS_SEEK_END)
            base = static_cast<int64_t>(_reader->size());
        else if (origin != paimon::FS_SEEK_SET)
            return PStatus::Invalid("Invalid seek origin");
        if (offset < -base || offset > std::numeric_limits<int64_t>::max() - base) {
            return PStatus::Invalid("Invalid seek offset");
        }
        _pos = base + offset;
        return PStatus::OK();
    }
    PResult<int64_t> GetPos() const override { return _pos; }
    PResult<int64_t> Length() const override { return static_cast<int64_t>(_reader->size()); }
    PResult<std::string> GetUri() const override { return _uri; }
    PResult<int64_t> Read(char* buffer, int64_t size) override {
        auto result = Read(buffer, size, _pos);
        if (result.ok()) _pos += result.value();
        return result;
    }
    PResult<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return io_call(_context, [&]() -> PResult<int64_t> {
            if (_closed || size < 0 || offset < 0 || (size > 0 && buffer == nullptr)) {
                return PStatus::Invalid("Invalid input read");
            }
            if (static_cast<uint64_t>(offset) >= _reader->size() || size == 0) return int64_t {0};
            size_t count = std::min<uint64_t>(size, _reader->size() - offset);
            size_t actual = 0;
            PAIMON_RETURN_NOT_OK(
                    io_status(_reader->read_at(offset, Slice(buffer, count), &actual)));
            return static_cast<int64_t>(actual);
        });
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(PStatus)>&& callback) override {
        // Inline completion is intentional: no unowned tasks/buffers survive cancellation.
        auto result = Read(buffer, size, offset);
        callback(!result.ok()             ? result.status()
                 : result.value() == size ? PStatus::OK()
                                          : PStatus::IOError("Short async read"));
    }

private:
    io::FileReaderSPtr _reader;
    io::FileSystemSPtr _fs;
    std::string _uri;
    std::shared_ptr<ResourceContext> _context;
    int64_t _pos = 0;
    bool _closed = false;
    PStatus _close_status;
};

class Output final : public paimon::OutputStream {
public:
    Output(io::FileWriterPtr writer, io::FileSystemSPtr fs, std::string uri,
           std::shared_ptr<ResourceContext> context)
            : _writer(std::move(writer)),
              _fs(std::move(fs)),
              _uri(std::move(uri)),
              _context(std::move(context)) {}
    ~Output() override {
        // Do NOT close/complete an abandoned upload. S3FileWriter destruction waits for
        // submitted IO and releases buffers; remote incomplete MPUs use storage lifecycle
        // cleanup because Doris's ObjStorageClient currently has no abort-MPU interface.
        (void)io_call(_context, [&] {
            _writer.reset();
            _fs.reset();
            return PStatus::OK();
        });
    }
    PResult<int64_t> Write(const char* buffer, int64_t size) override {
        if (!_status.ok()) return _status;
        auto result = io_call(_context, [&]() -> PResult<int64_t> {
            if (!_writer || size < 0 || (size > 0 && buffer == nullptr) ||
                size > std::numeric_limits<int64_t>::max() - _pos) {
                return PStatus::Invalid("Invalid output write");
            }
            if (size != 0) PAIMON_RETURN_NOT_OK(io_status(_writer->append(Slice(buffer, size))));
            _pos += size;
            return size;
        });
        if (!result.ok()) _status = result.status();
        return result;
    }
    PStatus Flush() override {
        // Doris append already transfers ownership of the input bytes. Object visibility
        // and durability are guaranteed by successful Close(), not by this buffer barrier.
        return _status;
    }
    PStatus Close() override {
        if (!_writer) return _status;
        _status = io_call(_context, [&] {
            // Release under the query context even if close throws; never complete a failed write.
            auto writer = std::move(_writer);
            return _status.ok() ? io_status(writer->close(false)) : _status;
        });
        return _status;
    }
    PResult<int64_t> GetPos() const override { return _pos; }
    PResult<std::string> GetUri() const override { return _uri; }

private:
    io::FileWriterPtr _writer;
    io::FileSystemSPtr _fs;
    std::string _uri;
    std::shared_ptr<ResourceContext> _context;
    int64_t _pos = 0;
    PStatus _status;
};

class BasicStatus final : public paimon::BasicFileStatus {
public:
    BasicStatus(std::string path, bool dir) : _path(std::move(path)), _dir(dir) {}
    bool IsDir() const override { return _dir; }
    std::string GetPath() const override { return _path; }

private:
    std::string _path;
    bool _dir;
};

class FileStatus final : public paimon::FileStatus {
public:
    FileStatus(std::string path, int64_t size, bool dir)
            : _path(std::move(path)), _size(size), _dir(dir) {}
    bool IsDir() const override { return _dir; }
    std::string GetPath() const override { return _path; }
    int64_t GetLen() const override { return _size; }
    int64_t GetModificationTime() const override { return -1; } // Doris FileInfo has no mtime
private:
    std::string _path;
    int64_t _size;
    bool _dir;
};
} // namespace

DorisPaimonFileSystem::DorisPaimonFileSystem(io::FileSystemSPtr fs, std::string table_root,
                                             std::string storage_root,
                                             std::shared_ptr<ResourceContext> context)
        : _fs(std::move(fs)),
          _table_root(trim_root(std::move(table_root))),
          _storage_root(trim_root(std::move(storage_root))),
          _context(std::move(context)) {}

DorisPaimonFileSystem::~DorisPaimonFileSystem() {
    auto status = cleanup_owned_files();
    if (!status.ok()) LOG(WARNING) << "Paimon output cleanup failed: " << status.ToString();
    (void)io_call(_context, [&] {
        _owned_files.clear();
        _fs.reset();
        return PStatus::OK();
    });
}

PStatus DorisPaimonFileSystem::cleanup_owned_files() {
    return io_call(_context, [&]() -> PStatus {
        std::lock_guard lock(_owned_mutex);
        _ownership_finished = true;
        PStatus first_error;
        for (auto it = _owned_files.begin(); it != _owned_files.end();) {
            // Continue cleaning other files even if one request or allocation fails.
            auto status = io_call(_context, [&] { return io_status(_fs->delete_file(*it)); });
            if (status.ok()) {
                it = _owned_files.erase(it);
            } else {
                if (first_error.ok()) first_error = status;
                ++it;
            }
        }
        return first_error;
    });
}

void DorisPaimonFileSystem::release_owned_files() {
    with_paimon_resource_context(_context, [&] {
        std::lock_guard lock(_owned_mutex);
        _ownership_finished = true;
        _owned_files.clear();
    });
}

PResult<std::string> DorisPaimonFileSystem::storage_path(const std::string& path) const {
    if (path.empty() || _table_root.empty() || _storage_root.empty() ||
        path.find('\0') != std::string::npos ||
        (_fs->type() == io::FileSystemType::S3 && path.find_first_of("?#") != std::string::npos)) {
        return PStatus::Invalid("Invalid Paimon IO path");
    }
    std::string_view suffix;
    if (path == _table_root) return _storage_root;
    if (_table_root == "/" && path.front() == '/')
        suffix = std::string_view(path).substr(1);
    else if (path.size() > _table_root.size() &&
             path.compare(0, _table_root.size(), _table_root) == 0 &&
             path[_table_root.size()] == '/')
        suffix = std::string_view(path).substr(_table_root.size() + 1);
    else
        return PStatus::Invalid("Paimon IO path is outside the bound table");
    if (!safe_suffix(suffix)) return PStatus::Invalid("Ambiguous Paimon IO path");
    return child_path(_storage_root, std::string(suffix));
}

PResult<std::unique_ptr<paimon::InputStream>> DorisPaimonFileSystem::Open(
        const std::string& path) const {
    return io_call(_context, [&]() -> PResult<std::unique_ptr<paimon::InputStream>> {
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        io::FileReaderSPtr reader;
        io::FileReaderOptions options; // no process-global data cache for mutable schema metadata
        PAIMON_RETURN_NOT_OK(io_status(_fs->open_file(mapped, &reader, &options)));
        if (reader->size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return PStatus::Invalid("Paimon file is too large");
        }
        return std::unique_ptr<paimon::InputStream>(
                new Input(std::move(reader), _fs, path, _context));
    });
}

PResult<std::unique_ptr<paimon::OutputStream>> DorisPaimonFileSystem::Create(
        const std::string& path, bool overwrite) const {
    return io_call(_context, [&]() -> PResult<std::unique_ptr<paimon::OutputStream>> {
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        if (trim_root(path) == _table_root) return PStatus::Invalid("Cannot overwrite table root");
        bool exists = false;
        PAIMON_RETURN_NOT_OK(io_status(_fs->exists(mapped, &exists)));
        {
            std::lock_guard lock(_owned_mutex);
            if (_ownership_finished) return PStatus::Invalid("Paimon output ownership is finished");
            if (exists && (!overwrite || !_owned_files.contains(mapped))) {
                return PStatus::Exist("Paimon output already exists and cannot be replaced");
            }
            // SDK Init/Abort may delete this path even when opening the file fails.
            // A failed existence check must never grant ownership.
            _owned_files.insert(mapped);
        }
        // This is a UUID data-file writer, NOT a conditional-create metadata committer.
        // Doris has no atomic create-if-absent API. AtomicStore is explicitly disabled.
        if (_fs->type() == io::FileSystemType::LOCAL) {
            PAIMON_RETURN_NOT_OK(io_status(_fs->create_directory(io::Path(mapped).parent_path())));
        }
        io::FileWriterOptions options;
        options.used_by_s3_committer = false; // Close must complete the object BEFORE FE commit
        options.allow_adaptive_file_cache_write = false;
        io::FileWriterPtr writer;
        PAIMON_RETURN_NOT_OK(io_status(_fs->create_file(mapped, &writer, &options)));
        return std::unique_ptr<paimon::OutputStream>(
                new Output(std::move(writer), _fs, path, _context));
    });
}

PStatus DorisPaimonFileSystem::Mkdirs(const std::string& path) const {
    return io_call(_context, [&]() -> PStatus {
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        return io_status(_fs->create_directory(mapped));
    });
}

PStatus DorisPaimonFileSystem::Rename(const std::string&, const std::string&) const {
    return PStatus::NotImplemented("Native append adapter does not provide atomic rename");
}

PStatus DorisPaimonFileSystem::AtomicStore(const std::string&, const std::string&) {
    return PStatus::NotImplemented("Paimon metadata commit remains owned by FE");
}

PResult<std::unique_ptr<paimon::FileStatus>> DorisPaimonFileSystem::GetFileStatus(
        const std::string& path) const {
    return io_call(_context, [&]() -> PResult<std::unique_ptr<paimon::FileStatus>> {
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        if (_fs->type() == io::FileSystemType::LOCAL) {
            bool exists = false;
            PAIMON_RETURN_NOT_OK(io_status(_fs->exists(mapped, &exists)));
            if (!exists) return PStatus::NotExist("Paimon path does not exist");
            bool dir = false;
            PAIMON_RETURN_NOT_OK(
                    io_status(io::global_local_filesystem()->is_directory(mapped, &dir)));
            if (dir) return std::unique_ptr<paimon::FileStatus>(new FileStatus(path, 0, true));
        }
        int64_t size = 0;
        auto status = _fs->file_size(mapped, &size);
        if (status.ok())
            return std::unique_ptr<paimon::FileStatus>(new FileStatus(path, size, false));
        if (!status.is<ErrorCode::NOT_FOUND>()) return io_status(status);
        std::vector<io::FileInfo> files;
        bool exists = false;
        PAIMON_RETURN_NOT_OK(io_status(_fs->list(mapped, false, &files, &exists)));
        // S3FileSystem::list always reports exists=true: only objects establish a prefix.
        if (files.empty()) return PStatus::NotExist("Paimon path does not exist");
        return std::unique_ptr<paimon::FileStatus>(new FileStatus(path, 0, true));
    });
}

PStatus DorisPaimonFileSystem::ListFileStatus(
        const std::string& path, std::vector<std::unique_ptr<paimon::FileStatus>>* result) const {
    return io_call(_context, [&]() -> PStatus {
        if (!result) return PStatus::Invalid("Null directory listing output");
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        PAIMON_ASSIGN_OR_RAISE(auto children, list_children(_fs, mapped));
        std::vector<std::unique_ptr<paimon::FileStatus>> staged;
        staged.reserve(children.size());
        for (const auto& [name, info] : children) {
            staged.push_back(std::make_unique<FileStatus>(child_path(path, name), info.file_size,
                                                          !info.is_file));
        }
        result->swap(staged);
        return PStatus::OK();
    });
}

PStatus DorisPaimonFileSystem::ListDir(
        const std::string& path,
        std::vector<std::unique_ptr<paimon::BasicFileStatus>>* result) const {
    return io_call(_context, [&]() -> PStatus {
        if (!result) return PStatus::Invalid("Null directory listing output");
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        PAIMON_ASSIGN_OR_RAISE(auto children, list_children(_fs, mapped));
        std::vector<std::unique_ptr<paimon::BasicFileStatus>> staged;
        staged.reserve(children.size());
        for (const auto& [name, info] : children)
            staged.push_back(std::make_unique<BasicStatus>(child_path(path, name), !info.is_file));
        result->swap(staged);
        return PStatus::OK();
    });
}

PResult<bool> DorisPaimonFileSystem::Exists(const std::string& path) const {
    auto result = GetFileStatus(path);
    if (result.ok()) return true;
    if (result.status().IsNotExist()) return false;
    return result.status();
}

PStatus DorisPaimonFileSystem::Delete(const std::string& path, bool /*recursive*/) const {
    return io_call(_context, [&]() -> PStatus {
        PAIMON_ASSIGN_OR_RAISE(std::string mapped, storage_path(path));
        if (trim_root(path) == _table_root)
            return PStatus::Invalid("Cannot delete bound table root");
        std::lock_guard lock(_owned_mutex);
        if (!_owned_files.contains(mapped)) {
            // SDK Init calls Abort even when Create failed with Exist. Never delete
            // another writer's file, or recursively delete a shared object prefix.
            return PStatus::NotImplemented("Paimon append cleanup only deletes writer-owned files");
        }
        PAIMON_RETURN_NOT_OK(io_status(_fs->delete_file(mapped)));
        // Retain ownership for idempotent retries until cleanup or explicit handoff.
        return PStatus::OK();
    });
}

PStatus DorisPaimonFileSystem::WriteFile(const std::string& path, const std::string& content,
                                         bool overwrite) {
    return io_call(_context, [&]() -> PStatus {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::OutputStream> out, Create(path, overwrite));
        PAIMON_ASSIGN_OR_RAISE(int64_t written, out->Write(content.data(), content.size()));
        if (written != static_cast<int64_t>(content.size()))
            return PStatus::IOError("Short file write");
        PAIMON_RETURN_NOT_OK(out->Flush());
        return out->Close(); // 0.3.0's default implementation discards this error
    });
}
} // namespace doris
#endif
