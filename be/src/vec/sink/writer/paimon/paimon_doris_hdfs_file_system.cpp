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

#include "vec/sink/writer/paimon/paimon_doris_hdfs_file_system.h"

#ifdef WITH_PAIMON_CPP

#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/path.h"
#include "io/hdfs_builder.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "util/slice.h"

namespace {

std::string _extract_hdfs_fs_name(const std::string& uri) {
    auto starts_with = [&](const char* prefix) { return uri.rfind(prefix, 0) == 0; };
    std::string_view scheme;
    if (starts_with("hdfs://")) {
        scheme = "hdfs://";
    } else if (starts_with("dfs://")) {
        scheme = "dfs://";
    } else {
        return {};
    }
    size_t authority_start = scheme.size();
    size_t first_slash = uri.find('/', authority_start);
    if (first_slash == std::string::npos) {
        return uri;
    }
    if (first_slash == authority_start) {
        return {};
    }
    return uri.substr(0, first_slash);
}

paimon::Status _to_paimon_status(const doris::Status& st) {
    if (st.ok()) {
        return paimon::Status::OK();
    }
    return paimon::Status::IOError(st.to_string());
}

class DorisPaimonBasicFileStatus final : public paimon::BasicFileStatus {
public:
    DorisPaimonBasicFileStatus(std::string path, bool is_dir)
            : _path(std::move(path)), _is_dir(is_dir) {}

    bool IsDir() const override { return _is_dir; }

    std::string GetPath() const override { return _path; }

private:
    std::string _path;
    bool _is_dir;
};

class DorisPaimonFileStatus final : public paimon::FileStatus {
public:
    DorisPaimonFileStatus(std::string path, bool is_dir, uint64_t len, int64_t mtime_ms)
            : _path(std::move(path)), _is_dir(is_dir), _len(len), _mtime_ms(mtime_ms) {}

    uint64_t GetLen() const override { return _len; }

    bool IsDir() const override { return _is_dir; }

    std::string GetPath() const override { return _path; }

    int64_t GetModificationTime() const override { return _mtime_ms; }

private:
    std::string _path;
    bool _is_dir;
    uint64_t _len;
    int64_t _mtime_ms;
};

class DorisPaimonInputStream final : public paimon::InputStream {
public:
    DorisPaimonInputStream(doris::io::FileReaderSPtr reader, std::string uri)
            : _reader(std::move(reader)), _uri(std::move(uri)) {}

    paimon::Status Close() override { return _to_paimon_status(_reader->close()); }

    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override {
        int64_t base = 0;
        switch (origin) {
        case paimon::FS_SEEK_SET:
            base = 0;
            break;
        case paimon::FS_SEEK_CUR:
            base = _pos;
            break;
        case paimon::FS_SEEK_END: {
            auto len = Length();
            if (!len.ok()) {
                return len.status();
            }
            base = static_cast<int64_t>(len.value());
            break;
        }
        default:
            return paimon::Status::Invalid("invalid seek origin");
        }
        int64_t next = base + offset;
        if (next < 0) {
            return paimon::Status::Invalid("negative seek position: ", next);
        }
        _pos = next;
        return paimon::Status::OK();
    }

    paimon::Result<int64_t> GetPos() const override { return _pos; }

    paimon::Result<int32_t> Read(char* buffer, uint32_t size) override {
        size_t bytes_read = 0;
        doris::Slice slice(buffer, size);
        doris::Status st = _reader->read_at(_pos, slice, &bytes_read, nullptr);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        _pos += static_cast<int64_t>(bytes_read);
        return static_cast<int32_t>(bytes_read);
    }

    paimon::Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override {
        size_t bytes_read = 0;
        doris::Slice slice(buffer, size);
        doris::Status st = _reader->read_at(offset, slice, &bytes_read, nullptr);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        return static_cast<int32_t>(bytes_read);
    }

    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(paimon::Status)>&& callback) override {
        auto res = Read(buffer, size, offset);
        if (res.ok()) {
            callback(paimon::Status::OK());
        } else {
            callback(res.status());
        }
    }

    paimon::Result<std::string> GetUri() const override { return _uri; }

    paimon::Result<uint64_t> Length() const override {
        return static_cast<uint64_t>(_reader->size());
    }

private:
    doris::io::FileReaderSPtr _reader;
    std::string _uri;
    int64_t _pos = 0;
};

class DorisPaimonOutputStream final : public paimon::OutputStream {
public:
    DorisPaimonOutputStream(doris::io::FileWriterPtr writer, std::string uri)
            : _writer(std::move(writer)), _uri(std::move(uri)) {}

    paimon::Status Close() override { return _to_paimon_status(_writer->close()); }

    paimon::Result<int32_t> Write(const char* buffer, uint32_t size) override {
        doris::Slice slice(buffer, size);
        doris::Status st = _writer->append(slice);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        _pos += size;
        return static_cast<int32_t>(size);
    }

    paimon::Status Flush() override { return paimon::Status::OK(); }

    paimon::Result<int64_t> GetPos() const override { return _pos; }

    paimon::Result<std::string> GetUri() const override { return _uri; }

private:
    doris::io::FileWriterPtr _writer;
    std::string _uri;
    int64_t _pos = 0;
};

class DorisPaimonFileSystem final : public paimon::FileSystem {
public:
    explicit DorisPaimonFileSystem(std::shared_ptr<doris::io::FileSystem> fs)
            : _fs(std::move(fs)) {}

    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(
            const std::string& path) const override {
        doris::io::FileReaderSPtr reader;
        doris::Status st = _fs->open_file(doris::io::Path(path), &reader, nullptr);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        return std::make_unique<DorisPaimonInputStream>(std::move(reader), path);
    }

    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override {
        bool exists = false;
        doris::Status exists_st = _fs->exists(doris::io::Path(path), &exists);
        if (!exists_st.ok()) {
            return paimon::Status::IOError(exists_st.to_string());
        }
        if (exists) {
            if (!overwrite) {
                return paimon::Status::Exist("path already exists: ", path);
            }
            doris::Status del_st = _fs->delete_directory(doris::io::Path(path));
            if (!del_st.ok()) {
                del_st = _fs->delete_file(doris::io::Path(path));
            }
            if (!del_st.ok()) {
                return paimon::Status::IOError(del_st.to_string());
            }
        }

        doris::io::FileWriterPtr writer;
        doris::Status st = _fs->create_file(doris::io::Path(path), &writer, nullptr);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        return std::make_unique<DorisPaimonOutputStream>(std::move(writer), path);
    }

    paimon::Status Mkdirs(const std::string& path) const override {
        return _to_paimon_status(_fs->create_directory(doris::io::Path(path), false));
    }

    paimon::Status Rename(const std::string& src, const std::string& dst) const override {
        return _to_paimon_status(_fs->rename(doris::io::Path(src), doris::io::Path(dst)));
    }

    paimon::Status Delete(const std::string& path, bool recursive) const override {
        if (recursive) {
            return _to_paimon_status(_fs->delete_directory(doris::io::Path(path)));
        }
        return _to_paimon_status(_fs->delete_file(doris::io::Path(path)));
    }

    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(
            const std::string& path) const override {
        bool exists = false;
        doris::Status st = _fs->exists(doris::io::Path(path), &exists);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        if (!exists) {
            return paimon::Status::NotExist("path not exists: ", path);
        }

        bool is_dir = false;
        {
            std::vector<doris::io::FileInfo> files;
            bool dir_exists = false;
            doris::Status list_st = _fs->list(doris::io::Path(path), false, &files, &dir_exists);
            is_dir = list_st.ok() && dir_exists;
        }

        uint64_t len = 0;
        if (!is_dir) {
            int64_t file_size = 0;
            doris::Status size_st = _fs->file_size(doris::io::Path(path), &file_size);
            if (!size_st.ok()) {
                return paimon::Status::IOError(size_st.to_string());
            }
            len = static_cast<uint64_t>(file_size);
        }
        return std::make_unique<DorisPaimonFileStatus>(path, is_dir, len, 0);
    }

    paimon::Status ListDir(const std::string& directory,
                           std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list)
            const override {
        std::vector<doris::io::FileInfo> files;
        bool exists = false;
        doris::Status st = _fs->list(doris::io::Path(directory), false, &files, &exists);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        if (!exists) {
            return paimon::Status::OK();
        }
        file_status_list->clear();
        file_status_list->reserve(files.size());
        for (const auto& f : files) {
            std::string full_path = directory;
            if (!full_path.empty() && full_path.back() != '/') {
                full_path.push_back('/');
            }
            full_path += f.file_name;
            file_status_list->push_back(
                    std::make_unique<DorisPaimonBasicFileStatus>(std::move(full_path), !f.is_file));
        }
        return paimon::Status::OK();
    }

    paimon::Status ListFileStatus(
            const std::string& path,
            std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const override {
        std::vector<doris::io::FileInfo> files;
        bool exists = false;
        doris::Status st = _fs->list(doris::io::Path(path), false, &files, &exists);

        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        if (!exists) {
            return paimon::Status::OK();
        }
        file_status_list->clear();
        file_status_list->reserve(files.size());
        for (const auto& f : files) {
            std::string full_path = path;
            if (!full_path.empty() && full_path.back() != '/') {
                full_path.push_back('/');
            }
            full_path += f.file_name;
            file_status_list->push_back(std::make_unique<DorisPaimonFileStatus>(
                    std::move(full_path), !f.is_file, static_cast<uint64_t>(f.file_size), 0));
        }
        return paimon::Status::OK();
    }

    paimon::Result<bool> Exists(const std::string& path) const override {
        bool exists = false;
        doris::Status st = _fs->exists(doris::io::Path(path), &exists);
        if (!st.ok()) {
            return paimon::Status::IOError(st.to_string());
        }
        return exists;
    }

private:
    // std::string _resolve_path(const std::string& path) const {
    //     size_t scheme_end = path.find("://");
    //     if (scheme_end != std::string::npos) {
    //         size_t path_start = path.find('/', scheme_end + 3);
    //         if (path_start != std::string::npos) {
    //             return path.substr(path_start);
    //         } else {
    //             return "/";
    //         }
    //     }
    //     return path;
    // }

    std::shared_ptr<doris::io::FileSystem> _fs;
};

class DorisHdfsFileSystemFactory final : public paimon::FileSystemFactory {
public:
    const char* Identifier() const override {
        return doris::vectorized::kPaimonDorisHdfsFsIdentifier;
    }

    paimon::Result<std::unique_ptr<paimon::FileSystem>> Create(
            const std::string& path,
            const std::map<std::string, std::string>& options) const override {
        doris::THdfsParams hdfs_params = doris::parse_properties(options);
        std::string fs_name =
                hdfs_params.__isset.fs_name ? hdfs_params.fs_name : _extract_hdfs_fs_name(path);
        if (fs_name.empty()) {
            return paimon::Status::Invalid("missing hdfs fs_name for path: ", path);
        }
        std::shared_ptr<doris::io::HdfsFileSystem> fs;
        auto fs_res = doris::io::HdfsFileSystem::create(hdfs_params, fs_name, fs_name, nullptr);
        if (fs_res.has_value() == false) {
            return paimon::Status::IOError(fs_res.error().to_string());
        }
        fs = std::move(fs_res).value();
        return std::make_unique<DorisPaimonFileSystem>(std::move(fs));
    }
};

} // namespace

namespace doris::vectorized {

void ensure_paimon_doris_hdfs_file_system_registered() {
    static std::once_flag once;
    std::call_once(once, [] {
        auto* creator = paimon::FactoryCreator::GetInstance();
        if (creator->Create(kPaimonDorisHdfsFsIdentifier) != nullptr) {
            return;
        }
        creator->Register(kPaimonDorisHdfsFsIdentifier, new DorisHdfsFileSystemFactory());
    });
}

} // namespace doris::vectorized

#endif
