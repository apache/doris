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

#include "paimon_doris_file_system.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "paimon/factories/factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

struct ParsedUri {
    std::string scheme;
    std::string authority;
};

std::string to_lower(std::string value) {
    std::ranges::transform(value, value.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

ParsedUri parse_uri(const std::string& path) {
    ParsedUri parsed;
    size_t scheme_pos = path.find("://");
    size_t delim_len = 3;
    if (scheme_pos == std::string::npos) {
        scheme_pos = path.find(":/");
        delim_len = 2;
    }
    if (scheme_pos == std::string::npos || scheme_pos == 0) {
        return parsed;
    }
    parsed.scheme = to_lower(path.substr(0, scheme_pos));
    size_t authority_start = scheme_pos + delim_len;
    if (authority_start >= path.size() || path[authority_start] == '/') {
        return parsed;
    }
    size_t next_slash = path.find('/', authority_start);
    if (next_slash == std::string::npos) {
        parsed.authority = path.substr(authority_start);
    } else {
        parsed.authority = path.substr(authority_start, next_slash - authority_start);
    }
    return parsed;
}

bool is_s3_scheme(const std::string& scheme) {
    return scheme == "s3" || scheme == "s3a" || scheme == "s3n" || scheme == "oss" ||
           scheme == "obs" || scheme == "cos" || scheme == "cosn" || scheme == "gs" ||
           scheme == "abfs" || scheme == "abfss" || scheme == "wasb" || scheme == "wasbs";
}

bool is_hdfs_scheme(const std::string& scheme) {
    return scheme == "hdfs" || scheme == "viewfs" || scheme == "local";
}

bool is_http_scheme(const std::string& scheme) {
    return scheme == "http" || scheme == "https";
}

doris::TFileType::type map_scheme_to_file_type(const std::string& scheme) {
    if (scheme.empty()) {
        return doris::TFileType::FILE_HDFS;
    }
    if (scheme == "file") {
        return doris::TFileType::FILE_LOCAL;
    }
    if (is_hdfs_scheme(scheme)) {
        return doris::TFileType::FILE_HDFS;
    }
    if (is_s3_scheme(scheme)) {
        return doris::TFileType::FILE_S3;
    }
    if (is_http_scheme(scheme)) {
        return doris::TFileType::FILE_HTTP;
    }
    if (scheme == "ofs" || scheme == "gfs" || scheme == "jfs") {
        return doris::TFileType::FILE_BROKER;
    }
    return doris::TFileType::FILE_HDFS;
}

std::string replace_scheme(const std::string& path, const std::string& scheme) {
    size_t scheme_pos = path.find("://");
    size_t delim_len = 3;
    if (scheme_pos == std::string::npos) {
        scheme_pos = path.find(":/");
        delim_len = 2;
    }
    if (scheme_pos == std::string::npos) {
        return path;
    }
    return scheme + "://" + path.substr(scheme_pos + delim_len);
}

std::string normalize_local_path(const std::string& path) {
    if (!path.starts_with("file:")) {
        return path;
    }
    constexpr size_t file_prefix_len = 5;
    size_t start = file_prefix_len;
    if (path.compare(start, 2, "//") == 0 && path.size() - start > 2) {
        size_t next_slash = path.find('/', start + 2);
        if (next_slash == std::string::npos) {
            return "";
        }
        start = next_slash;
    }
    return path.substr(start);
}

std::string normalize_path_for_type(const std::string& path, const std::string& scheme,
                                    doris::TFileType::type type) {
    if (type == doris::TFileType::FILE_LOCAL) {
        return normalize_local_path(path);
    }
    if (type == doris::TFileType::FILE_S3 && scheme != "s3" && !is_http_scheme(scheme)) {
        return replace_scheme(path, "s3");
    }
    return path;
}

std::string build_fs_cache_key(doris::TFileType::type type, const ParsedUri& uri,
                               const std::string& default_fs_name) {
    switch (type) {
    case doris::TFileType::FILE_LOCAL:
        return "local";
    case doris::TFileType::FILE_S3:
        return "s3://" + uri.authority;
    case doris::TFileType::FILE_HTTP:
        return "http://" + uri.authority;
    case doris::TFileType::FILE_BROKER:
        return "broker";
    case doris::TFileType::FILE_HDFS:
    default:
        if (!uri.scheme.empty() || !uri.authority.empty()) {
            return uri.scheme + "://" + uri.authority;
        }
        return default_fs_name;
    }
}

paimon::Status to_paimon_status(const doris::Status& status) {
    if (status.ok()) {
        return paimon::Status::OK();
    }
    switch (status.code()) {
    case doris::ErrorCode::NOT_FOUND:
    case doris::ErrorCode::DIR_NOT_EXIST:
        return paimon::Status::NotExist(status.to_string());
    case doris::ErrorCode::ALREADY_EXIST:
    case doris::ErrorCode::FILE_ALREADY_EXIST:
        return paimon::Status::Exist(status.to_string());
    case doris::ErrorCode::INVALID_ARGUMENT:
    case doris::ErrorCode::INVALID_INPUT_SYNTAX:
        return paimon::Status::Invalid(status.to_string());
    case doris::ErrorCode::NOT_IMPLEMENTED_ERROR:
        return paimon::Status::NotImplemented(status.to_string());
    default:
        return paimon::Status::IOError(status.to_string());
    }
}

std::string join_path(const std::string& base, const std::string& child) {
    if (base.empty()) {
        return child;
    }
    if (base.back() == '/') {
        return base + child;
    }
    return base + "/" + child;
}

std::string parent_path_no_scheme(const std::string& path) {
    if (path.empty()) {
        return "";
    }
    size_t end = path.size();
    while (end > 1 && path[end - 1] == '/') {
        --end;
    }
    size_t pos = path.rfind('/', end - 1);
    if (pos == std::string::npos) {
        return "";
    }
    if (pos == 0) {
        return "/";
    }
    return path.substr(0, pos);
}

std::string parent_path(const std::string& path) {
    ParsedUri uri = parse_uri(path);
    if (uri.scheme.empty()) {
        return parent_path_no_scheme(path);
    }
    size_t scheme_pos = path.find("://");
    size_t delim_len = 3;
    if (scheme_pos == std::string::npos) {
        scheme_pos = path.find(":/");
        delim_len = 2;
    }
    if (scheme_pos == std::string::npos) {
        return parent_path_no_scheme(path);
    }
    size_t start = scheme_pos + delim_len;
    size_t slash = path.find('/', start);
    if (slash == std::string::npos) {
        return "";
    }
    std::string path_part = path.substr(slash);
    std::string parent_part = parent_path_no_scheme(path_part);
    if (parent_part.empty()) {
        return "";
    }
    std::string prefix = uri.scheme + "://";
    if (!uri.authority.empty()) {
        prefix += uri.authority;
    }
    return prefix + parent_part;
}

class DorisInputStream : public InputStream {
public:
    DorisInputStream(doris::io::FileReaderSPtr reader, std::string path)
            : reader_(std::move(reader)), path_(std::move(path)) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        int64_t target = 0;
        if (origin == SeekOrigin::FS_SEEK_SET) {
            target = offset;
        } else if (origin == SeekOrigin::FS_SEEK_CUR) {
            target = position_ + offset;
        } else if (origin == SeekOrigin::FS_SEEK_END) {
            target = static_cast<int64_t>(reader_->size()) + offset;
        } else {
            return Status::Invalid("unknown seek origin");
        }
        if (target < 0) {
            return Status::Invalid("seek position is negative");
        }
        position_ = target;
        return Status::OK();
    }

    Result<int64_t> GetPos() const override { return position_; }

    Result<int32_t> Read(char* buffer, uint32_t size) override {
        size_t bytes_read = 0;
        doris::Status status = reader_->read_at(position_, doris::Slice(buffer, size), &bytes_read);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        position_ += static_cast<int64_t>(bytes_read);
        return static_cast<int32_t>(bytes_read);
    }

    Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override {
        size_t bytes_read = 0;
        doris::Status status = reader_->read_at(offset, doris::Slice(buffer, size), &bytes_read);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        return static_cast<int32_t>(bytes_read);
    }

    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(Status)>&& callback) override {
        Result<int32_t> result = Read(buffer, size, offset);
        Status status = Status::OK();
        if (!result.ok()) {
            status = result.status();
        }
        callback(status);
    }

    Result<std::string> GetUri() const override { return path_; }

    Result<uint64_t> Length() const override { return static_cast<uint64_t>(reader_->size()); }

    Status Close() override { return to_paimon_status(reader_->close()); }

private:
    doris::io::FileReaderSPtr reader_;
    std::string path_;
    int64_t position_ = 0;
};

class DorisOutputStream : public OutputStream {
public:
    DorisOutputStream(doris::io::FileWriterPtr writer, std::string path)
            : writer_(std::move(writer)), path_(std::move(path)) {}

    Result<int32_t> Write(const char* buffer, uint32_t size) override {
        doris::Status status = writer_->append(doris::Slice(buffer, size));
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        return static_cast<int32_t>(size);
    }

    Status Flush() override { return Status::OK(); }

    Result<int64_t> GetPos() const override {
        return static_cast<int64_t>(writer_->bytes_appended());
    }

    Result<std::string> GetUri() const override { return path_; }

    Status Close() override { return to_paimon_status(writer_->close()); }

private:
    doris::io::FileWriterPtr writer_;
    std::string path_;
};

class DorisBasicFileStatus : public BasicFileStatus {
public:
    DorisBasicFileStatus(std::string path, bool is_dir) : path_(std::move(path)), is_dir_(is_dir) {}

    bool IsDir() const override { return is_dir_; }
    std::string GetPath() const override { return path_; }

private:
    std::string path_;
    bool is_dir_;
};

class DorisFileStatus : public FileStatus {
public:
    DorisFileStatus(std::string path, bool is_dir, uint64_t length, int64_t mtime)
            : path_(std::move(path)), is_dir_(is_dir), length_(length), mtime_(mtime) {}

    uint64_t GetLen() const override { return length_; }
    bool IsDir() const override { return is_dir_; }
    std::string GetPath() const override { return path_; }
    int64_t GetModificationTime() const override { return mtime_; }

private:
    std::string path_;
    bool is_dir_;
    uint64_t length_;
    int64_t mtime_;
};

class DorisFileSystem : public FileSystem {
public:
    explicit DorisFileSystem(std::map<std::string, std::string> options)
            : options_(std::move(options)) {
        auto it = options_.find("fs.defaultFS");
        if (it != options_.end()) {
            default_fs_name_ = it->second;
        }
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        auto& fs = resolved.first;
        auto& normalized_path = resolved.second;
        doris::io::FileReaderSPtr reader;
        doris::io::FileReaderOptions reader_options = doris::io::FileReaderOptions::DEFAULT;
        doris::Status status = fs->open_file(normalized_path, &reader, &reader_options);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        return std::make_unique<DorisInputStream>(std::move(reader), normalized_path);
    }

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        auto& fs = resolved.first;
        auto& normalized_path = resolved.second;
        if (!overwrite) {
            bool exists = false;
            doris::Status exists_status = fs->exists(normalized_path, &exists);
            if (!exists_status.ok()) {
                return to_paimon_status(exists_status);
            }
            if (exists) {
                return Status::Exist("file already exists: ", normalized_path);
            }
        }
        std::string parent = parent_path(normalized_path);
        if (!parent.empty()) {
            doris::Status mkdir_status = fs->create_directory(parent);
            if (!mkdir_status.ok()) {
                return to_paimon_status(mkdir_status);
            }
        }
        doris::io::FileWriterPtr writer;
        doris::Status status = fs->create_file(normalized_path, &writer);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        return std::make_unique<DorisOutputStream>(std::move(writer), normalized_path);
    }

    Status Mkdirs(const std::string& path) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        doris::Status status = resolved.first->create_directory(resolved.second);
        return to_paimon_status(status);
    }

    Status Rename(const std::string& src, const std::string& dst) const override {
        PAIMON_ASSIGN_OR_RAISE(auto src_resolved, resolve_path(src));
        PAIMON_ASSIGN_OR_RAISE(auto dst_resolved, resolve_path(dst));
        doris::Status status = src_resolved.first->rename(src_resolved.second, dst_resolved.second);
        return to_paimon_status(status);
    }

    Status Delete(const std::string& path, bool recursive = true) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        bool exists = false;
        doris::Status exists_status = resolved.first->exists(resolved.second, &exists);
        if (!exists_status.ok()) {
            return to_paimon_status(exists_status);
        }
        if (!exists) {
            return Status::OK();
        }
        int64_t size = 0;
        doris::Status size_status = resolved.first->file_size(resolved.second, &size);
        if (size_status.ok()) {
            return to_paimon_status(resolved.first->delete_file(resolved.second));
        }
        if (recursive) {
            return to_paimon_status(resolved.first->delete_directory(resolved.second));
        }
        return to_paimon_status(size_status);
    }

    Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override {
        ParsedUri uri = parse_uri(path);
        doris::TFileType::type type = map_scheme_to_file_type(uri.scheme);
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        bool exists = false;
        doris::Status exists_status = resolved.first->exists(resolved.second, &exists);
        if (!exists_status.ok()) {
            return to_paimon_status(exists_status);
        }
        if (!exists) {
            if (type != doris::TFileType::FILE_S3) {
                return Status::NotExist("path not exists: ", resolved.second);
            }
            std::vector<doris::io::FileInfo> files;
            bool list_exists = false;
            doris::Status list_status =
                    resolved.first->list(resolved.second, false, &files, &list_exists);
            if (!list_status.ok()) {
                return to_paimon_status(list_status);
            }
            if (!list_exists && files.empty()) {
                return Status::NotExist("path not exists: ", resolved.second);
            }
            return std::make_unique<DorisFileStatus>(resolved.second, true, 0, 0);
        }
        int64_t size = 0;
        doris::Status size_status = resolved.first->file_size(resolved.second, &size);
        if (size_status.ok()) {
            return std::make_unique<DorisFileStatus>(resolved.second, false,
                                                     static_cast<uint64_t>(size), 0);
        }
        std::vector<doris::io::FileInfo> files;
        bool list_exists = false;
        doris::Status list_status =
                resolved.first->list(resolved.second, false, &files, &list_exists);
        if (!list_status.ok()) {
            return to_paimon_status(list_status);
        }
        if (!list_exists && files.empty()) {
            return Status::NotExist("path not exists: ", resolved.second);
        }
        return std::make_unique<DorisFileStatus>(resolved.second, true, 0, 0);
    }

    Status ListDir(const std::string& directory,
                   std::vector<std::unique_ptr<BasicFileStatus>>* status_list) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(directory));
        auto file_status = GetFileStatus(directory);
        if (file_status.ok() && !file_status.value()->IsDir()) {
            return Status::IOError("path is not a directory: ", directory);
        }
        std::vector<doris::io::FileInfo> files;
        bool exists = false;
        doris::Status status = resolved.first->list(resolved.second, false, &files, &exists);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        if (!exists) {
            return Status::OK();
        }
        status_list->reserve(status_list->size() + files.size());
        for (const auto& file : files) {
            status_list->emplace_back(std::make_unique<DorisBasicFileStatus>(
                    join_path(resolved.second, file.file_name), !file.is_file));
        }
        return Status::OK();
    }

    Status ListFileStatus(const std::string& path,
                          std::vector<std::unique_ptr<FileStatus>>* status_list) const override {
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        auto self_status = GetFileStatus(path);
        if (!self_status.ok()) {
            if (self_status.status().IsNotExist()) {
                return Status::OK();
            }
            return self_status.status();
        }
        if (!self_status.value()->IsDir()) {
            status_list->emplace_back(std::move(self_status).value());
            return Status::OK();
        }
        std::vector<doris::io::FileInfo> files;
        bool exists = false;
        doris::Status list_status = resolved.first->list(resolved.second, false, &files, &exists);
        if (!list_status.ok()) {
            return to_paimon_status(list_status);
        }
        if (!exists) {
            return Status::OK();
        }
        status_list->reserve(status_list->size() + files.size());
        for (const auto& file : files) {
            uint64_t length = file.is_file ? static_cast<uint64_t>(file.file_size) : 0;
            status_list->emplace_back(std::make_unique<DorisFileStatus>(
                    join_path(resolved.second, file.file_name), !file.is_file, length, 0));
        }
        return Status::OK();
    }

    Result<bool> Exists(const std::string& path) const override {
        ParsedUri uri = parse_uri(path);
        doris::TFileType::type type = map_scheme_to_file_type(uri.scheme);
        PAIMON_ASSIGN_OR_RAISE(auto resolved, resolve_path(path));
        bool exists = false;
        doris::Status status = resolved.first->exists(resolved.second, &exists);
        if (!status.ok()) {
            return to_paimon_status(status);
        }
        if (!exists && type == doris::TFileType::FILE_S3) {
            std::vector<doris::io::FileInfo> files;
            bool list_exists = false;
            doris::Status list_status =
                    resolved.first->list(resolved.second, false, &files, &list_exists);
            if (!list_status.ok()) {
                return to_paimon_status(list_status);
            }
            return list_exists || !files.empty();
        }
        return exists;
    }

private:
    Result<std::pair<doris::io::FileSystemSPtr, std::string>> resolve_path(
            const std::string& path) const {
        auto uri = parse_uri(path);
        doris::TFileType::type type = map_scheme_to_file_type(uri.scheme);
        std::string normalized_path = normalize_path_for_type(path, uri.scheme, type);
        if (type == doris::TFileType::FILE_LOCAL) {
            doris::io::FileSystemSPtr fs = doris::io::global_local_filesystem();
            return std::make_pair(std::move(fs), normalized_path);
        }
        std::string fs_key = build_fs_cache_key(type, uri, default_fs_name_);
        {
            std::lock_guard lock(fs_lock_);
            auto it = fs_cache_.find(fs_key);
            if (it != fs_cache_.end()) {
                return std::make_pair(it->second, normalized_path);
            }
        }
        doris::io::FSPropertiesRef fs_properties(type);
        const std::map<std::string, std::string>* properties = &options_;
        std::map<std::string, std::string> properties_override;
        if (type == doris::TFileType::FILE_HTTP && !options_.contains("uri") &&
            !uri.scheme.empty()) {
            properties_override = options_;
            properties_override["uri"] = uri.scheme + "://" + uri.authority;
            properties = &properties_override;
        }
        fs_properties.properties = properties;
        if (!broker_addresses_.empty()) {
            fs_properties.broker_addresses = &broker_addresses_;
        }
        doris::io::FileDescription file_description = {
                .path = normalized_path, .file_size = -1, .mtime = 0, .fs_name = default_fs_name_};
        auto fs_result = doris::FileFactory::create_fs(fs_properties, file_description);
        if (!fs_result.has_value()) {
            return to_paimon_status(fs_result.error());
        }
        doris::io::FileSystemSPtr fs = std::move(fs_result).value();
        {
            std::lock_guard lock(fs_lock_);
            fs_cache_.emplace(std::move(fs_key), fs);
        }
        return std::make_pair(std::move(fs), std::move(normalized_path));
    }

    std::map<std::string, std::string> options_;
    std::vector<doris::TNetworkAddress> broker_addresses_;
    std::string default_fs_name_;
    mutable std::mutex fs_lock_;
    mutable std::unordered_map<std::string, doris::io::FileSystemSPtr> fs_cache_;
};

class DorisFileSystemFactory : public FileSystemFactory {
public:
    static const char IDENTIFIER[];

    const char* Identifier() const override { return IDENTIFIER; }

    Result<std::unique_ptr<FileSystem>> Create(
            const std::string& path,
            const std::map<std::string, std::string>& options) const override {
        return std::make_unique<DorisFileSystem>(options);
    }
};

const char DorisFileSystemFactory::IDENTIFIER[] = "doris";

REGISTER_PAIMON_FACTORY(DorisFileSystemFactory);

} // namespace paimon

namespace doris::vectorized {

void register_paimon_doris_file_system() {}

} // namespace doris::vectorized
