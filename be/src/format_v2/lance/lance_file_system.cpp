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

#include "format_v2/lance/lance_file_system.h"

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <utility>
#include <vector>

#include "io/file_factory.h"
#include "io/fs/http_file_reader.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/md5.h"

namespace doris::format::lance {

namespace {
struct StorageUri {
    std::string scheme;
    std::string authority;
    std::string path;
};

int hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

Result<StorageUri> parse_storage_uri(const std::string& uri) {
    StorageUri result;
    auto start = uri.find("://");
    if (start == std::string::npos) {
        result.path = uri;
        return result;
    }
    result.scheme = uri.substr(0, start);
    start += 3;
    auto end = uri.find('/', start);
    result.authority = uri.substr(start, end - start);
    const auto encoded =
            end == std::string::npos ? std::string_view("/") : std::string_view(uri).substr(end);
    if (result.authority.find('@') != std::string::npos ||
        encoded.find_first_of("?#") != std::string_view::npos) {
        return ResultError(Status::InvalidArgument("Host storage requires a canonical object URI"));
    }
    for (size_t i = 0; i < encoded.size(); ++i) {
        char c = encoded[i];
        if (c == '%') {
            if (i + 2 >= encoded.size() || hex_digit(encoded[i + 1]) < 0 ||
                hex_digit(encoded[i + 2]) < 0) {
                return ResultError(Status::InvalidArgument("Invalid escaped storage URI"));
            }
            c = static_cast<char>((hex_digit(encoded[i + 1]) << 4) | hex_digit(encoded[i + 2]));
            i += 2;
        }
        if (c == '\0') return ResultError(Status::InvalidArgument("NUL in storage path"));
        result.path += c;
    }
    return result;
}

std::string object_uri(const StorageUri& base, std::string_view path) {
    if (base.scheme.empty()) return std::string(path);
    constexpr char hex[] = "0123456789ABCDEF";
    auto result = base.scheme + "://" + base.authority;
    for (unsigned char c : path) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '/' || c == '-' || c == '_' || c == '.' || c == '~') {
            result += static_cast<char>(c);
        } else {
            result += '%';
            result += hex[c >> 4];
            result += hex[c & 15];
        }
    }
    return result;
}
} // namespace

struct LanceFileSystem::OpenFile {
    LanceFileSystem* owner;
    io::FileReaderSPtr reader;
    std::mutex mutex;
    bool cached;
    bool remote;

    ~OpenFile() {
        reader.reset();
        release(owner);
    }
};

Result<std::shared_ptr<LanceFileSystem>> LanceFileSystem::create(const std::string& uri,
                                                                 const char* const* storage_options,
                                                                 RuntimeState* state) {
    auto storage =
            std::shared_ptr<LanceFileSystem>(new LanceFileSystem, [](auto* p) { release(p); });
    RETURN_IF_ERROR_RESULT(storage->initialize(uri, storage_options, state));
    return storage;
}

Status LanceFileSystem::initialize(const std::string& uri, const char* const* options,
                                   RuntimeState* state) {
    if (options) {
        for (size_t i = 0; options[i] != nullptr; i += 2) {
            if (options[i + 1] == nullptr)
                return Status::InvalidArgument("Incomplete Lance storage option");
            _properties.emplace(options[i], options[i + 1]);
        }
    }
    static const std::map<std::string, std::string> aliases {
            {"aws_access_key_id", "AWS_ACCESS_KEY"},
            {"aws_secret_access_key", "AWS_SECRET_KEY"},
            {"aws_session_token", "AWS_TOKEN"},
            {"aws_region", "AWS_REGION"},
            {"aws_endpoint", "AWS_ENDPOINT"},
            {"aws_endpoint_url", "AWS_ENDPOINT"},
            {"oss_access_key_id", "AWS_ACCESS_KEY"},
            {"oss_access_key_secret", "AWS_SECRET_KEY"},
            {"oss_secret_access_key", "AWS_SECRET_KEY"},
            {"oss_security_token", "AWS_TOKEN"},
            {"oss_region", "AWS_REGION"},
            {"oss_endpoint", "AWS_ENDPOINT"}};
    for (const auto& [from, to] : aliases) {
        if (auto it = _properties.find(from); it != _properties.end()) {
            _properties.try_emplace(to, it->second);
        }
    }
    // object_store gives the S3-specific endpoint precedence over the generic endpoint.
    if (auto it = _properties.find("aws_endpoint_url_s3"); it != _properties.end()) {
        _properties["AWS_ENDPOINT"] = it->second;
    }
    const auto parsed = DORIS_TRY(parse_storage_uri(uri));
    if (parsed.scheme == "s3" || parsed.scheme == "s3a" || parsed.scheme == "oss") {
        if (!_properties.contains("AWS_REGION")) {
            const char* region = std::getenv("AWS_REGION");
            if (!region || !*region) region = std::getenv("AWS_DEFAULT_REGION");
            _properties["AWS_REGION"] = region && *region ? region : "us-east-1";
        }
        if (!_properties.contains("AWS_ENDPOINT")) {
            if (parsed.scheme == "oss")
                return Status::InvalidArgument("Lance OSS requires oss_endpoint");
            _properties["AWS_ENDPOINT"] =
                    "https://s3." + _properties.at("AWS_REGION") + ".amazonaws.com";
        }
    }
    if (auto it = _properties.find("aws_virtual_hosted_style_request"); it != _properties.end()) {
        if (it->second != "true" && it->second != "false") {
            return Status::InvalidArgument("Invalid Lance virtual-hosted-style option");
        }
        _properties.try_emplace("use_path_style", it->second == "true" ? "false" : "true");
    }
    if (parsed.scheme == "s3" || parsed.scheme == "s3a" || parsed.scheme == "oss") {
        // Preserve object_store's path-style default instead of the native client's default.
        _properties.try_emplace("use_path_style", "true");
    }
    if (state) {
        _query_options = state->query_options();
    }
    Md5Digest digest;
    const auto append = [&](const std::string& part) {
        const auto framed = std::to_string(part.size()) + ":" + part;
        digest.update(framed.data(), framed.size());
    };
    append(uri);
    for (const auto& [key, value] : _properties) {
        append(key);
        append(value);
    }
    digest.digest();
    _storage_namespace = "lance-" + digest.hex();
    // Cached SDK indices may retain readers across queries. Partition them by read policy,
    // while keeping the physical file-cache identity independent of that query policy.
    Md5Digest policy;
    const auto identity =
            _storage_namespace + ":" +
            std::to_string(_query_options.__isset.enable_file_cache &&
                           _query_options.enable_file_cache) +
            ":" +
            (_query_options.__isset.file_cache_base_path ? _query_options.file_cache_base_path
                                                         : "");
    policy.update(identity.data(), identity.size());
    policy.digest();
    _cache_namespace = "lance-" + policy.hex();
    return Status::OK();
}

LanceHostStorageOptionsV1 LanceFileSystem::options() {
    return {.abi_version = 1,
            .struct_size = sizeof(LanceHostStorageOptionsV1),
            .context = this,
            .retain_context = retain,
            .release_context = release,
            .cache_namespace = _cache_namespace.c_str(),
            .open = open,
            .read_at = read_at,
            .close = close,
            .list = list};
}

LanceFileSystem::Statistics LanceFileSystem::statistics() const {
    return {_cache_bytes.load(std::memory_order_relaxed),
            _remote_bytes.load(std::memory_order_relaxed)};
}

void LanceFileSystem::retain(void* context) {
    static_cast<LanceFileSystem*>(context)->_refs.fetch_add(1, std::memory_order_relaxed);
}

void LanceFileSystem::release(void* context) {
    auto* storage = static_cast<LanceFileSystem*>(context);
    if (storage->_refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete storage;
}

Status LanceFileSystem::check_cancelled() const {
    // A cached SDK index can retain this adapter beyond its opening query. Cancellation and
    // query attribution must follow the task executing now, never that original query.
    if (thread_context()->is_attach_task() &&
        thread_context()->resource_ctx()->task_controller()->is_cancelled()) {
        return Status::Cancelled("Lance storage query ended");
    }
    return Status::OK();
}

Status LanceFileSystem::filesystem(const std::string& uri, io::FileSystemSPtr* fs,
                                   std::string* path) {
    const auto parsed = DORIS_TRY(parse_storage_uri(uri));
    const auto& scheme = parsed.scheme;
    io::FileSystemProperties properties;
    properties.properties = _properties;
    if (scheme == "file" || scheme == "file+uring" || scheme.empty()) {
        if (!parsed.authority.empty() && parsed.authority != "localhost") {
            return Status::NotSupported("Remote file URI authority is not supported");
        }
        properties.system_type = TFileType::FILE_LOCAL;
        *path = parsed.path;
    } else if (scheme == "s3" || scheme == "s3a" || scheme == "oss") {
        if (parsed.authority.empty()) return Status::InvalidArgument("Missing storage bucket");
        properties.system_type = TFileType::FILE_S3;
        // Create the Doris client from the bucket, then pass decoded keys directly to its
        // reader/list APIs. Parsing them as S3URI would strip literal '?' and '#' in keys.
        *path = "s3://" + parsed.authority + "/lance-host";
    } else if (scheme == "hdfs") {
        properties.system_type = TFileType::FILE_HDFS;
        *path = scheme + "://" + parsed.authority + parsed.path;
    } else if (scheme == "http" || scheme == "https") {
        properties.system_type = TFileType::FILE_HTTP;
        properties.properties["uri"] = uri;
        *path = uri;
    } else {
        return Status::NotSupported("Lance host storage does not support URI scheme '{}'", scheme);
    }
    const auto key = scheme + "://" + parsed.authority;
    std::lock_guard lock(_fs_mutex);
    auto it = _filesystems.find(key);
    if (it == _filesystems.end()) {
        io::FileDescription description;
        description.path = *path;
        auto created = FileFactory::create_fs(io::FSPropertiesRef(properties), description);
        if (!created.has_value()) return std::move(created).error();
        it = _filesystems.emplace(key, std::move(created).value()).first;
    }
    *fs = it->second;
    return Status::OK();
}

Status LanceFileSystem::open_file(const char* uri, OpenFile** out, LanceHostFileMeta* meta) {
    RETURN_IF_ERROR(check_cancelled());
    io::FileSystemSPtr fs;
    std::string path;
    RETURN_IF_ERROR(filesystem(uri, &fs, &path));
    io::FileReaderSPtr reader;
    io::FileReaderOptions options;
    const auto parsed = DORIS_TRY(parse_storage_uri(uri));
    // Data filenames are immutable. Mutable manifests and control files must bypass both caches.
    const bool immutable_data =
            parsed.path.ends_with(".lance") &&
            std::filesystem::path(parsed.path).parent_path().filename() == "data";
    if (immutable_data && fs->type() != io::FileSystemType::LOCAL) {
        std::lock_guard lock(_fs_mutex);
        if (auto it = _immutable_metadata.find(uri); it != _immutable_metadata.end()) {
            options.file_size = static_cast<int64_t>(it->second.size);
            options.mtime = it->second.modified_seconds;
        }
    }
    Status status;
    if (fs->type() == io::FileSystemType::S3) {
        const auto& s3 = static_cast<io::S3FileSystem&>(*fs);
        auto result = io::S3FileReader::create(s3.client_holder(), s3.bucket(),
                                               parsed.path.substr(1), options.file_size, nullptr);
        if (!result.has_value()) return std::move(result).error();
        reader = std::move(result).value();
        path = parsed.path;
    } else if (fs->type() == io::FileSystemType::HTTP) {
        auto result = io::HttpFileReader::create(uri, _properties, options, nullptr);
        if (!result.has_value()) return std::move(result).error();
        reader = std::move(result).value();
        path = parsed.path;
    } else {
        status = fs->open_file(path, &reader, &options);
        if (!status.ok()) {
            bool exists = true;
            if (fs->exists(path, &exists).ok() && !exists) {
                return Status::NotFound("Lance storage object does not exist");
            }
            return status;
        }
    }
    if (reader->size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("Lance storage file is too large");
    }
    meta->size = reader->size();
    meta->modified_seconds = reader->mtime();
    if (immutable_data && fs->type() != io::FileSystemType::LOCAL) {
        // ObjectStore range calls open a fresh native reader. Bound the metadata map and
        // reuse immutable sizes so a cache hit does not issue a remote HEAD for every range.
        std::lock_guard lock(_fs_mutex);
        constexpr size_t MAX_CACHED_FILE_METADATA = 1024;
        if (_immutable_metadata.size() >= MAX_CACHED_FILE_METADATA)
            _immutable_metadata.erase(_immutable_metadata.begin());
        _immutable_metadata[uri] = *meta;
    }
    io::FileDescription description;
    description.path = path;
    description.file_size = static_cast<int64_t>(reader->size());
    description.mtime = reader->mtime();
    description.is_immutable = immutable_data;
    description.file_cache_admission = immutable_data;
    options = FileFactory::get_reader_options(_query_options, description);
    // FileReader paths do not necessarily include the storage endpoint or credential scope.
    options.cache_key = _storage_namespace + ":" + uri;
    const bool cached = fs->type() != io::FileSystemType::LOCAL &&
                        options.cache_type == io::FileCachePolicy::FILE_BLOCK_CACHE;
    if (cached) {
        auto result = io::create_cached_file_reader(reader, options);
        if (!result.has_value()) return std::move(result).error();
        reader = std::move(result).value();
    }
    auto file = std::unique_ptr<OpenFile>(new OpenFile {
            this, std::move(reader), {}, cached, fs->type() != io::FileSystemType::LOCAL});
    retain(this);
    *out = file.release();
    return Status::OK();
}

int32_t LanceFileSystem::report(const Status& status, const char** error) noexcept {
    if (status.ok()) return 0;
    try {
        thread_local std::string message;
        message = status.to_string();
        if (error) *error = message.c_str();
    } catch (...) {
        if (error) *error = "Lance host storage failed";
    }
    return status.code() == ErrorCode::NOT_FOUND ? 1 : 2;
}

int32_t LanceFileSystem::open(void* context, const char* uri, void** file, LanceHostFileMeta* meta,
                              const char** error) noexcept {
    if (file) *file = nullptr;
    try {
        SCOPED_INIT_THREAD_CONTEXT();
        if (!context || !uri || !file || !meta)
            return report(Status::InvalidArgument("Invalid host open arguments"), error);
        OpenFile* result = nullptr;
        const auto status = static_cast<LanceFileSystem*>(context)->open_file(uri, &result, meta);
        *file = result;
        return report(status, error);
    } catch (...) {
        if (error) *error = "Lance host open threw an exception";
        return 2;
    }
}

int32_t LanceFileSystem::read_at(void* context, uint64_t offset, uint8_t* buffer, size_t length,
                                 size_t* bytes_read, const char** error) noexcept {
    if (bytes_read) *bytes_read = 0;
    try {
        SCOPED_INIT_THREAD_CONTEXT();
        if (!context || !bytes_read || (!buffer && length != 0) ||
            offset > std::numeric_limits<size_t>::max())
            return report(Status::InvalidArgument("Invalid host read arguments"), error);
        auto* file = static_cast<OpenFile*>(context);
        auto status = file->owner->check_cancelled();
        if (!status.ok()) return report(status, error);
        io::FileCacheStatistics statistics;
        io::IOContext io_context;
        io_context.reader_type = ReaderType::READER_QUERY;
        io_context.file_cache_stats = &statistics;
        TUniqueId query_id;
        if (thread_context()->is_attach_task()) {
            query_id = thread_context()->resource_ctx()->task_controller()->task_id();
            io_context.query_id = &query_id;
        }
        // Some native readers accumulate non-atomic statistics, so serialize one handle's reads.
        std::lock_guard lock(file->mutex);
        status = file->reader->read_at(static_cast<size_t>(offset), Slice(buffer, length),
                                       bytes_read, &io_context);
        if (file->cached) {
            file->owner->_cache_bytes.fetch_add(statistics.bytes_read_from_local,
                                                std::memory_order_relaxed);
            file->owner->_remote_bytes.fetch_add(statistics.bytes_read_from_remote,
                                                 std::memory_order_relaxed);
        } else if (file->remote) {
            file->owner->_remote_bytes.fetch_add(*bytes_read, std::memory_order_relaxed);
        }
        return report(status, error);
    } catch (...) {
        if (error) *error = "Lance host read threw an exception";
        return 2;
    }
}

void LanceFileSystem::close(void* context) noexcept {
    std::unique_ptr<OpenFile> file(static_cast<OpenFile*>(context));
    try {
        SCOPED_INIT_THREAD_CONTEXT();
        if (file) static_cast<void>(file->reader->close());
    } catch (...) {
    }
}

Status LanceFileSystem::list_files(const char* uri, LanceHostListEntry emit, void* context) {
    RETURN_IF_ERROR(check_cancelled());
    std::vector<std::string> pending {uri};
    while (!pending.empty()) {
        auto directory = std::move(pending.back());
        pending.pop_back();
        io::FileSystemSPtr fs;
        std::string path;
        RETURN_IF_ERROR(filesystem(directory, &fs, &path));
        const auto parsed = DORIS_TRY(parse_storage_uri(directory));
        auto prefix = parsed.path;
        if (!prefix.ends_with('/')) prefix += '/';
        std::vector<io::FileInfo> files;
        if (fs->type() == io::FileSystemType::S3) {
            const auto& s3 = static_cast<io::S3FileSystem&>(*fs);
            const auto client = s3.client_holder()->get();
            if (!client) return Status::InternalError("Lance storage client unavailable");
            std::vector<io::ObjectMeta> objects;
            auto response = client->list_objects(
                    {.bucket = s3.bucket(), .prefix = prefix.substr(1)}, &objects);
            if (!response.ok()) return {response.status.code, std::move(response.status.msg)};
            for (const auto& object : objects) {
                RETURN_IF_ERROR(check_cancelled());
                if (object.key.ends_with('/')) continue;
                auto child = object_uri(parsed, "/" + object.key);
                LanceHostFileMeta meta {.size = static_cast<uint64_t>(object.size),
                                        .modified_seconds = 0};
                if (emit(context, child.c_str(), &meta) != 0)
                    return Status::IOError("Host listing receiver rejected an entry");
            }
            continue;
        }
        bool exists = false;
        RETURN_IF_ERROR(fs->list(path, false, &files, &exists));
        if (!exists) continue;
        for (const auto& file : files) {
            auto child = object_uri(parsed, prefix + file.file_name);
            if (!file.is_file) {
                pending.push_back(std::move(child));
                continue;
            }
            LanceHostFileMeta meta {.size = static_cast<uint64_t>(file.file_size),
                                    .modified_seconds = 0};
            if (emit(context, child.c_str(), &meta) != 0)
                return Status::IOError("Host listing receiver rejected an entry");
        }
        RETURN_IF_ERROR(check_cancelled());
    }
    return Status::OK();
}

int32_t LanceFileSystem::list(void* storage, const char* uri, LanceHostListEntry emit,
                              void* context, const char** error) noexcept {
    try {
        SCOPED_INIT_THREAD_CONTEXT();
        if (!storage || !uri || !emit)
            return report(Status::InvalidArgument("Invalid host list arguments"), error);
        return report(static_cast<LanceFileSystem*>(storage)->list_files(uri, emit, context),
                      error);
    } catch (...) {
        if (error) *error = "Lance host list threw an exception";
        return 2;
    }
}

} // namespace doris::format::lance
