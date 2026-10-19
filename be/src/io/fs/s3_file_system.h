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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/obj_storage/obj_storage_config.h"
#include "util/s3_util.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3
namespace Aws::Utils::Threading {
class PooledThreadExecutor;
} // namespace Aws::Utils::Threading

namespace doris::io {
// One immutable generation of an ObjClientHolder. The conf and the client built from it
// are published together, so a caller never pairs a new conf with an old client. For
// example, a reset from ak1 to ak2 that races with full_s3_path() and get() lets each
// caller see either (ak1 conf, ak1 client) or (ak2 conf, ak2 client), never a mix.
struct ObjClientState {
    // The typed configuration the client is built from.
    ObjClientConfig config;
    // The legacy flat conf `config` was resolved from. Set for holders created from an
    // S3ClientConf (Resource, Storage Vault, S3-style properties), whose reset() merges
    // legacy fields; unset for holders created from a typed config such as the one decoded
    // from TAzureClientConfig, which has no legacy representation (e.g. SAS).
    std::optional<S3ClientConf> legacy_conf {};
    // Null until init() succeeds.
    std::shared_ptr<ObjStorageClient> client;
    // Increases by one on every successful publish (init or reset).
    uint64_t generation = 0;
};

// In runtime, AK and SK may be modified, and the original `S3Client` instance will be replaced.
// The `S3FileReader` cached by the `Segment` must hold a shared `ObjClientHolder` in order to
// access S3 data with latest AK SK.
//
// The holder owns an immutable ObjClientState and replaces it as a whole. A request takes
// a snapshot and calls the SDK outside the lock; requests that took the old snapshot keep
// using the old client until they finish, later requests see the new one.
class ObjClientHolder {
public:
    explicit ObjClientHolder(S3ClientConf conf);
    explicit ObjClientHolder(ObjClientConfig config);
    ~ObjClientHolder();

    Status init();

    // Update s3 conf and reset client if `conf` is different. This method is threadsafe.
    // Only holders created from an S3ClientConf can be reset: Resource and Storage Vault
    // refreshes, the only callers, always build their holders from one.
    //
    // The new client is built outside the lock and published only if no other reset or
    // init published in between. Otherwise the update is rejected rather than applied on
    // top of a state it was not merged with, so an older conf cannot overwrite a newer one;
    // the caller keeps the current state and retries with its next refresh.
    Status reset(const S3ClientConf& conf);

    std::shared_ptr<const ObjClientState> state() const {
        std::shared_lock lock(_mtx);
        return _state;
    }

    // Called for every remote read, so copy only the client rather than the whole snapshot.
    std::shared_ptr<ObjStorageClient> get() const {
        std::shared_lock lock(_mtx);
        return _state->client;
    }

    Result<int64_t> object_file_size(const std::string& bucket, const std::string& key) const;

    // For error msg
    std::string full_s3_path(std::string_view bucket, std::string_view key) const;

    // A copy of the current legacy conf. Only valid for holders created from an S3ClientConf;
    // use state() to read the typed config and client of one generation.
    S3ClientConf s3_client_conf() const;

#ifdef BE_TEST
    // Publishes a new generation that keeps the current config and uses `client`.
    void set_client_for_test(std::shared_ptr<ObjStorageClient> client) {
        std::lock_guard lock(_mtx);
        _state = std::make_shared<ObjClientState>(ObjClientState {
                .config = _state->config,
                .legacy_conf = _state->legacy_conf,
                .client = std::move(client),
                .generation = _state->generation + 1,
        });
    }
#endif

private:
    // Replaces the state with (config, legacy_conf, client) if `expected` is still current.
    Status _publish(const std::shared_ptr<const ObjClientState>& expected, ObjClientConfig config,
                    std::optional<S3ClientConf> legacy_conf,
                    std::shared_ptr<ObjStorageClient> client);

    mutable std::shared_mutex _mtx;
    std::shared_ptr<const ObjClientState> _state;
};

// File system for S3 compatible object storage
// When creating S3FileSystem, all required info should be set in S3Conf,
// such as ak, sk, region, endpoint, bucket.
// And the root_path of S3FileSystem is s3_conf.prefix.
// When using S3FileSystem, it accepts 2 kinds of path:
//  1. Full path: s3://bucket/path/to/file.txt
//      In this case, the root_path is not used.
//  2. only key: path/to/file.txt
//      In this case, the final key will be "prefix + path/to/file.txt"
class S3FileSystem final : public RemoteFileSystem {
public:
    static Result<std::shared_ptr<S3FileSystem>> create(S3Conf s3_conf, std::string id);

    // Typed access, e.g. FE-sent TAzureClientConfig. There is no root prefix: every path is a
    // full location or a key. With an Azure backend, full paths are parsed by the Azure
    // location rules, so abfss://container@acct.dfs.core.windows.net/key works as well as the
    // s3-style s3://container/key.
    static Result<std::shared_ptr<S3FileSystem>> create(ObjClientConfig config, std::string bucket,
                                                        std::string id);

    ~S3FileSystem() override;

    const std::shared_ptr<ObjClientHolder>& client_holder() const { return _client; }

    const std::string& bucket() const { return _bucket; }
    const std::string& prefix() const { return _prefix; }

    std::string generate_presigned_url(const Path& path, int64_t expiration_secs,
                                       bool is_public_endpoint) const;

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                              const FileReaderOptions& opts) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;

    Status absolute_path(const Path& path, Path& abs_path) const override {
        if (path.string().find("://") != std::string::npos) {
            // the path is with schema, which means this is a full path like:
            // s3://bucket/path/to/file.txt
            // so no need to concat with prefix
            abs_path = path;
        } else {
            // path with no schema
            abs_path = _prefix / path;
        }
        return Status::OK();
    }

private:
    S3FileSystem(S3Conf s3_conf, std::string id);
    S3FileSystem(ObjClientConfig config, std::string bucket, std::string id);

    Status init();

    Result<std::string> _get_key(const Path& full_path) const;

    // For error msg
    std::string full_s3_path(std::string_view key) const;

    std::string _bucket;
    std::string _prefix;
    // Set only for typed Azure access; the legacy S3Conf path keeps the s3-style locations
    // FE normalized Azure paths to, so it stays on S3URI.
    bool _native_azure_location = false;
    std::shared_ptr<ObjClientHolder> _client;
};

} // namespace doris::io
