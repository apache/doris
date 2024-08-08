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

#include "recycler/storage_vault.h"

#include <cpp/remote_path_helper.h>
#include <glog/logging.h>

#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "gen_cpp/cloud.pb.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/inverted_index_info_cache.h"
#include "recycler/s3_accessor.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/sync_executor.h"

namespace doris::cloud {

StorageVault::~StorageVault() = default;

std::string StorageVault::inverted_index_path_v1(int64_t tablet_id, std::string_view rowset_id,
                                                 int64_t segment_id, int64_t inverted_index_id,
                                                 std::string_view index_path_suffix) const {
    // path v0: data/${tablet_id}/${rowset_id}_${seg_id}_${inverted_index_id}[@${index_path_suffix}].idx
    // path v1: data/${shard_id}/${tablet_id}/${rowset_id}/${seg_id}_${inverted_index_id}[@${index_path_suffix}].idx
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return fmt::format("{}{}_{}{}.idx", rowset_path_prefix(tablet_id, rowset_id), segment_id,
                       inverted_index_id, suffix);
}

std::string StorageVault::inverted_index_path_v2(int64_t tablet_id, std::string_view rowset_id,
                                                 int64_t segment_id) const {
    // path v0: data/${tablet_id}/${rowset_id}_${seg_id}.idx
    // path v1: data/${shard_id}/${tablet_id}/${rowset_id}/${seg_id}.idx
    return fmt::format("{}{}.idx", rowset_path_prefix(tablet_id, rowset_id), segment_id);
}

std::optional<StorageVault> StorageVault::create(const ObjectStoreInfoPB& obj_store_info,
                                                 const RemotePathContext& ctx) {
    auto s3_conf = S3Conf::from_obj_store_info(obj_store_info);
    if (!s3_conf) {
        return std::nullopt;
    }

    std::shared_ptr<S3Accessor> accessor;
    if (S3Accessor::create(*s3_conf, &accessor) != 0) {
        return std::nullopt;
    }

    return StorageVault(std::move(accessor), RemotePathHelper(ctx));
}

std::optional<StorageVault> StorageVault::create(const StorageVaultPB& storage_vault_pb) {
    RemotePathContext ctx;
    if (storage_vault_pb.has_path_format()) {
        ctx.resource_id = storage_vault_pb.id();
        ctx.path_version = storage_vault_pb.path_format().path_version();
        ctx.num_shards = storage_vault_pb.path_format().shard_num();
    }

    if (storage_vault_pb.has_obj_info()) {
        return create(storage_vault_pb.obj_info(), ctx);
    } else if (storage_vault_pb.has_hdfs_info()) {
        auto accessor = std::make_shared<HdfsAccessor>(storage_vault_pb.hdfs_info());
        if (accessor->init() != 0) {
            return std::nullopt;
        }

        return StorageVault(std::move(accessor), RemotePathHelper(ctx));
    } else {
        LOG_WARNING("Unsupported storage vault type").tag("resource_id", storage_vault_pb.id());
        return std::nullopt;
    }
}

class S3StorageVaultRecycler final : public StorageVaultRecycler {
public:
    explicit S3StorageVaultRecycler(StorageVault storage_vault,
                                    std::shared_ptr<SimpleThreadPool> worker_pool,
                                    InvertedIndexInfoCache& inverted_index_info_cache)
            : storage_vault_(std::move(storage_vault)),
              executor_(std::make_unique<SyncExecutor<int>>(std::move(worker_pool),
                                                            "S3StorageVaultRecycler")),
              inverted_index_info_cache_(inverted_index_info_cache) {}

    ~S3StorageVaultRecycler() override = default;

    int submit_delete_rowset(const RowsetMetaCloudPB& rowset) override {
        int64_t num_segments = rowset.num_segments();
        if (num_segments <= 0) {
            return 0;
        }

        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [this](int*) {
            if (to_delete_paths_.size() >= delete_batch_size) {
                executor_->add([this, paths = std::move(to_delete_paths_)]() -> int {
                    return storage_vault_.accessor()->delete_files(paths);
                });
            }
        });

        InvertedIndexInfo inverted_index_info;
        if (rowset.has_tablet_schema()) {
            inverted_index_info = into_inverted_index_info(rowset.tablet_schema());
        } else {
            int res = inverted_index_info_cache_.get(rowset.index_id(), rowset.schema_version(),
                                                     inverted_index_info);
            if (res != 0) {
                if (res == 1) {
                    // Schema kv not found, treat as rowset is in dropped materialized index, do nothing
                    return 0;
                }
                // Other errors
                return res;
            }
        }

        for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
            to_delete_paths_.push_back(
                    storage_vault_.segment_path(rowset.tablet_id(), rowset.rowset_id_v2(), seg_id));
        }

        if (std::holds_alternative<InvertedIndexInfoV1>(inverted_index_info)) {
            auto& info = std::get<InvertedIndexInfoV1>(inverted_index_info);
            for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
                for (auto&& [index_id, index_path_suffix] : info) {
                    to_delete_paths_.push_back(storage_vault_.inverted_index_path_v1(
                            rowset.tablet_id(), rowset.rowset_id_v2(), seg_id, index_id,
                            index_path_suffix));
                }
            }
        } else if (std::holds_alternative<InvertedIndexInfoV2>(inverted_index_info)) {
            for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
                to_delete_paths_.push_back(storage_vault_.inverted_index_path_v2(
                        rowset.tablet_id(), rowset.rowset_id_v2(), seg_id));
            }
        } else if (std::holds_alternative<UnknownInvertedIndex>(inverted_index_info)) {
            LOG_WARNING("Unknown inverted index format")
                    .tag("index_id", rowset.index_id())
                    .tag("schema_version", rowset.schema_version());
            return -1;
        }

        return 0;
    }

    int submit_delete_rowset(int64_t tablet_id, std::string_view rowset_id) override {
        auto rowset_path = storage_vault_.rowset_path_prefix(tablet_id, rowset_id);
        executor_->add([this, rowset_path = std::move(rowset_path)]() -> int {
            return storage_vault_.accessor()->delete_prefix(rowset_path);
        });
        return 0;
    }

    int submit_delete_tablet(int64_t tablet_id) override {
        executor_->add([this, tablet_id]() -> int {
            return storage_vault_.accessor()->delete_directory(
                    storage_vault_.tablet_path(tablet_id));
        });
        return 0;
    }

    int wait() override {
        if (!to_delete_paths_.empty()) {
            executor_->add([this, paths = std::move(to_delete_paths_)]() -> int {
                return storage_vault_.accessor()->delete_files(paths);
            });
        }

        bool finished;
        auto res = executor_->when_all(&finished);
        executor_->reset();
        return finished && std::all_of(res.begin(), res.end(), [](int r) { return r == 0; }) ? 0
                                                                                             : -1;
    }

private:
    inline static constexpr int delete_batch_size = 128;

    StorageVault storage_vault_;
    std::unique_ptr<SyncExecutor<int>> executor_;
    InvertedIndexInfoCache& inverted_index_info_cache_;
    std::vector<std::string> to_delete_paths_;
};

class HdfsStorageVaultRecyclerV0 final : public StorageVaultRecycler {
public:
    explicit HdfsStorageVaultRecyclerV0(StorageVault storage_vault,
                                        std::shared_ptr<SimpleThreadPool> worker_pool,
                                        InvertedIndexInfoCache& inverted_index_info_cache)
            : storage_vault_(std::move(storage_vault)),
              executor_(std::make_unique<SyncExecutor<int>>(std::move(worker_pool),
                                                            "HdfsStorageVaultRecyclerV0")),
              inverted_index_info_cache_(inverted_index_info_cache) {}

    ~HdfsStorageVaultRecyclerV0() override = default;

    int submit_delete_rowset(const RowsetMetaCloudPB& rowset) override {
        int64_t num_segments = rowset.num_segments();
        if (num_segments <= 0) {
            return 0;
        }

        InvertedIndexInfo inverted_index_info;
        if (rowset.has_tablet_schema()) {
            inverted_index_info = into_inverted_index_info(rowset.tablet_schema());
        } else {
            int res = inverted_index_info_cache_.get(rowset.index_id(), rowset.schema_version(),
                                                     inverted_index_info);
            if (res != 0) {
                if (res == 1) {
                    // Schema kv not found, treat as rowset is in dropped materialized index, do nothing
                    return 0;
                }
                // Other errors
                return res;
            }
        }

        for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto segment_path =
                    storage_vault_.segment_path(rowset.tablet_id(), rowset.rowset_id_v2(), seg_id);
            executor_->add([this, segment_path = std::move(segment_path)]() -> int {
                return storage_vault_.accessor()->delete_file(segment_path);
            });
        }

        if (std::holds_alternative<InvertedIndexInfoV1>(inverted_index_info)) {
            auto& info = std::get<InvertedIndexInfoV1>(inverted_index_info);
            for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
                for (auto&& [index_id, index_path_suffix] : info) {
                    auto path = storage_vault_.inverted_index_path_v1(rowset.tablet_id(),
                                                                      rowset.rowset_id_v2(), seg_id,
                                                                      index_id, index_path_suffix);
                    executor_->add([this, path = std::move(path)]() -> int {
                        return storage_vault_.accessor()->delete_file(path);
                    });
                }
            }
        } else if (std::holds_alternative<InvertedIndexInfoV2>(inverted_index_info)) {
            for (int seg_id = 0; seg_id < num_segments; ++seg_id) {
                auto path = storage_vault_.inverted_index_path_v2(rowset.tablet_id(),
                                                                  rowset.rowset_id_v2(), seg_id);
                executor_->add([this, path = std::move(path)]() -> int {
                    return storage_vault_.accessor()->delete_file(path);
                });
            }
        } else if (std::holds_alternative<UnknownInvertedIndex>(inverted_index_info)) {
            LOG_WARNING("Unknown inverted index format")
                    .tag("index_id", rowset.index_id())
                    .tag("schema_version", rowset.schema_version());
            return -1;
        }

        return 0;
    }

    int submit_delete_rowset(int64_t tablet_id, std::string_view rowset_id) override {
        auto rowset_path = storage_vault_.rowset_path_prefix(tablet_id, rowset_id);
        // This operation does nothing, so it doesn't need to be executed in parallel
        storage_vault_.accessor()->delete_prefix(rowset_path);
        return 0;
    }

    int submit_delete_tablet(int64_t tablet_id) override {
        executor_->add([this, tablet_id]() -> int {
            return storage_vault_.accessor()->delete_directory(
                    storage_vault_.tablet_path(tablet_id));
        });
        return 0;
    }

    int wait() override {
        bool finished;
        auto res = executor_->when_all(&finished);
        executor_->reset();
        return finished && std::all_of(res.begin(), res.end(), [](int r) { return r == 0; }) ? 0
                                                                                             : -1;
    }

private:
    StorageVault storage_vault_;
    std::unique_ptr<SyncExecutor<int>> executor_;
    InvertedIndexInfoCache& inverted_index_info_cache_;
};

class HdfsStorageVaultRecyclerV1 final : public StorageVaultRecycler {
public:
    explicit HdfsStorageVaultRecyclerV1(StorageVault storage_vault,
                                        std::shared_ptr<SimpleThreadPool> worker_pool)
            : storage_vault_(std::move(storage_vault)),
              executor_(std::make_unique<SyncExecutor<int>>(std::move(worker_pool),
                                                            "HdfsStorageVaultRecyclerV1")) {}
    ~HdfsStorageVaultRecyclerV1() override = default;

    int submit_delete_rowset(const RowsetMetaCloudPB& rowset) override {
        return submit_delete_rowset(rowset.tablet_id(), rowset.rowset_id_v2());
    }

    int submit_delete_rowset(int64_t tablet_id, std::string_view rowset_id) override {
        auto rowset_path = storage_vault_.rowset_path_prefix(tablet_id, rowset_id);
        executor_->add([this, rowset_path = std::move(rowset_path)]() -> int {
            return storage_vault_.accessor()->delete_directory(rowset_path);
        });
        return 0;
    }

    int submit_delete_tablet(int64_t tablet_id) override {
        executor_->add([this, tablet_id]() -> int {
            return storage_vault_.accessor()->delete_directory(
                    storage_vault_.tablet_path(tablet_id));
        });
        return 0;
    }

    int wait() override {
        bool finished;
        auto res = executor_->when_all(&finished);
        executor_->reset();
        return finished && std::all_of(res.begin(), res.end(), [](int r) { return r == 0; }) ? 0
                                                                                             : -1;
    }

private:
    StorageVault storage_vault_;
    std::unique_ptr<SyncExecutor<int>> executor_;
};

std::unique_ptr<StorageVaultRecycler> StorageVault::create_recycler(
        std::shared_ptr<SimpleThreadPool> worker_pool,
        InvertedIndexInfoCache& inverted_index_info_cache) const {
    switch (accessor_->type()) {
    case AccessorType::S3:
        return std::make_unique<S3StorageVaultRecycler>(*this, std::move(worker_pool),
                                                        inverted_index_info_cache);
    case AccessorType::HDFS:
        switch (remote_path_helper_.path_version()) {
        case 0:
            return std::make_unique<HdfsStorageVaultRecyclerV0>(*this, std::move(worker_pool),
                                                                inverted_index_info_cache);
        case 1:
            return std::make_unique<HdfsStorageVaultRecyclerV1>(*this, std::move(worker_pool));
        default:
            __builtin_unreachable();
            exit(-1);
        }
    case AccessorType::MOCK:
        return std::make_unique<S3StorageVaultRecycler>(*this, std::move(worker_pool),
                                                        inverted_index_info_cache);
    }
}

} // namespace doris::cloud
