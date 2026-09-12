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

#include <paimon/fs/file_system.h>

#include <mutex>
#include <set>

#include "io/fs/file_system.h"

namespace doris {
class ResourceContext;

// Writer-only adapter for Paimon 0.3.0. The logical table location remains unchanged;
// only IO paths are mapped onto FE-resolved Doris storage. No SDK storage plugins or
// independently interpreted Hadoop credentials are involved.
class DorisPaimonFileSystem final : public paimon::FileSystem {
public:
    DorisPaimonFileSystem(io::FileSystemSPtr fs, std::string table_root, std::string storage_root,
                          std::shared_ptr<ResourceContext> context);
    ~DorisPaimonFileSystem() override;
    // Call only after SDK shutdown and stream draining. Failed deletions remain owned
    // for retry. Destruction without a successful handoff performs best-effort cleanup.
    paimon::Status cleanup_owned_files();
    void release_owned_files();
    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(
            const std::string& path) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override;
    paimon::Status Mkdirs(const std::string& path) const override;
    paimon::Status Rename(const std::string& src, const std::string& dst) const override;
    paimon::Status Delete(const std::string& path, bool recursive = true) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(
            const std::string& path) const override;
    paimon::Status ListDir(
            const std::string& path,
            std::vector<std::unique_ptr<paimon::BasicFileStatus>>* result) const override;
    paimon::Status ListFileStatus(
            const std::string& path,
            std::vector<std::unique_ptr<paimon::FileStatus>>* result) const override;
    paimon::Result<bool> Exists(const std::string& path) const override;
    paimon::Status WriteFile(const std::string& path, const std::string& content,
                             bool overwrite) override;
    paimon::Status AtomicStore(const std::string& path, const std::string& content) override;

private:
    paimon::Result<std::string> storage_path(const std::string& path) const;
    io::FileSystemSPtr _fs;
    std::string _table_root;
    std::string _storage_root;
    std::shared_ptr<ResourceContext> _context;
    mutable std::mutex _owned_mutex;
    mutable std::set<std::string> _owned_files;
    bool _ownership_finished = false;
};
} // namespace doris
