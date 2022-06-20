#pragma once

#include "io/fs/file_system.h"

namespace doris {
namespace io {

class RemoteFileSystem : public FileSystem {
public:
    RemoteFileSystem(Path&& root_path, ResourceId&& resource_id, FileSystemType type)
            : FileSystem(std::move(root_path), std::move(resource_id), type) {}
    ~RemoteFileSystem() override = default;

    // `local_path` should be an absolute path on local filesystem.
    virtual Status upload(const Path& local_path, const Path& dest_path) = 0;

    virtual Status batch_upload(const std::vector<Path>& local_paths,
                                const std::vector<Path>& dest_paths) = 0;

    virtual Status connect() = 0;
};

} // namespace io
} // namespace doris
