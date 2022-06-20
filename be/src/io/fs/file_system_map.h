#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "io/fs/file_system.h"

namespace doris {
namespace io {

class FileSystemMap {
public:
    static FileSystemMap* instance();
    ~FileSystemMap() = default;

    void insert(ResourceId id, FileSystemPtr fs);

    // If `id` is not in `_map`, return nullptr.
    FileSystemPtr get(const ResourceId& id);

private:
    FileSystemMap() = default;

private:
    std::shared_mutex _mu;
    std::unordered_map<ResourceId, FileSystemPtr> _map; // GUARED_BY(_mu)
};

} // namespace io
} // namespace doris
