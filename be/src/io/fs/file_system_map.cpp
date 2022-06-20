#include "io/fs/file_system_map.h"

namespace doris {
namespace io {

FileSystemMap* FileSystemMap::instance() {
    static FileSystemMap map;
    return &map;
}

void FileSystemMap::insert(ResourceId id, FileSystemPtr fs) {
    std::unique_lock wlock(_mu);
    _map.try_emplace(std::move(id), std::move(fs));
}

FileSystemPtr FileSystemMap::get(const ResourceId& id) {
    std::shared_lock rlock(_mu);
    auto it = _map.find(id);
    if (it != _map.end()) {
        return it->second;
    }
    return nullptr;
}

} // namespace io
} // namespace doris
