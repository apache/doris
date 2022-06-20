#pragma once

#include "io/fs/file_system.h"
#include "util/file_cache.h"

namespace doris {
namespace io {

class LocalFileSystem final : public FileSystem {
public:
    LocalFileSystem(Path root_path, ResourceId resource_id = ResourceId());
    ~LocalFileSystem() override;

    Status create_file(const Path& path, std::unique_ptr<FileWriter>* writer) override;

    Status open_file(const Path& path, std::unique_ptr<FileReader>* reader) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    Status delete_directory(const Path& path) override;

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

private:
    Path absolute_path(const Path& path) const;

    std::unique_ptr<FileCache<int>> _file_cache;
};

LocalFileSystem* global_local_filesystem();

} // namespace io
} // namespace doris
