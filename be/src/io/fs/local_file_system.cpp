#include "io/fs/local_file_system.h"

#include "io/fs/file_system.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "olap/storage_engine.h"

namespace doris {
namespace io {

LocalFileSystem::LocalFileSystem(Path root_path, ResourceId resource_id)
        : FileSystem(std::move(root_path), std::move(resource_id), FileSystemType::LOCAL) {
#ifdef BE_TEST
    _file_cache.reset(
            new FileCache<int>("Readable_file_cache", config::file_descriptor_cache_capacity));
#else
    _file_cache.reset(new FileCache<int>("Readable_file_cache",
                                         doris::StorageEngine::instance()->file_cache()));
#endif
}

LocalFileSystem::~LocalFileSystem() = default;

Path LocalFileSystem::absolute_path(const Path& path) const {
    if (path.is_absolute()) {
        return path;
    }
    return _root_path / path;
}

Status LocalFileSystem::create_file(const Path& path, std::unique_ptr<FileWriter>* writer) {
    auto fs_path = absolute_path(path);
    int fd = ::open(fs_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    if (-1 == fd) {
        return Status::IOError(
                fmt::format("cannot open {}: {}", fs_path.native(), std::strerror(errno)));
    }
    *writer = std::make_unique<LocalFileWriter>(std::move(fs_path), fd);
    return Status::OK();
}

Status LocalFileSystem::open_file(const Path& path, std::unique_ptr<FileReader>* reader) {
    auto fs_path = absolute_path(path);
    std::shared_ptr<OpenedFileHandle<int>> file_handle(new OpenedFileHandle<int>());
    bool found = _file_cache->lookup(fs_path.native(), file_handle.get());
    if (!found) {
        int fd = -1;
        RETRY_ON_EINTR(fd, open(fs_path.c_str(), O_RDONLY));
        if (fd < 0) {
            return Status::IOError(
                    fmt::format("cannot open {}: {}", fs_path.native(), std::strerror(errno)));
        }
        int* p_fd = new int(fd);
        _file_cache->insert(fs_path.native(), p_fd, file_handle.get(),
                            [](const CacheKey& key, void* value) {
                                auto fd = reinterpret_cast<int*>(value);
                                ::close(*fd);
                                delete fd;
                            });
    }
    size_t fsize = 0;
    RETURN_IF_ERROR(file_size(fs_path, &fsize));
    *reader = std::make_unique<LocalFileReader>(std::move(fs_path), fsize, std::move(file_handle));
    return Status::OK();
}

Status LocalFileSystem::delete_file(const Path& path) {
    auto fs_path = absolute_path(path);
    if (!std::filesystem::is_regular_file(fs_path)) {
        return Status::IOError(fmt::format("{} is not a file", fs_path.native()));
    }
    std::error_code ec;
    std::filesystem::remove(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("cannot delete {}: {}", fs_path.native(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::create_directory(const Path& path) {
    auto fs_path = absolute_path(_root_path) / path;
    if (std::filesystem::exists(fs_path)) {
        return Status::IOError(fmt::format("{} exists", fs_path.native()));
    }
    std::error_code ec;
    std::filesystem::create_directories(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("cannot create {}: {}", fs_path.native(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory(const Path& path) {
    auto fs_path = absolute_path(path);
    if (!std::filesystem::is_directory(fs_path)) {
        return Status::IOError(fmt::format("{} is not a directory", fs_path.native()));
    }
    std::error_code ec;
    std::filesystem::remove_all(fs_path, ec);
    if (ec) {
        return Status::IOError(
                fmt::format("cannot delete {}: {}", fs_path.native(), std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::link_file(const Path& src, const Path& dest) {
    if (::link(src.c_str(), dest.c_str()) != 0) {
        return Status::IOError(fmt::format("fail to create hard link: {}. from {} to {}",
                                           std::strerror(errno), src.native(), dest.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::exists(const Path& path, bool* res) const {
    auto fs_path = absolute_path(path);
    *res = std::filesystem::exists(fs_path);
    return Status::OK();
}

Status LocalFileSystem::file_size(const Path& path, size_t* file_size) const {
    auto fs_path = absolute_path(path);
    std::error_code ec;
    *file_size = std::filesystem::file_size(fs_path, ec);
    if (ec) {
        return Status::IOError(fmt::format("cannot get file size {}: {}", fs_path.native(),
                                           std::strerror(ec.value())));
    }
    return Status::OK();
}

Status LocalFileSystem::list(const Path& path, std::vector<Path>* files) {
    files->clear();
    auto fs_path = absolute_path(path);
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(fs_path, ec)) {
        files->push_back(entry.path().filename());
    }
    if (ec) {
        return Status::IOError(
                fmt::format("cannot list {}: {}", fs_path.native(), std::strerror(ec.value())));
    }
    return Status::OK();
}

LocalFileSystem* global_local_filesystem() {
    static LocalFileSystem fs("");
    return &fs;
}

} // namespace io
} // namespace doris
