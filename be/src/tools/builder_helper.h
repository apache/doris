#pragma once
#include "common/status.h"
#include "io/fs/hdfs_file_system.h"

namespace doris {

class BuilderHelper {
public:
    static BuilderHelper* init_instance();
    ~BuilderHelper();
    // Return global instance.
    static BuilderHelper* instance() { return _s_instance; }

    void initial_build_env();
    void open(const std::string& meta_file, const std::string& build_dir,
              const std::string& data_path, const std::string& file_type, bool isHDFS);
    void upload_to_HDFS(std::string& read_path, std::string& write_path, void* bufferPtr);
    void build();

private:
    BuilderHelper() {}
    static BuilderHelper* _s_instance;
    std::string read_local_file(const std::string& file);
    bool _isHDFS;
    std::unique_ptr<io::HdfsFileSystem> hdfsFileSystem; // TODO: need adapt for open HDFS
    std::string _meta_file;
    std::string _build_dir;
    std::string _data_path;
    std::string _file_type;
};

} // namespace doris
