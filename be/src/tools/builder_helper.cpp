#include "tools/builder_helper.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "env/env.h"
#include "exec/parquet_scanner.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "io/buffered_reader.h"
#include "io/file_factory.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/file_helper.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/row.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"
#include "tools/builder_scanner.h"
#include "tools/builder_scanner_memtable.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/disk_info.h"
#include "util/file_utils.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/vbroker_scan_node.h"

namespace doris {
#define BUFFER_SIZE 1048576

BuilderHelper* BuilderHelper::_s_instance = nullptr;

BuilderHelper* BuilderHelper::init_instance() {
    // DCHECK(_s_instance == nullptr);
    static BuilderHelper instance;
    _s_instance = &instance;
    return _s_instance;
}

void BuilderHelper::initial_build_env() {
    char doris_home[] = "DORIS_HOME=/tmp";
    putenv(doris_home);

    if (!doris::config::init(nullptr, true, false, true)) {
        LOG(FATAL) << "init config fail";
        exit(-1);
    }
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();

    // write buffer size before flush
    config::write_buffer_size = 14097152000;
    // max buffer size used in memtable for the aggregated table
    config::memtable_max_buffer_size = 18194304000;
    doris::thread_context()->thread_mem_tracker_mgr->set_check_limit(false);
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::ChunkAllocator::init_instance(4096);

    doris::MemInfo::init();
}

void BuilderHelper::open(const std::string& meta_file, const std::string& build_dir,
                         const std::string& data_path, const std::string& file_type, bool isHDFS) {
    _meta_file = meta_file;
    _build_dir = build_dir;
    if (data_path.at(data_path.size() - 1) != '/')
        _data_path = data_path + "/";
    else
        _data_path = data_path;

    _file_type = file_type;
    _isHDFS = isHDFS;

    // TODO: need adapt for open HDFS
    if (_isHDFS) {
        THdfsParams hdfs_params;
        hdfsFileSystem = std::make_unique<io::HdfsFileSystem>(hdfs_params, _data_path);
    }

    std::filesystem::path dir_path(std::filesystem::absolute(std::filesystem::path(build_dir)));
    if (!std::filesystem::is_directory(std::filesystem::status(dir_path)))
        LOG(FATAL) << "build dir should be a directory";

    // init and open storage engine
    std::vector<doris::StorePath> paths;
    auto olap_res = doris::parse_conf_store_paths(_build_dir, &paths);
    if (!olap_res) {
        LOG(FATAL) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }
    doris::ExecEnv::init(doris::ExecEnv::GetInstance(), paths);

    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    doris::StorageEngine* engine = nullptr;
    auto st = doris::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st;
        exit(-1);
    }
}

std::string BuilderHelper::read_local_file(const std::string& file) {
    std::filesystem::path path(std::filesystem::absolute(std::filesystem::path(file)));
    if (!std::filesystem::exists(path)) LOG(FATAL) << "file not exist:" << file;

    std::ifstream f(path, std::ios::in | std::ios::binary);
    const auto sz = std::filesystem::file_size(path);
    std::string result(sz, '\0');
    f.read(result.data(), sz);

    return result;
}

void BuilderHelper::build() {
    // load tablet
    std::string buf;
    if (_isHDFS) {
        FileSystemProperties system_properties;
        LOG(INFO) << "read meta from HDFS:" << _meta_file;
        auto* dfs = hdfsFileSystem.get();
        bool exist = false;
        auto st = dfs->exists(_meta_file, &exist);
        if (!st.ok() || !exist) {
            LOG(FATAL) << "file not exist:" /*<< dfs->GetLastError() */ << _meta_file;
        }

        size_t size = 0;
        st = dfs->file_size(_meta_file, &size);
        if (!st.ok() || size <= 0)
            LOG(FATAL) << "meta file size abnormal.."; //<< dfs->GetLastError();
        IOContext io_ctx;
        io::FileReaderSPtr read_ptr = nullptr;
        st = dfs->open_file(_meta_file, &read_ptr, &io_ctx);
        if (!st.ok() || !read_ptr)
            LOG(FATAL) << "cannot open file:" /*<< dfs->GetLastError()*/ << _meta_file;

        buf.resize(size);

        size_t read_size = 0;
        read_ptr->read_at(0, Slice(buf.data(), size), io_ctx, &read_size);
        if (read_size < size) {
            LOG(FATAL) << "read meta file size abnormal.."; //<< dfs->GetLastError();
        }

        read_ptr->close();
    } else {
        buf = read_local_file(_meta_file);
    }

    FileHeader<TabletMetaPB> file_header;
    buf = buf.substr(file_header.size());
    // init tablet
    TabletMeta* tablet_meta = new TabletMeta();
    Status status = tablet_meta->deserialize(buf);
    if (!status.ok()) {
        LOG(FATAL) << "fail to load tablet meta :" << status.to_string();
        return;
    }

    LOG(INFO) << "table id:" << tablet_meta->table_id() << " tablet id:" << tablet_meta->tablet_id()
              << " shard id:" << tablet_meta->shard_id();

    auto data_dir = StorageEngine::instance()->get_store(_build_dir);
    TabletMetaSharedPtr tablet_meta_ptr(tablet_meta);
    TabletSharedPtr new_tablet = doris::Tablet::create_tablet_from_meta(tablet_meta_ptr, data_dir);
    status = StorageEngine::instance()->tablet_manager()->_add_tablet_unlocked(
            tablet_meta->tablet_id(), new_tablet, false, true);

    if (!status.ok()) {
        LOG(FATAL) << "fail to add tablet to storage :" << status.to_string();
        return;
    }

    std::vector<std::string> files;
    if (_isHDFS) {
        auto* dfs = hdfsFileSystem.get();
        std::vector<io::Path> names;
        dfs->list(_data_path, &names);
        for (const auto& name : names) {
            std::string filename = name.filename().c_str();
            if (filename.substr(filename.size() - _file_type.size()) == _file_type) {
                LOG(INFO) << "get file:" << name;
                files.emplace_back(filename);
            }
        }
    } else {
        for (const auto& file : std::filesystem::directory_iterator(_data_path)) {
            auto file_path = file.path().string();
            if (file_path.substr(file_path.size() - _file_type.size()) == _file_type) {
                LOG(INFO) << "get file:" << file_path;
                files.emplace_back(file_path);
            }
        }
    }

    BuilderScannerMemtable scanner(new_tablet, _build_dir, _file_type, _isHDFS);
    scanner.doSegmentBuild(files);

    std::string local_new_header =
            _build_dir + "/" + std::to_string(new_tablet->table_id()) + ".hdr";
    TabletMetaSharedPtr new_tablet_meta = std::make_shared<TabletMeta>();
    new_tablet->generate_tablet_meta_copy(new_tablet_meta);
    new_tablet_meta->save(local_new_header);

    if (_isHDFS) {
        //upload segment to meta file directory
        auto pos = _meta_file.rfind("/");
        std::string remote_path = _meta_file.substr(0, pos + 1);
        LOG(INFO) << "Upload to remote fs:" << remote_path;
        auto* dfs = hdfsFileSystem.get();
        bool exist = false;
        auto st = dfs->exists(remote_path, &exist);
        if (!st.ok() || !exist) {
            st = dfs->create_directory(remote_path);
            if (!st.ok()) LOG(INFO) << "fail to create directory:" << remote_path;
        }
        std::string local_segment_path = _build_dir + "/segment";
        std::string data_suffix = ".dat";
        std::vector<std::string> segments;
        std::vector<std::string> remote_segments;

        for (const auto& file : std::filesystem::directory_iterator(local_segment_path)) {
            if (!file.is_regular_file()) continue;
            std::string file_path = file.path().string();
            if (file_path.substr(file_path.size() - data_suffix.size()) == data_suffix) {
                LOG(INFO) << "get segment file:" << file_path;
                segments.emplace_back(file_path);
                auto pos = file_path.rfind("/");
                std::string name = file_path.substr(pos + 1);
                remote_segments.emplace_back(remote_path + name);
            }
        }

        LOG(INFO) << "get total segments:" << segments.size();
        std::unique_ptr<char[]> buffer(new char[BUFFER_SIZE]);
        for (size_t i = 0; i < segments.size(); ++i) {
            upload_to_HDFS(segments[i], remote_segments[i], buffer.get());
        }

        LOG(INFO) << "update header:" << _meta_file;
        upload_to_HDFS(local_new_header, _meta_file, buffer.get());
    }
}

// TODO: need adapt the open libhdfs to upload segment & meta file
void BuilderHelper::upload_to_HDFS(std::string& read_path, std::string& write_path,
                                   void* bufferPtr) {
    LOG(INFO) << "going to upload file:" << write_path;
}

BuilderHelper::~BuilderHelper() {
    doris::ExecEnv::destroy(ExecEnv::GetInstance());
}

} // namespace doris
