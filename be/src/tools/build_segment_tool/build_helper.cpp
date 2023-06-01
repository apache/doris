#include "tools/build_segment_tool/build_helper.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "common/config.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema_cache.h"
#include "olap/file_header.h"
#include "runtime/exec_env.h"
#include "tools/build_segment_tool/builder_scanner_memtable.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

namespace doris {

BuildHelper* BuildHelper::_s_instance = nullptr;

BuildHelper* BuildHelper::init_instance() {
    // DCHECK(_s_instance == nullptr);
    static BuildHelper instance;
    _s_instance = &instance;
    return _s_instance;
}

void BuildHelper::initial_build_env() {
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
    config::write_buffer_size = 209715200;
    // max buffer size used in memtable for the aggregated table
    config::write_buffer_size_for_agg = 8194304000;
    // CONF_mInt64(memtable_max_buffer_size, "8194304000");

    // std::shared_ptr<doris::MemTrackerLimiter> process_mem_tracker =
    //         std::make_shared<doris::MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL, "Process");
    // doris::ExecEnv::GetInstance()->set_orphan_mem_tracker(process_mem_tracker);
    // doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(process_mem_tracker,
    //                                                                         TUniqueId());

    // doris::thread_context()->thread_mem_tracker_mgr->init();
    // doris::thread_context()->thread_mem_tracker_mgr->set_check_limit(false);
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::ChunkAllocator::init_instance(4096);

}

void BuildHelper::open(const std::string& meta_file, const std::string& build_dir,
                         const std::string& data_path, const std::string& file_type) {
    _meta_file = meta_file;
    _build_dir = build_dir;
    if (data_path.at(data_path.size() - 1) != '/') {
        _data_path = data_path + "/";
    } else {
        _data_path = data_path;
    }

    _file_type = file_type;

    std::filesystem::path dir_path(std::filesystem::absolute(std::filesystem::path(build_dir)));
    if (!std::filesystem::is_directory(std::filesystem::status(dir_path))) {
        LOG(FATAL) << "build dir should be a directory";
    }

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

std::string BuildHelper::read_local_file(const std::string& file) {
    std::filesystem::path path(std::filesystem::absolute(std::filesystem::path(file)));
    if (!std::filesystem::exists(path)) { LOG(FATAL) << "file not exist:" << file;
    }

    std::ifstream f(path, std::ios::in | std::ios::binary);
    const auto sz = std::filesystem::file_size(path);
    std::string result(sz, '\0');
    f.read(result.data(), sz);

    return result;
}

Status BuildHelper::build() {
    // load meta file
    io::FileReaderSPtr file_reader;
    TabletMeta* tablet_meta = new TabletMeta();
    Status status = tablet_meta->create_from_file(_meta_file);
    if (!status.ok()) {
        std::cout << "load pb meta file:" << _meta_file << " failed"
                  << ", status:" << status << std::endl;
        return status;
    }

    LOG(INFO) << "table id:" << tablet_meta->table_id() << " tablet id:" << tablet_meta->tablet_id()
              << " shard id:" << tablet_meta->shard_id();

    // for debug
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    json_options.bytes_to_base64 = false;
    tablet_meta->to_json(&json_meta, json_options);
    LOG(INFO) << "\n" << json_meta;
    //
    auto data_dir = StorageEngine::instance()->get_store(_build_dir);
    TabletMetaSharedPtr tablet_meta_ptr(tablet_meta);
    TabletSharedPtr new_tablet = doris::Tablet::create_tablet_from_meta(tablet_meta_ptr, data_dir);
    status = StorageEngine::instance()->tablet_manager()->add_tablet_unlocked(
            tablet_meta->tablet_id(), new_tablet, false, true);

    if (!status.ok()) {
        LOG(FATAL) << "fail to add tablet to storage :" << status.to_string();
        return status;
    }

    std::vector<std::filesystem::directory_entry> files;
        for (const auto& file : std::filesystem::directory_iterator(_data_path)) {
            auto file_path = file.path().string();
            if (file_path.substr(file_path.size() - _file_type.size()) == _file_type) {
                LOG(INFO) << "get file:" << file_path;
                files.emplace_back(file);
            }
    }
    

    BuilderScannerMemtable scanner(new_tablet, _build_dir, _file_type);
    scanner.doSegmentBuild(files);

    std::string local_new_header =
            _build_dir + "/" + std::to_string(new_tablet->tablet_id()) + ".hdr";
    TabletMetaSharedPtr new_tablet_meta = std::make_shared<TabletMeta>();
    new_tablet->generate_tablet_meta_copy(new_tablet_meta);
    auto st = new_tablet_meta->save(local_new_header);
    // post check
    std::filesystem::path header = local_new_header;
    if (!st.ok())
    {
        LOG(WARNING) << " save meta file error..., need retry...";
        std::filesystem::remove(header);
        int reTry = 3;
        while(--reTry >= 0 && !st.ok()) {
            st = new_tablet_meta->save(local_new_header);
        }

        if (reTry < 0) {
            LOG(WARNING) << " save meta fail...";
            std::exit(1);
        }
    }
    size_t  header_size = std::filesystem::file_size(header);
    if (header_size <= 0) {
        LOG(WARNING) << " header size abnormal ..." << local_new_header;
        std::exit(1);
    }
    LOG(INFO) << " got header size:" << header_size;
    return Status::OK();
}


BuildHelper::~BuildHelper() {
    doris::ExecEnv::destroy(ExecEnv::GetInstance());
}

} // namespace doris
