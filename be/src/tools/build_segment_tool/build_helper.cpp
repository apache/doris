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

#include "tools/build_segment_tool/build_helper.h"

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "olap/file_header.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema_cache.h"
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
    //         std::make_shared<doris::MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL,
    //         "Process");
    // doris::ExecEnv::GetInstance()->set_orphan_mem_tracker(process_mem_tracker);
    // doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(process_mem_tracker,
    //                                                                         TUniqueId());

    // doris::thread_context()->thread_mem_tracker_mgr->init();
    // doris::thread_context()->thread_mem_tracker_mgr->set_check_limit(false);
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::ChunkAllocator::init_instance(4096);
}

void BuildHelper::open(const std::string& meta_file, const std::string& build_dir,
                       const std::string& data_path, const std::string& file_type, 
                       const bool& enable_post_compaction) {
    _meta_file = meta_file;
    _build_dir = build_dir;
    if (data_path.at(data_path.size() - 1) != '/') {
        _data_path = data_path + "/";
    } else {
        _data_path = data_path;
    }

    _file_type = file_type;
    _enable_post_compaction = enable_post_compaction;

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
    auto exec_env = doris::ExecEnv::GetInstance();
    doris::ExecEnv::init(exec_env, paths);
    // doris::TabletSchemaCache::create_global_schema_cache();
    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    doris::StorageEngine* engine = nullptr;
    auto st = doris::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st;
        exit(-1);
    }
    exec_env->set_storage_engine(engine);
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
    // std::string json_meta;
    // json2pb::Pb2JsonOptions json_options;
    // json_options.pretty_json = true;
    // json_options.bytes_to_base64 = false;
    // tablet_meta->to_json(&json_meta, json_options);
    // LOG(INFO) << "\n" << json_meta;
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
    RowsetSharedPtr rowset = scanner.doSegmentBuild(files);
    if (rowset == nullptr) {
         LOG(FATAL) << "invaild return rowset, build failed";
    }
    // do compaction
    do {
        if (!_enable_post_compaction) {
            LOG(INFO) << "post compaction is disabled, skip";
            break;
        }

        if (!rowset->is_segments_overlapping()) {
            LOG(INFO) << "segment is not overlapping, skip compaction";
            break;
        }

        auto t0 = std::chrono::steady_clock::now();
        std::vector<RowsetReaderSharedPtr> _input_rs_readers;
        std::unique_ptr<RowsetWriter> _output_rs_writer;
        Merger::Statistics stats;

        // create input reader
        RowsetReaderSharedPtr rs_reader;
        rowset->create_reader(&rs_reader);
        _input_rs_readers.push_back(std::move(rs_reader));
        Version _output_version = Version(rowset->start_version(), rowset->end_version());
        RowsetWriterContext ctx;
        ctx.version = _output_version;
        ctx.rowset_state = VISIBLE;
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.tablet_schema = new_tablet->tablet_schema();
        ctx.newest_write_timestamp = rowset->newest_write_timestamp();
        
        // create output writer
        new_tablet->create_rowset_writer(ctx, &_output_rs_writer);
        std::filesystem::path segment_path(
            std::filesystem::path(_build_dir + "/output/" +
            std::to_string(new_tablet->tablet_id()) + "/" + std::to_string(new_tablet->schema_hash())));
        _output_rs_writer->set_writer_path(segment_path.string());
        Status res = Merger::vmerge_rowsets(new_tablet, ReaderType::READER_CUMULATIVE_COMPACTION, new_tablet->tablet_schema(),
                                         _input_rs_readers, _output_rs_writer.get(), &stats);
        if (!res.ok()) {
             LOG(WARNING) << "fail to do compaction. res=" << res
                     << ", tablet=" << new_tablet->full_name()
                     << ", output_version=" << _output_version;
            break;
        }
        RowsetSharedPtr _output_rowset = _output_rs_writer->build();
        // remove origin rowset
        rowset->remove();
        rowset = _output_rowset;
        auto t1 = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> d {t1 - t0};
        LOG(INFO) << "finish compaction:"
                     << "tablet=" << new_tablet->full_name() 
                     << ", output_version=" << _output_version
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows()
                     << ", spend time=" << d.count() << "ms";


    } while(false);

    // reset new tablet rs_metas
    std::vector<RowsetMetaSharedPtr> metas {rowset->rowset_meta()};
        new_tablet->tablet_meta()->revise_rs_metas(std::move(metas));
    
    std::string new_header_path = _build_dir + "/output/" + 
                                  std::to_string(new_tablet->tablet_id()) + "/" +
                                  std::to_string(new_tablet->schema_hash()) + "/" +
                                  std::to_string(new_tablet->tablet_id()) + ".hdr";
    TabletMetaSharedPtr new_tablet_meta = std::make_shared<TabletMeta>();
    new_tablet->generate_tablet_meta_copy(new_tablet_meta);
    auto st = new_tablet_meta->save(new_header_path);
    // post check
    if (!st.ok()) {
        LOG(WARNING) << "save meta file error..., need retry...";
        std::filesystem::remove(new_header_path);
        int reTry = 3;
        while (--reTry >= 0 && !st.ok()) {
            st = new_tablet_meta->save(new_header_path);
        }

        if (reTry < 0) {
            LOG(WARNING) << "save meta fail...";
            std::exit(1);
        }
    }
    size_t header_size = std::filesystem::file_size(new_header_path);
    if (header_size <= 0) {
        LOG(WARNING) << "header size abnormal ..." << new_header_path;
        std::exit(1);
    }
    LOG(INFO) << "got header size: " << header_size;
    return Status::OK();
}

BuildHelper::~BuildHelper() {
    // doris::TabletSchemaCache::stop_and_join();
    doris::ExecEnv::destroy(ExecEnv::GetInstance());
}

} // namespace doris
