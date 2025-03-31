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

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <roaring/roaring.hh>
#include <sstream>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include "common/signal_handler.h"
#include "gutil/strings/strip.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/ann_index_writer.h"
#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/x_index_file_writer.h"
#include "olap/tablet_schema.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "vector/diskann_vector_index.h"
#include "vector/vector_index.h"

using doris::segment_v2::DorisCompoundReader;
using doris::segment_v2::DorisFSDirectoryFactory;
using doris::segment_v2::XIndexFileWriter;
using doris::segment_v2::InvertedIndexDescriptor;
using doris::segment_v2::InvertedIndexFileReader;
using doris::io::FileInfo;
using doris::TabletIndex;
using namespace doris::segment_v2;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;
using doris::io::FileSystem;

#include "io/fs/path.h"

const std::string& file_dir = "/home/users/clz/run/test_diskann/123456_0.idx";
std::filesystem::path index_data_path(file_dir);

int index_id = 100;
std::string rowset_id = "rowset_id";
int seg_id = 0;

std::shared_ptr<FileSystem> get_local_file_filesystem() {
    return doris::io::global_local_filesystem();
}

void test_add() {
    auto fs = get_local_file_filesystem();
    int index_id = 100;

    doris::io::FileWriterPtr file_writer;

    auto st = doris::io::global_local_filesystem()->create_file(file_dir, &file_writer);
    if (!st.ok()) {
        std::cerr << "failed create_file" << file_dir << std::endl;
    }
    std::unique_ptr<XIndexFileWriter> index_file_writer = std::make_unique<XIndexFileWriter>(
            fs, index_data_path.parent_path(), rowset_id, seg_id,
            doris::InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    doris::TabletIndexPB index_pb;

    index_pb.set_index_id(index_id);
    TabletIndex index_meta;
    index_meta.init_from_pb(index_pb);
    index_meta._index_type = doris::IndexType::ANN;
    index_meta._properties["index_type"] = "diskann";
    index_meta._properties["metric_type"] = "l2";
    index_meta._properties["dim"] = "7";
    index_meta._properties["max_degree"] = "32";
    index_meta._properties["search_list"] = "100";

    std::string field_name = "word_embeding";
    std::unique_ptr<AnnIndexColumnWriter> ann_writer = std::make_unique<AnnIndexColumnWriter>(
            field_name, index_file_writer.get(), &index_meta, true);
    st = ann_writer->init();
    if (!st.ok()) {
        std::cout << "failed to ann_writer->init()" << std::endl;
        return;
    }

    //writer
    int field_size = 4;
    float value[14] = {1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1};
    int64_t offsets[3] = {0, 7, 14};
    st = ann_writer->add_array_values(field_size, (const void*)value, nullptr,
                                      (const uint8_t*)offsets, 2);
    if (!st.ok()) {
        std::cout << "failed to ann_writer->add_array_values" << std::endl;
        return;
    }

    //finish
    st = ann_writer->finish();
    if (!st.ok()) {
        std::cout << "failed to ann_writer->finish" << std::endl;
        return;
    }

    //save to disk
    st = index_file_writer->close();
    if (!st.ok()) {
        std::cout << "failed to indexwriter->close" << std::endl;
        return;
    }
}

void init_env() {
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();

    string custom_conffile = "/home/users/clz/run/be_1.conf";
    if (!doris::config::init(custom_conffile.c_str(), true, false, true)) {
        fprintf(stderr, "error read custom config file. \n");
        return;
    }

    std::vector<doris::StorePath> paths;
    std::string storage = "/home/users/clz/run/storage";
    std::string spill = "/home/users/clz/run/splill";
    std::string broken_storage_path = "/home/users/clz/run/broken";

    auto olap_res = doris::parse_conf_store_paths(storage, &paths);
    if (!olap_res) {
        LOG(ERROR) << "parse config storage path failed, path=" << storage;
        exit(-1);
    }

    std::vector<doris::StorePath> spill_paths;
    olap_res = doris::parse_conf_store_paths(spill, &spill_paths);
    if (!olap_res) {
        LOG(ERROR) << "parse config spill storage path failed, path=" << spill;
        exit(-1);
    }
    std::set<std::string> broken_paths;
    doris::parse_conf_broken_store_paths(broken_storage_path, &broken_paths);

    // auto it = paths.begin();
    // for (; it != paths.end();) {
    //     if (broken_paths.count(it->path) > 0) {
    //         if (doris::config::ignore_broken_disk) {
    //             LOG(WARNING) << "ignore broken disk, path = " << it->path;
    //             it = paths.erase(it);
    //         } else {
    //             LOG(ERROR) << "a broken disk is found " << it->path;
    //             exit(-1);
    //         }
    //     } else if (!doris::check_datapath_rw(it->path)) {
    //         if (doris::config::ignore_broken_disk) {
    //             LOG(WARNING) << "read write test file failed, path=" << it->path;
    //             it = paths.erase(it);
    //         } else {
    //             LOG(ERROR) << "read write test file failed, path=" << it->path;
    //             // if only one disk and the disk is full, also need exit because rocksdb will open failed
    //             exit(-1);
    //         }
    //     } else {
    //         ++it;
    //     }
    // }

    // if (paths.empty()) {
    //     LOG(ERROR) << "All disks are broken, exit.";
    //     exit(-1);
    // }

    // it = spill_paths.begin();
    // for (; it != spill_paths.end();) {
    //     if (!doris::check_datapath_rw(it->path)) {
    //         if (doris::config::ignore_broken_disk) {
    //             LOG(WARNING) << "read write test file failed, path=" << it->path;
    //             it = spill_paths.erase(it);
    //         } else {
    //             LOG(ERROR) << "read write test file failed, path=" << it->path;
    //             exit(-1);
    //         }
    //     } else {
    //         ++it;
    //     }
    // }
    // if (spill_paths.empty()) {
    //     LOG(ERROR) << "All spill disks are broken, exit.";
    //     exit(-1);
    // }

    // // initialize libcurl here to avoid concurrent initialization
    // auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    // if (curl_ret != 0) {
    //     LOG(ERROR) << "fail to initialize libcurl, curl_ret=" << curl_ret;
    //     exit(-1);
    // }
    // // add logger for thrift internal
    // apache::thrift::GlobalOutput.setOutputFunction(doris::thrift_output);

    // Status status = Status::OK();
    // if (doris::config::enable_java_support) {
    //     // Init jni
    //     status = doris::JniUtil::Init();
    //     if (!status.ok()) {
    //         LOG(WARNING) << "Failed to initialize JNI: " << status;
    //         exit(1);
    //     } else {
    //         LOG(INFO) << "Doris backend JNI is initialized.";
    //     }
    // }

    // // Doris own signal handler must be register after jvm is init.
    // // Or our own sig-handler for SIGINT & SIGTERM will not be chained ...
    // // https://www.oracle.com/java/technologies/javase/signals.html
    // doris::init_signals();
    // // ATTN: MUST init before `ExecEnv`, `StorageEngine` and other daemon services
    // //
    // //       Daemon ───┬──► StorageEngine ──► ExecEnv ──► Disk/Mem/CpuInfo
    // //                 │
    // //                 │
    // // BackendService ─┘
    // doris::CpuInfo::init();
    // doris::DiskInfo::init();
    // doris::MemInfo::init();

    // LOG(INFO) << doris::CpuInfo::debug_string();
    // LOG(INFO) << doris::DiskInfo::debug_string();
    // LOG(INFO) << doris::MemInfo::debug_string();

    // // PHDR speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    // // will work only after additional call of this function.
    // // rewrites dl_iterate_phdr will cause Jemalloc to fail to run after enable profile. see #
    // // updatePHDRCache();
    // if (!doris::BackendOptions::init()) {
    //     exit(-1);
    // }

    // doris::ThreadLocalHandle::create_thread_local_if_not_exits();

    // init exec env
    //auto* exec_env(doris::ExecEnv::GetInstance());
    doris::Status status =
            doris::ExecEnv::init(doris::ExecEnv::GetInstance(), paths, spill_paths, broken_paths);
    if (!status.ok()) {
        std::cout << "init fail" << std::endl;
    }
}

void test_search() {
    auto fs = get_local_file_filesystem();
    auto index_file_reader = std::make_unique<InvertedIndexFileReader>(
            fs, "/home/users/clz/run/test_diskann/123456_0",
            doris::InvertedIndexStorageFormatPB::V2);
    auto st = index_file_reader->init(4096);
    if (!st.ok()) {
        std::cout << "failed to index_file_reader->init" << st << std::endl;
        return;
    }
    doris::TabletIndexPB index_pb;
    index_pb.set_index_id(index_id);
    TabletIndex index_meta;
    index_meta.init_from_pb(index_pb);

    auto ret = index_file_reader->open(&index_meta);
    if (!ret.has_value()) {
        std::cerr << "InvertedIndexFileReader open error:" << ret.error() << std::endl;
        return;
    }
    using T = std::decay_t<decltype(ret)>;
    std::shared_ptr<DorisCompoundReader> dir = std::forward<T>(ret).value();

    std::shared_ptr<DiskannVectorIndex> vindex = std::make_shared<DiskannVectorIndex>(dir);
    st = vindex->load(VectorIndex::Metric::L2);
    if (!st.ok()) {
        std::cout << "failed to vindex->load" << std::endl;
        return;
    }
    float query_vec[7] = {1, 2, 3, 4, 5, 6, 7};
    SearchResult result;
    std::shared_ptr<DiskannSearchParameter> searchParams =
            std::make_shared<DiskannSearchParameter>();
    searchParams->with_search_list(100);
    searchParams->with_beam_width(2);

    //设置过滤条件
    std::shared_ptr<IDFilter> filter = nullptr;
    std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
    // bitmap->add(1);
    // filter.reset(new IDFilter(bitmap));
    // searchParams->set_filter(filter);
    st = vindex->search(query_vec, 5, &result, searchParams.get());
    if (!st.ok()) {
        std::cout << "failed to vindex->search" << std::endl;
        return;
    }
    if (result.has_rows()) {
        for (int i = 0; i < result.row_count(); i++) {
            std::cout << "idx:" << result.get_id(i) << ", distance:" << result.get_distance(i)
                      << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <test_add|test_search>" << std::endl;
        return 1;
    }
    doris::signal::InstallFailureSignalHandler();
    init_env();
    std::string command = argv[1];
    if (command == "add") {
        test_add();
    } else if (command == "search") {
        test_search();
    } else {
        std::cout << "unkonw command" << std::endl;
    }
    return 0;
}
