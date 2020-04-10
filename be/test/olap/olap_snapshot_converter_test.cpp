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

#include "olap/olap_snapshot_converter.h"

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "boost/filesystem.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "json2pb/json_to_pb.h"
#include "olap/lru_cache.h"
#include "olap/olap_meta.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/txn_manager.h"
#include "util/file_utils.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

static StorageEngine* k_engine = nullptr;

class OlapSnapshotConverterTest : public testing::Test {
public:
    virtual void SetUp() {
        std::vector<StorePath> paths;
        paths.emplace_back("_engine_data_path", -1);
        EngineOptions options;
        options.store_paths = paths;
        options.backend_uid = UniqueId::gen_uid();
        if (k_engine == nullptr) {
            k_engine = new StorageEngine(options);
        }

        auto cache = new_lru_cache(config::file_descriptor_cache_capacity);
        FileHandler::set_fd_cache(cache);
        string test_engine_data_path = "./be/test/olap/test_data/converter_test_data/data";
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        boost::filesystem::remove_all(_engine_data_path);
        FileUtils::create_dir(_engine_data_path);

        _data_dir = new DataDir(_engine_data_path, 1000000000);
        _data_dir->init();
        _meta_path = "./meta";
        string tmp_data_path = _engine_data_path + "/data";
        if (boost::filesystem::exists(tmp_data_path)) {
            boost::filesystem::remove_all(tmp_data_path);
        }
        copy_dir(test_engine_data_path, tmp_data_path);
        _tablet_id = 15007;
        _schema_hash = 368169781;
        _tablet_data_path = tmp_data_path + "/" + std::to_string(0) + "/" +
                            std::to_string(_tablet_id) + "/" + std::to_string(_schema_hash);
        if (boost::filesystem::exists(_meta_path)) {
            boost::filesystem::remove_all(_meta_path);
        }
        ASSERT_TRUE(boost::filesystem::create_directory(_meta_path));
        ASSERT_TRUE(boost::filesystem::exists(_meta_path));
        _meta = new (std::nothrow) OlapMeta(_meta_path);
        ASSERT_NE(nullptr, _meta);
        OLAPStatus st = _meta->init();
        ASSERT_TRUE(st == OLAP_SUCCESS);
    }

    virtual void TearDown() {
        delete _meta;
        delete _data_dir;
        if (boost::filesystem::exists(_meta_path)) {
            ASSERT_TRUE(boost::filesystem::remove_all(_meta_path));
        }
        if (boost::filesystem::exists(_engine_data_path)) {
            ASSERT_TRUE(boost::filesystem::remove_all(_engine_data_path));
        }
    }

private:
    DataDir* _data_dir;
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    std::string _engine_data_path;
    std::string _meta_path;
    int64_t _tablet_id;
    int32_t _schema_hash;
    string _tablet_data_path;
};

TEST_F(OlapSnapshotConverterTest, ToNewAndToOldSnapshot) {
    // --- start to convert old snapshot to new snapshot
    string header_file_path = _tablet_data_path + "/" + "olap_header.json";
    std::ifstream infile(header_file_path);
    string buffer;
    std::string json_header;
    while (getline(infile, buffer)) {
        json_header = json_header + buffer;
    }
    boost::algorithm::trim(json_header);
    OLAPHeaderMessage header_msg;
    bool ret = json2pb::JsonToProtoMessage(json_header, &header_msg);
    ASSERT_TRUE(ret);
    OlapSnapshotConverter converter;
    TabletMetaPB tablet_meta_pb;
    vector<RowsetMetaPB> pending_rowsets;
    OLAPStatus status = converter.to_new_snapshot(header_msg, _tablet_data_path, _tablet_data_path,
                                                  &tablet_meta_pb, &pending_rowsets, true);
    ASSERT_TRUE(status == OLAP_SUCCESS);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(tablet_meta_pb.schema());
    string data_path_prefix = _data_dir->get_absolute_tablet_path(
            tablet_meta_pb.shard_id(), tablet_meta_pb.tablet_id(), tablet_meta_pb.schema_hash());
    // check converted new tabletmeta pb and its files
    // check visible delta
    ASSERT_TRUE(tablet_meta_pb.rs_metas().size() == header_msg.delta().size());
    for (auto& pdelta : header_msg.delta()) {
        int64_t start_version = pdelta.start_version();
        int64_t end_version = pdelta.end_version();
        bool found = false;
        for (auto& visible_rowset : tablet_meta_pb.rs_metas()) {
            if (visible_rowset.start_version() == start_version &&
                visible_rowset.end_version() == end_version) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
    for (auto& visible_rowset : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(visible_rowset);
        AlphaRowset rowset(&tablet_schema, data_path_prefix, alpha_rowset_meta);
        ASSERT_TRUE(rowset.init() == OLAP_SUCCESS);
        ASSERT_TRUE(rowset.load() == OLAP_SUCCESS);
        std::vector<std::string> old_files;
        rowset.remove_old_files(&old_files);
    }
    // check incremental delta
    ASSERT_TRUE(tablet_meta_pb.inc_rs_metas().size() == header_msg.incremental_delta().size());
    for (auto& pdelta : header_msg.incremental_delta()) {
        int64_t start_version = pdelta.start_version();
        int64_t end_version = pdelta.end_version();
        int64_t version_hash = pdelta.version_hash();
        bool found = false;
        for (auto& inc_rowset : tablet_meta_pb.inc_rs_metas()) {
            if (inc_rowset.start_version() == start_version &&
                inc_rowset.end_version() == end_version &&
                inc_rowset.version_hash() == version_hash) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
    for (auto& inc_rowset : tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(inc_rowset);
        AlphaRowset rowset(&tablet_schema, data_path_prefix, alpha_rowset_meta);
        ASSERT_TRUE(rowset.init() == OLAP_SUCCESS);
        ASSERT_TRUE(rowset.load() == OLAP_SUCCESS);
        AlphaRowset tmp_rowset(&tablet_schema, data_path_prefix + "/incremental_delta",
                               alpha_rowset_meta);
        ASSERT_TRUE(tmp_rowset.init() == OLAP_SUCCESS);
        std::vector<std::string> old_files;
        tmp_rowset.remove_old_files(&old_files);
    }
    // check pending delta
    ASSERT_TRUE(pending_rowsets.size() == header_msg.pending_delta().size());
    for (auto& pdelta : header_msg.pending_delta()) {
        int64_t partition_id = pdelta.partition_id();
        int64_t transaction_id = pdelta.transaction_id();
        bool found = false;
        for (auto& pending_rowset : pending_rowsets) {
            if (pending_rowset.partition_id() == partition_id &&
                pending_rowset.txn_id() == transaction_id &&
                pending_rowset.tablet_uid().hi() == tablet_meta_pb.tablet_uid().hi() &&
                pending_rowset.tablet_uid().lo() == tablet_meta_pb.tablet_uid().lo()) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
    for (auto& pending_rowset : pending_rowsets) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(pending_rowset);
        AlphaRowset rowset(&tablet_schema, data_path_prefix, alpha_rowset_meta);
        ASSERT_TRUE(rowset.init() == OLAP_SUCCESS);
        ASSERT_TRUE(rowset.load() == OLAP_SUCCESS);
        std::vector<std::string> old_files;
        rowset.remove_old_files(&old_files);
    }

    // old files are removed, then convert new snapshot to old snapshot
    OLAPHeaderMessage old_header_msg;
    status = converter.to_old_snapshot(tablet_meta_pb, _tablet_data_path, _tablet_data_path,
                                       &old_header_msg);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    for (auto& pdelta : header_msg.delta()) {
        bool found = false;
        for (auto& converted_pdelta : old_header_msg.delta()) {
            if (converted_pdelta.start_version() == pdelta.start_version() &&
                converted_pdelta.end_version() == pdelta.end_version()) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
    for (auto& pdelta : header_msg.incremental_delta()) {
        bool found = false;
        for (auto& converted_pdelta : old_header_msg.incremental_delta()) {
            if (converted_pdelta.start_version() == pdelta.start_version() &&
                converted_pdelta.end_version() == pdelta.end_version() &&
                converted_pdelta.version_hash() == pdelta.version_hash()) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
