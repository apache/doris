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

#include "storage/rowset/rowset_meta_manager.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/algorithm/string/replace.hpp>
#include <map>
#include <set>
#include <filesystem>
#include <fstream>
#include <new>
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "storage/olap_meta.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/storage/test_data/rowset_meta.json";

class RowsetMetaManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        LOG(INFO) << "SetUp";

        std::string meta_path = "./meta";
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
        EXPECT_TRUE(std::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        boost::replace_all(_json_rowset_meta, "\r", "");
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        SAFE_DELETE(_meta);
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
        LOG(INFO) << "TearDown";
    }

protected:
    RowsetMetaSharedPtr create_rowset_meta(int64_t rowset_id, RowsetStatePB state, Version version,
                                           bool is_row_binlog = false) {
        auto rowset_meta = std::make_shared<RowsetMeta>();
        EXPECT_TRUE(rowset_meta->init_from_json(_json_rowset_meta));
        RowsetId rs_id;
        rs_id.init(rowset_id);
        rowset_meta->set_rowset_id(rs_id);
        rowset_meta->set_tablet_uid(_tablet_uid);
        rowset_meta->set_rowset_state(state);
        rowset_meta->set_version(version);
        if (is_row_binlog) {
            rowset_meta->mark_row_binlog();
        }
        return rowset_meta;
    }

    RowsetMetaPB to_rowset_meta_pb(const RowsetMetaSharedPtr& rowset_meta) {
        std::string serialized;
        EXPECT_TRUE(rowset_meta->serialize(&serialized));
        RowsetMetaPB rowset_meta_pb;
        EXPECT_TRUE(rowset_meta_pb.ParseFromString(serialized));
        return rowset_meta_pb;
    }

    OlapMeta* meta() { return _meta; }
    TabletUid tablet_uid() const { return _tablet_uid; }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid {0, 0};
};

TEST_F(RowsetMetaManagerTest, SaveAndLoad) {
    auto base_rowset_meta = create_rowset_meta(20000, RowsetStatePB::COMMITTED, Version {7, 7});
    auto attach_rowset_meta =
            create_rowset_meta(20001, RowsetStatePB::COMMITTED, Version {7, 7}, true);

    std::map<RowsetId, RowsetMetaPB> attach_rowset_map;
    attach_rowset_map.emplace(attach_rowset_meta->rowset_id(), to_rowset_meta_pb(attach_rowset_meta));

    auto st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                      to_rowset_meta_pb(base_rowset_meta), BinlogFormatPB::ROW,
                                      &attach_rowset_map);
    ASSERT_TRUE(st.ok()) << st;

    RowsetMetaSharedPtr loaded_base_meta = std::make_shared<RowsetMeta>();
    st = RowsetMetaManager::get_rowset_meta(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                            loaded_base_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(loaded_base_meta->rowset_id(), base_rowset_meta->rowset_id());
    EXPECT_EQ(loaded_base_meta->tablet_uid().to_string(), tablet_uid().to_string());
    EXPECT_EQ(loaded_base_meta->version(), base_rowset_meta->version());

    std::vector<std::tuple<TabletUid, RowsetId, RowsetId, RowsetMetaSharedPtr>> traversed_attach_metas;
    st = RowsetMetaManager::traverse_row_binlog_metas(
            meta(), [&traversed_attach_metas](const TabletUid& tablet_uid, const RowsetId& base_rowset_id,
                                              const RowsetId& row_binlog_rowset_id,
                                              const std::string& value) {
                auto rowset_meta = std::make_shared<RowsetMeta>();
                EXPECT_TRUE(rowset_meta->init(value));
                traversed_attach_metas.emplace_back(tablet_uid, base_rowset_id, row_binlog_rowset_id,
                                                    std::move(rowset_meta));
                return true;
            });
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(traversed_attach_metas.size(), 1);
    EXPECT_EQ(std::get<0>(traversed_attach_metas[0]).to_string(), tablet_uid().to_string());
    EXPECT_EQ(std::get<1>(traversed_attach_metas[0]), base_rowset_meta->rowset_id());
    EXPECT_EQ(std::get<2>(traversed_attach_metas[0]), attach_rowset_meta->rowset_id());
    EXPECT_EQ(std::get<3>(traversed_attach_metas[0])->rowset_id(), attach_rowset_meta->rowset_id());
    EXPECT_TRUE(std::get<3>(traversed_attach_metas[0])->is_row_binlog());
}

TEST_F(RowsetMetaManagerTest, Remove) {
    auto base_rowset_meta = create_rowset_meta(20010, RowsetStatePB::VISIBLE, Version {9, 9});
    auto attach_rowset_meta =
            create_rowset_meta(20011, RowsetStatePB::VISIBLE, Version {9, 9}, true);

    std::map<RowsetId, RowsetMetaPB> attach_rowset_map;
    attach_rowset_map.emplace(attach_rowset_meta->rowset_id(), to_rowset_meta_pb(attach_rowset_meta));

    auto st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                      to_rowset_meta_pb(base_rowset_meta), BinlogFormatPB::ROW,
                                      &attach_rowset_map);
    ASSERT_TRUE(st.ok()) << st;

    std::map<RowsetId, RowsetId> base_rowset_id_to_row_binlog;
    st = RowsetMetaManager::get_row_binlog_base_rowset_ids(
            meta(), tablet_uid(), base_rowset_id_to_row_binlog,
            std::set<RowsetId> {attach_rowset_meta->rowset_id()});
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(base_rowset_id_to_row_binlog.size(), 1);
    EXPECT_EQ(base_rowset_id_to_row_binlog.begin()->first, base_rowset_meta->rowset_id());
    EXPECT_EQ(base_rowset_id_to_row_binlog.begin()->second, attach_rowset_meta->rowset_id());

    st = RowsetMetaManager::remove_row_binlog(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                              attach_rowset_meta->rowset_id());
    ASSERT_TRUE(st.ok()) << st;

    int traversed_attach_meta_count = 0;
    st = RowsetMetaManager::traverse_row_binlog_metas(
            meta(), [&traversed_attach_meta_count](const TabletUid&, const RowsetId&, const RowsetId&,
                                                   const std::string&) {
                ++traversed_attach_meta_count;
                return true;
            });
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(traversed_attach_meta_count, 0);

    auto base_rowset_meta_2 = create_rowset_meta(20012, RowsetStatePB::VISIBLE, Version {10, 10});
    auto attach_rowset_meta_2 =
            create_rowset_meta(20013, RowsetStatePB::VISIBLE, Version {10, 10}, true);
    std::map<RowsetId, RowsetMetaPB> attach_rowset_map_2;
    attach_rowset_map_2.emplace(attach_rowset_meta_2->rowset_id(),
                                to_rowset_meta_pb(attach_rowset_meta_2));

    st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta_2->rowset_id(),
                                 to_rowset_meta_pb(base_rowset_meta_2), BinlogFormatPB::ROW,
                                 &attach_rowset_map_2);
    ASSERT_TRUE(st.ok()) << st;

    st = RowsetMetaManager::remove_row_binlog_metas(
            meta(), tablet_uid(), std::set<RowsetId> {attach_rowset_meta_2->rowset_id()});
    ASSERT_TRUE(st.ok()) << st;

    traversed_attach_meta_count = 0;
    st = RowsetMetaManager::traverse_row_binlog_metas(
            meta(), [&traversed_attach_meta_count](const TabletUid&, const RowsetId&, const RowsetId&,
                                                   const std::string&) {
                ++traversed_attach_meta_count;
                return true;
            });
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(traversed_attach_meta_count, 0);
}

} // namespace doris
