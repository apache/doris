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

#include <gtest/gtest.h>

#include <random>

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"
#include "runtime/exec_env.h"

namespace doris {

TEST(PathGcTest, GcTabletAndRowset) {
    const std::string dir_path = "ut_dir/path_gc_test";
    Defer defer {[&] {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        auto st = io::global_local_filesystem()->delete_directory(dir_path);
    }};
    auto&& fs = io::global_local_filesystem();
    auto st = fs->delete_directory(dir_path);
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(dir_path);
    ASSERT_TRUE(st.ok()) << st;

    StorageEngine engine({});
    DataDir data_dir(engine, dir_path);
    st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    // Prepare tablets
    auto create_tablet = [&](int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        (void)tablet_meta->set_partition_id(10000);
        tablet_meta->set_tablet_uid({tablet_id, 0});
        tablet_meta->set_shard_id(tablet_id % 4);
        tablet_meta->_schema_hash = tablet_id;
        auto tablet = std::make_shared<Tablet>(engine, std::move(tablet_meta), &data_dir);
        auto& tablet_map = engine.tablet_manager()->_get_tablet_map(tablet_id);
        tablet_map[tablet_id] = tablet;
        return tablet;
    };
    std::vector<TabletSharedPtr> active_tablets;
    int64_t next_tablet_id = 10000;
    for (int64_t i = 0; i < 10; ++i) {
        int64_t tablet_id = ++next_tablet_id;
        active_tablets.push_back(create_tablet(tablet_id));
    }
    // Prepare tablet directories
    for (auto&& tablet : active_tablets) {
        st = fs->create_directory(tablet->tablet_path());
        ASSERT_TRUE(st.ok()) << st;
    }
    // Prepare garbage tablet directories
    for (int64_t i = 0; i < 10; ++i) {
        int64_t tablet_id = ++next_tablet_id;
        // {dir_path}/data/{shard_id}/{tablet_id}/{schema_hash}
        st = fs->create_directory(
                fmt::format("{}/data/{}/{}/{}", dir_path, tablet_id % 4, tablet_id, tablet_id));
        ASSERT_TRUE(st.ok()) << st;
    }

    // Test tablet gc

    // Prepare rowsets
    auto rng = std::default_random_engine {static_cast<uint32_t>(::time(nullptr))};
    std::uniform_int_distribution<int64_t> u(0, active_tablets.size() - 1);
    auto create_rowset = [&]() {
        auto rowset_meta = std::make_shared<RowsetMeta>();
        auto&& tablet = active_tablets[u(rng)];
        rowset_meta->set_tablet_id(tablet->tablet_id());
        rowset_meta->set_tablet_uid(tablet->tablet_uid());
        rowset_meta->set_rowset_id(engine.next_rowset_id());
        return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                            tablet->tablet_path());
    };
    // tablet_id -> filenames
    std::unordered_map<int64_t, std::vector<std::string>> expected_rowset_files;
    auto create_rowset_files = [&](const BetaRowset& rs, bool is_garbage) {
        auto& filenames = expected_rowset_files[rs.rowset_meta()->tablet_id()];
        std::unique_ptr<io::FileWriter> writer;
        auto filename = fmt::format("{}_{}.dat", rs.rowset_id().to_string(), 0);
        RETURN_IF_ERROR(fs->create_file(rs.tablet_path() + '/' + filename, &writer));
        if (!is_garbage) {
            filenames.push_back(std::move(filename));
        }
        RETURN_IF_ERROR(writer->close());
        filename = fmt::format("{}_{}_{}.idx", rs.rowset_id().to_string(), 0, 987);
        RETURN_IF_ERROR(fs->create_file(rs.tablet_path() + '/' + filename, &writer));
        if (!is_garbage) {
            filenames.push_back(std::move(filename));
        }
        RETURN_IF_ERROR(writer->close());
        filename = fmt::format("{}_{}.dat", rs.rowset_id().to_string(), 1);
        RETURN_IF_ERROR(fs->create_file(rs.tablet_path() + '/' + filename, &writer));
        if (!is_garbage) {
            filenames.push_back(std::move(filename));
        }
        RETURN_IF_ERROR(writer->close());
        filename = fmt::format("{}_{}_{}.idx", rs.rowset_id().to_string(), 1, 987);
        RETURN_IF_ERROR(fs->create_file(rs.tablet_path() + '/' + filename, &writer));
        if (!is_garbage) {
            filenames.push_back(std::move(filename));
        }
        return writer->close();
    };
    // Prepare pending rowsets
    std::vector<PendingRowsetGuard> guards;
    for (int i = 0; i < 20; ++i) {
        auto rs = create_rowset();
        st = create_rowset_files(*rs, false);
        ASSERT_TRUE(st.ok()) << st;
        guards.push_back(engine.pending_local_rowsets().add(rs->rowset_id()));
    }
    // Prepare unused rowsets
    for (int i = 0; i < 30; ++i) {
        auto rs = create_rowset();
        st = create_rowset_files(*rs, false);
        ASSERT_TRUE(st.ok()) << st;
        engine.add_unused_rowset(std::move(rs));
    }
    // Prepare visible rowsets
    for (int i = 0; i < 30; ++i) {
        auto rs = create_rowset();
        st = create_rowset_files(*rs, false);
        ASSERT_TRUE(st.ok()) << st;
        auto tablet = engine.tablet_manager()->get_tablet(rs->rowset_meta()->tablet_id());
        ASSERT_TRUE(tablet) << rs->rowset_meta()->tablet_id();
        auto max_version = tablet->max_version_unlocked();
        rs->rowset_meta()->set_version({max_version + 1, max_version + 1});
        st = tablet->add_inc_rowset(rs);
        ASSERT_TRUE(st.ok()) << st;
    }
    // Prepare rowsets in OlapMeta
    for (int i = 0; i < 20; ++i) {
        auto rs = create_rowset();
        st = create_rowset_files(*rs, false);
        ASSERT_TRUE(st.ok()) << st;
        st = RowsetMetaManager::save(data_dir.get_meta(), rs->rowset_meta()->tablet_uid(),
                                     rs->rowset_id(), rs->rowset_meta()->get_rowset_pb(), false);
        ASSERT_TRUE(st.ok()) << st;
    }
    // Prepare garbage rowset files
    for (int i = 0; i < 20; ++i) {
        auto rs = create_rowset();
        st = create_rowset_files(*rs, true);
        ASSERT_TRUE(st.ok()) << st;
    }

    // Test rowset gc
    data_dir.perform_path_gc();
    for (auto&& t : active_tablets) {
        std::vector<io::FileInfo> files;
        bool exists;
        st = fs->list(t->tablet_path(), true, &files, &exists);
        ASSERT_TRUE(st.ok()) << st;
        auto&& expected_files = expected_rowset_files[t->tablet_id()];
        ASSERT_EQ(files.size(), expected_files.size());
        std::sort(expected_files.begin(), expected_files.end());
        std::sort(files.begin(), files.end(),
                  [](auto&& file1, auto&& file2) { return file1.file_name < file2.file_name; });
        for (size_t i = 0; i < files.size(); ++i) {
            EXPECT_EQ(files[i].file_name, expected_files[i]);
        }
    }
}

} // namespace doris
