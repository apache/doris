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

#include "olap/base_compaction.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class TestBaseCompaction : public testing::Test {};

static RowsetSharedPtr create_rowset(Version version, int num_segments, bool overlapping,
                                     int data_size) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET); // important
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_num_segments(num_segments);
    rs_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
    rs_meta->set_total_disk_size(data_size);
    RowsetSharedPtr rowset;
    Status st = RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    if (!st.ok()) {
        return nullptr;
    }
    return rowset;
}

TEST_F(TestBaseCompaction, filter_input_rowset) {
    StorageEngine engine({});
    TabletMetaSharedPtr tablet_meta;
    tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                     UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                     TCompressionType::LZ4F));
    TabletSharedPtr tablet(new Tablet(engine, tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));
    tablet->_cumulative_point = 25;
    BaseCompaction compaction(engine, tablet);
    //std::vector<RowsetSharedPtr> rowsets;

    RowsetSharedPtr init_rs = create_rowset({0, 1}, 1, false, 0);
    tablet->_rs_version_map.emplace(init_rs->version(), init_rs);
    for (int i = 2; i < 30; ++i) {
        RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
        tablet->_rs_version_map.emplace(rs->version(), rs);
    }
    Status st = compaction.pick_rowsets_to_compact();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(compaction._input_rowsets.front()->start_version(), 0);
    EXPECT_EQ(compaction._input_rowsets.front()->end_version(), 1);

    EXPECT_EQ(compaction._input_rowsets.back()->start_version(), 21);
    EXPECT_EQ(compaction._input_rowsets.back()->end_version(), 21);
}

} // namespace doris
