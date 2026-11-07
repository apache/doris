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

#include "service/http/action/check_encryption_action.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

class CheckEncryptionActionTest : public testing::Test {
public:
    CheckEncryptionActionTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false, std::nullopt,
                "size_based", 1024, 1000, 3600, 5, 1, TInvertedIndexFileStorageFormat::V2,
                TEncryptionAlgorithm::AES256);
        ASSERT_EQ(tablet_meta->encryption_algorithm(), EncryptionAlgorithmPB::AES_256_CTR);
        _tablet = std::make_shared<CloudTablet>(_engine, std::move(tablet_meta));
    }

protected:
    void add_rowset(Version version, int64_t num_segments, const std::string& resource_id) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_num_segments(num_segments);
        // A set resource id makes the rowset remote, so its fs comes from the storage resource.
        rs_meta->set_resource_id(resource_id);
        RowsetSharedPtr rowset;
        ASSERT_TRUE(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
        std::unique_lock wlock(_tablet->get_header_lock());
        _tablet->add_rowsets({rowset}, false, wlock, false);
    }

    CloudStorageEngine _engine;
    std::shared_ptr<CloudTablet> _tablet;
};

// A version hole is filled with an empty rowset copied from the previous rowset's meta. In cloud
// mode that may be the initial [0-1] rowset, whose resource id is empty, so no fs can be resolved
// for it. It has no file, so it must be skipped rather than fail the whole check.
TEST_F(CheckEncryptionActionTest, EmptyRowsetWithoutStorageResourceIsSkipped) {
    add_rowset(Version(2, 2), 0, "");

    auto res = is_tablet_encrypted(_tablet);
    ASSERT_TRUE(res.has_value()) << res.error();
    EXPECT_TRUE(res.value());
}

// A rowset whose files cannot be checked must surface the error, not be reported as unencrypted.
TEST_F(CheckEncryptionActionTest, UncheckableRowsetReportsError) {
    add_rowset(Version(2, 2), 1, "not_exist_resource");

    auto res = is_tablet_encrypted(_tablet);
    ASSERT_FALSE(res.has_value()) << "unexpected value: " << res.value();
    EXPECT_NE(res.error().to_string().find("failed to get fs for rowset"), std::string::npos)
            << res.error();
}

} // namespace doris
