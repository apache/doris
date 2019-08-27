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

#ifndef DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
#define DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/data_dir.h"

namespace doris {

class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;

class BetaRowset : public Rowset {
public:
    BetaRowset(const TabletSchema* schema,
               std::string rowset_path,
               DataDir* data_dir,
               RowsetMetaSharedPtr rowset_meta);

    virtual ~BetaRowset() {}

    static std::string segment_file_path(const std::string& segment_dir, RowsetId rowset_id, int segment_id);

    OLAPStatus init() override;

    OLAPStatus load(bool use_cache = true) override;

    std::shared_ptr<RowsetReader> create_reader() override;

    OLAPStatus remove() override;

    OLAPStatus link_files_to(const std::string& dir, RowsetId new_rowset_id) override;

    OLAPStatus copy_files_to(const std::string& dir) override;

    // only applicable to alpha rowset, no op here
    OLAPStatus remove_old_files(std::vector<std::string>* files_to_remove) override {
        return OLAP_SUCCESS;
    };

    bool check_path(const std::string& path) override;

private:
    // TODO segment readers member
};

} // namespace doris

#endif //DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
