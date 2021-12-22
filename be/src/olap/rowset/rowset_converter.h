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

#pragma once

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_writer.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_meta.h"

namespace doris {

class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
class BetaRowsetReader;
class RowsetFactory;

class RowsetConverter {
public:
    ~RowsetConverter() = default;

    RowsetConverter(const TabletMetaSharedPtr& tablet_meta) : _tablet_meta(tablet_meta) {}

    OLAPStatus convert_beta_to_alpha(const RowsetMetaSharedPtr& src_rowset_meta,
                                     const FilePathDesc& rowset_path_desc, RowsetMetaPB* dst_rs_meta_pb);

    OLAPStatus convert_alpha_to_beta(const RowsetMetaSharedPtr& src_rowset_meta,
                                     const FilePathDesc& rowset_path_desc, RowsetMetaPB* dst_rs_meta_pb);

private:
    OLAPStatus _convert_rowset(const RowsetMetaSharedPtr& src_rowset_meta,
                               const FilePathDesc& rowset_path_desc,
                               RowsetTypePB dst_type, RowsetMetaPB* dst_rs_meta_pb);

private:
    TabletMetaSharedPtr _tablet_meta;
};

} // namespace doris
