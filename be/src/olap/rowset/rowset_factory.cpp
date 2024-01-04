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

#include "olap/rowset/rowset_factory.h"

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "beta_rowset.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/vertical_beta_rowset_writer.h"
#include "olap/storage_engine.h"

namespace doris {
using namespace ErrorCode;

Status RowsetFactory::create_rowset(const TabletSchemaSPtr& schema, const std::string& tablet_path,
                                    const RowsetMetaSharedPtr& rowset_meta,
                                    RowsetSharedPtr* rowset) {
    if (rowset_meta->rowset_type() == ALPHA_ROWSET) {
        return Status::Error<ROWSET_INVALID>("invalid rowset_type");
    }
    if (rowset_meta->rowset_type() == BETA_ROWSET) {
        rowset->reset(new BetaRowset(schema, tablet_path, rowset_meta));
        return (*rowset)->init();
    }
    return Status::Error<ROWSET_TYPE_NOT_FOUND>("invalid rowset_type"); // should never happen
}

Status RowsetFactory::create_rowset_writer(const RowsetWriterContext& context, bool is_vertical,
                                           std::unique_ptr<RowsetWriter>* output) {
    if (context.rowset_type == ALPHA_ROWSET) {
        return Status::Error<ROWSET_INVALID>("invalid rowset_type");
    }
    if (context.rowset_type == BETA_ROWSET) {
        if (is_vertical) {
            output->reset(new VerticalBetaRowsetWriter(*StorageEngine::instance()));
            return (*output)->init(context);
        }
        output->reset(new BetaRowsetWriter(*StorageEngine::instance()));
        return (*output)->init(context);
    }
    return Status::Error<ROWSET_TYPE_NOT_FOUND>("invalid rowset_type");
}

} // namespace doris
