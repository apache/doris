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

#include <memory>

#include "beta_rowset.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_writer.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

OLAPStatus RowsetFactory::create_rowset(const TabletSchema* schema, const FilePathDesc& rowset_path_desc,
                                        RowsetMetaSharedPtr rowset_meta, RowsetSharedPtr* rowset) {
    if (rowset_meta->rowset_type() == ALPHA_ROWSET) {
        rowset->reset(new AlphaRowset(schema, rowset_path_desc, rowset_meta));
        return (*rowset)->init();
    }
    if (rowset_meta->rowset_type() == BETA_ROWSET) {
        rowset->reset(new BetaRowset(schema, rowset_path_desc, rowset_meta));
        return (*rowset)->init();
    }
    return OLAP_ERR_ROWSET_TYPE_NOT_FOUND; // should never happen
}

OLAPStatus RowsetFactory::create_rowset_writer(const RowsetWriterContext& context,
                                               std::unique_ptr<RowsetWriter>* output) {
    if (context.rowset_type == ALPHA_ROWSET) {
        output->reset(new AlphaRowsetWriter);
        return (*output)->init(context);
    }
    if (context.rowset_type == BETA_ROWSET) {
        output->reset(new BetaRowsetWriter);
        return (*output)->init(context);
    }
    return OLAP_ERR_ROWSET_TYPE_NOT_FOUND;
}

} // namespace doris
