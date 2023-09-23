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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_FACTORY_H
#define DORIS_BE_SRC_OLAP_ROWSET_FACTORY_H

#include <memory>
#include <string>

#include "common/status.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"

namespace doris {

class RowsetWriter;
struct RowsetWriterContext;

class RowsetFactory {
public:
    // return OK and set inited rowset in `*rowset`.
    // return others if failed to create or init rowset.
    // NOTE: `rowset_meta` loaded from `RowsetMetaPB` before version 1.2 doesn't have tablet schema,
    //  use tablet's schema as rowset's schema for compatibility.
    static Status create_rowset(const TabletSchemaSPtr& schema, const std::string& tablet_path,
                                const RowsetMetaSharedPtr& rowset_meta, RowsetSharedPtr* rowset);

    // create and init rowset writer.
    // return OK and set `*output` to inited rowset writer.
    // return others if failed
    static Status create_rowset_writer(const RowsetWriterContext& context, bool is_vertical,
                                       std::unique_ptr<RowsetWriter>* output);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_FACTORY_H
