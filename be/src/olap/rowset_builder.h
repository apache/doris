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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_BUILDER_H
#define DORIS_BE_SRC_OLAP_ROWSET_BUILDER_H

#include "rowset/rowset.h"
#include "olap/new_status.h"
#include "olap/schema.h"
#include "olap/row_block.h"

namespace doris {

class RowsetBuilder {
public:
    NewStatus init(std::string rowset_id, const std::string& rowset_path_prefix, Schema* schema) = 0;

    // add a row block to rowset
    NewStatus add_row_block(RowBlock* row_block) = 0;

    // this is a temp api
    // it is used to get rewritten path for writing rowset data
    NewStatus generate_written_path(const std::string& src_path, std::string* dest_path) = 0;

    // get a rowset
    NewStatus build(Rowset* rowset) = 0;
};

}

#endif // DORIS_BE_SRC_OLAP_ROWSET_BUILDER_H
