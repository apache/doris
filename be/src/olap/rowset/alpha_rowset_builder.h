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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_BUILDER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_BUILDER_H

#include "olap/rowset/rowset_builder.h"

namespace doris {

class AlphaRowsetBuilder {
public:
    virtual NewStatus init(int64_t rowset_id, const std::string& rowset_path_prefix, Schema* schema);

    // add a row block to rowset
    virtual NewStatus add_row_block(const RowBlock& row_block);

    // this is a temp api
    // it is used to get rewritten path for writing rowset data
    virtual NewStatus generate_written_path(const std::string& src_path, std::string* dest_path);

    // get a rowset
    virtual NewStatus build(Rowset* rowset);
};

}

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_BUILDER_H

