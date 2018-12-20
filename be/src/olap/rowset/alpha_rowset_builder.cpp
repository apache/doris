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

#include "olap/rowset/alpha_rowset_builder.h"

namespace doris {

NewStatus AlphaRowsetBuilder::init(std::string rowset_id, const std::string& rowset_path_prefix, Schema* schema) {
    return NewStatus.OK();
}

NewStatus AlphaRowsetBuilder::add_row_block(RowBlock* row_block) {
    return NewStatus.OK();
}

NewStatus AlphaRowsetBuilder::generate_written_path(const std::string& src_path, std::string* dest_path) {
    return NewStatus.OK();
}

NewStatus AlphaRowsetBuilder::build(Rowset* rowset) {
    return NewStatus.OK();
}

}  // namespace doris