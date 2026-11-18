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

#include <memory>

#include "common/status.h"

namespace lucene::store {
class Directory;
} // namespace lucene::store

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris::segment_v2 {

// The rows written with the delete sign that a merge-on-write CLucene text index gives no terms
// (IndexColumnWriter::set_delete_sign_rows), kept in the sub-file
// InvertedIndexDescriptor::get_delete_sign_rows_file_name() of the index's directory as a portable
// roaring bitmap. The file is written even when no row has the sign, so its absence means the
// index records none: an older BE wrote or merged it.

// Writes `rows` into the index directory `dir`.
Status write_clucene_delete_sign_rows(lucene::store::Directory* dir, const roaring::Roaring& rows);

// Sets `out` to the rows the index in `dir` records, or to null when it records none.
Status read_clucene_delete_sign_rows(lucene::store::Directory* dir,
                                     std::shared_ptr<const roaring::Roaring>* out);

} // namespace doris::segment_v2
