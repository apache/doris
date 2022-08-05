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

#include "file_hdfs_scanner.h"

namespace doris::vectorized {

Status ParquetFileHdfsScanner::open() {
    return Status();
}

Status ParquetFileHdfsScanner::get_next(vectorized::Block* block, bool* eof) {
    return Status();
}

void ParquetFileHdfsScanner::close() {}

void ParquetFileHdfsScanner::_prefetch_batch() {
    // 1. call file reader next batch
    // 2. push batch to queue, when get_next is called, pop batch
}

} // namespace doris::vectorized