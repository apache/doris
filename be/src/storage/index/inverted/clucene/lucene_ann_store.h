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
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/index/ann/ann_index_storage.h"

namespace lucene::store {
class Directory;
} // namespace lucene::store

namespace doris::segment_v2 {

// ANN storage over a CLucene directory: how V1 and V2 keep an ANN index, and SNII until it serves
// ANN natively. On the read side the directory is the compound reader of the index file, on the
// write side the scratch directory begin_close() packs into it. Both adapters take the Directory
// base class because the ANN tests read and write through a RAMDirectory.
//
// Every CLuceneError comes out as an INVERTED_INDEX_CLUCENE_ERROR status, the code ANN reported
// for CLucene failures when it called CLucene itself. Reads go through CLucene's
// BufferedIndexInput as they did then, so V1 and V2 send the index file the same reads; see
// AnnIndexInput::read_at for why that buffer matters.

class LuceneAnnSource final : public AnnIndexSource {
public:
    explicit LuceneAnnSource(std::shared_ptr<lucene::store::Directory> dir);

    // Each input owns its own IndexInput: its cursor and read buffer. The inputs of a compound
    // reader share the reader's one file stream, which is why the source must outlive them.
    Result<std::unique_ptr<AnnIndexInput>> open_input(std::string_view name) override;
    std::string debug_string() const override;

private:
    std::shared_ptr<lucene::store::Directory> _dir;
};

class LuceneAnnSink final : public AnnIndexSink {
public:
    explicit LuceneAnnSink(std::shared_ptr<lucene::store::Directory> dir);

    Result<std::unique_ptr<AnnIndexOutput>> create_output(std::string_view name) override;
    std::string debug_string() const override;

private:
    std::shared_ptr<lucene::store::Directory> _dir;
};

} // namespace doris::segment_v2
