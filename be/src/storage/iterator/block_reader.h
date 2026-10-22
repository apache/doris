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

#include <cstddef>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "storage/iterator/tablet_block_iterator.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {
class Block;
class RuntimeProfile;

// The row-level reader of a tablet: merges the rowsets of a read through VCollectIterator
// and lets one TabletBlockIterator, chosen by table model and read mode, turn the merged
// rows into output Blocks. The reader itself keeps no per-read-mode state.
class BlockReader final : public TabletReader {
public:
    ~BlockReader() override;

    // Initialize BlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_block_with_aggregation(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override {
        return _vcollect_iter.update_profile(profile);
    }

    // Returns the configured preferred output block byte budget; 0 when adaptive is disabled.
    size_t preferred_block_size_bytes() const override {
        return config::enable_adaptive_batch_size ? _reader_context.preferred_block_size_bytes : 0;
    }

private:
    Status _init_collect_iter(const ReaderParams& read_params);

    // Pick and initialize the TabletBlockIterator of this read.
    Status _init_block_iterator(const ReaderParams& read_params, bool merge_by_sequence_mapping);

    // return false if keys of rowsets are mono ascending and disjoint
    bool _rowsets_not_mono_asc_disjoint(const ReaderParams& read_params);

    VCollectIterator _vcollect_iter;
    std::unique_ptr<TabletBlockIterator> _block_iter;
};

} // namespace doris
