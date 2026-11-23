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

#include "storage/index/inverted/analysis/tokenizer/standard/uax29_table.h"

#include <algorithm>

#include "common/check.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {

const Uax29Table& Uax29Table::instance() {
    // Built at first use, after the scanner's tables, which are initialized dynamically.
    static const Uax29Table table;
    return table;
}

Uax29Table::Uax29Table() {
    using Impl = StandardTokenizerImpl;
    DORIS_CHECK(Impl::ZZ_ATTRIBUTE.size() == kStates && Impl::ZZ_ACTION.size() == kStates &&
                Impl::ZZ_ROWMAP.size() == kStates);
    DORIS_CHECK(Impl::ZZ_CMAP_TOP.size() == kCmapTop && Impl::ZZ_CMAP_BLOCKS.size() == kCmapBlocks);
    DORIS_CHECK(*std::ranges::max_element(Impl::ZZ_CMAP_BLOCKS) == kClasses - 1);
    // The scanner looks the first 256 code points up in block 0 without the top level.
    DORIS_CHECK(Impl::ZZ_CMAP_TOP[0] == 0);

    for (int state = 0; state < kStates; ++state) {
        for (int cls = 0; cls < kClasses; ++cls) {
            const int32_t to = Impl::ZZ_TRANS[Impl::ZZ_ROWMAP[state] + cls];
            DORIS_CHECK(to >= -1 && to < kStates);
            next[state * kClasses + cls] = to < 0 ? kDead : static_cast<uint8_t>(to);
        }
        attribute[state] = static_cast<uint8_t>(Impl::ZZ_ATTRIBUTE[state]);
        action[state] = static_cast<uint8_t>(Impl::ZZ_ACTION[state]);
    }
    for (size_t c = 0; c < ascii_class.size(); ++c) {
        ascii_class[c] = static_cast<uint8_t>(Impl::ZZ_CMAP_BLOCKS[c]);
    }
    for (size_t i = 0; i < kCmapTop; ++i) {
        cmap_top[i] = static_cast<uint16_t>(Impl::ZZ_CMAP_TOP[i]);
    }
    for (size_t i = 0; i < kCmapBlocks; ++i) {
        cmap_blocks[i] = static_cast<uint8_t>(Impl::ZZ_CMAP_BLOCKS[i]);
    }
    initial = static_cast<uint8_t>(Impl::ZZ_LEXSTATE[Impl::YYINITIAL]);
}

} // namespace doris::segment_v2::inverted_index
