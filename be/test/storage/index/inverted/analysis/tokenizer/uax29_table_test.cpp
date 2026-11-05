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

#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <utility>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {
namespace {

using Impl = StandardTokenizerImpl;

TEST(Uax29TableTest, MatchesTheScannerTablesEntryByEntry) {
    const Uax29Table& table = Uax29Table::instance();
    ASSERT_EQ(Impl::ZZ_ATTRIBUTE.size(), Uax29Table::kStates);
    for (int state = 0; state < Uax29Table::kStates; ++state) {
        for (int cls = 0; cls < Uax29Table::kClasses; ++cls) {
            const int32_t to = Impl::ZZ_TRANS[Impl::ZZ_ROWMAP[state] + cls];
            EXPECT_EQ(table.next[state * Uax29Table::kClasses + cls],
                      to < 0 ? Uax29Table::kDead : to)
                    << "state " << state << " class " << cls;
        }
        EXPECT_EQ(table.attribute[state], Impl::ZZ_ATTRIBUTE[state]) << state;
        EXPECT_EQ(table.action[state], Impl::ZZ_ACTION[state]) << state;
    }
    for (int c = 0; c < 128; ++c) {
        EXPECT_EQ(table.ascii_class[c], Impl::ZZ_CMAP_BLOCKS[c]) << c;
    }
    EXPECT_EQ(table.initial, Impl::ZZ_LEXSTATE[Impl::YYINITIAL]);
}

TEST(Uax29TableTest, ClassesEveryCodePointAsTheScanner) {
    const Uax29Table& table = Uax29Table::instance();
    Impl scanner;
    for (uint32_t code_point = 0; code_point <= 0x10FFFF; ++code_point) {
        ASSERT_EQ(table.char_class(code_point), scanner.zz_cmap(static_cast<int32_t>(code_point)))
                << code_point;
    }
}

// The first character always makes a match, so a match is empty only when the window refuses the
// first character, which Uax29Table::longest_match() reports as action -1.
TEST(Uax29TableTest, EveryCharacterLeadsFromTheInitialStateToAnAcceptingOne) {
    const Uax29Table& table = Uax29Table::instance();
    EXPECT_EQ(table.attribute[table.initial] & Uax29Table::kAccepting, 0);
    for (int cls = 0; cls < Uax29Table::kClasses; ++cls) {
        const uint8_t to = table.next[table.initial * Uax29Table::kClasses + cls];
        ASSERT_NE(to, Uax29Table::kDead) << cls;
        EXPECT_NE(table.attribute[to] & Uax29Table::kAccepting, 0) << cls;
    }
}

// Why the builtin tokenizer may lowercase the whole value before it scans: folding A-Z changes no
// class, so the scan makes the same tokens, and every accepting state the DFA reaches through a
// letter is WORD, so no token of another type has a letter to fold. If a new table breaks either
// half, this fails before a token changes.
TEST(Uax29TableTest, LowercasingTheWholeValueOnlyLowercasesWords) {
    const Uax29Table& table = Uax29Table::instance();
    const int letter = table.ascii_class['A'];
    for (char c = 'A'; c <= 'Z'; ++c) {
        EXPECT_EQ(table.ascii_class[c], letter) << c;
        EXPECT_EQ(table.ascii_class[c - 'A' + 'a'], letter) << c;
    }
    // Every (state, a letter on the way there) pair the DFA can reach.
    std::set<std::pair<uint8_t, bool>> seen;
    std::vector<std::pair<uint8_t, bool>> todo {{table.initial, false}};
    while (!todo.empty()) {
        const auto [state, through_letter] = todo.back();
        todo.pop_back();
        if (!seen.insert({state, through_letter}).second) {
            continue;
        }
        for (int cls = 0; cls < Uax29Table::kClasses; ++cls) {
            const uint8_t to = table.next[state * Uax29Table::kClasses + cls];
            if (to != Uax29Table::kDead) {
                todo.emplace_back(to, through_letter || cls == letter);
            }
        }
    }
    size_t words = 0;
    for (const auto& [state, through_letter] : seen) {
        if (through_letter && (table.attribute[state] & Uax29Table::kAccepting) != 0) {
            EXPECT_EQ(table.action[state], Uax29Table::kWord) << static_cast<int>(state);
            ++words;
        }
    }
    EXPECT_GT(words, 0);
}

} // namespace
} // namespace doris::segment_v2::inverted_index
