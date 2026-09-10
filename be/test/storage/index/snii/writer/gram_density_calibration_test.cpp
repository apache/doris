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
//
// The write path solving its own density.
//
// Two properties have to hold together, and they pull against each other. Every row of a
// segment must be cut at ONE rate, because the query side reconstructs a segment's grams from
// the single rate its metadata records -- so the rate has to be known before the first row is
// cut, which is why the first rows are held back. And no row may be lost to that: a segment
// shorter than the sample never fills it, and its rows still have to be indexed.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/gram/gram_density.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

// A gram-family analyzer at a deliberately dense configured rate, so a solved rate is
// distinguishable from the configured one.
class ScopedGramPolicies {
public:
    ScopedGramPolicies() {
        auto* exec_env = ExecEnv::GetInstance();
        _previous = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &_manager;

        TIndexPolicy tokenizer;
        tokenizer.id = 9401;
        tokenizer.name = "dens_cal_tokenizer";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "ngram";
        tokenizer.properties["mode"] = "sparse";
        tokenizer.properties["min_gram"] = "3";
        tokenizer.properties["max_gram"] = "4";
        tokenizer.properties["density"] = "0.500";

        TIndexPolicy analyzer;
        analyzer.id = 9402;
        analyzer.name = "dens_cal_analyzer";
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = "dens_cal_tokenizer";
        _manager.apply_policy_changes({tokenizer, analyzer}, {});
    }
    ~ScopedGramPolicies() { ExecEnv::GetInstance()->_index_policy_mgr = _previous; }

private:
    IndexPolicyMgr _manager;
    IndexPolicyMgr* _previous = nullptr;
};

TabletIndex make_index_meta() {
    TabletIndex meta;
    TabletIndexPB pb;
    pb.set_index_id(4401);
    pb.set_index_name("idx_dens");
    pb.set_index_type(IndexType::INVERTED);
    (*pb.mutable_properties())["analyzer"] = "dens_cal_analyzer";
    (*pb.mutable_properties())["support_phrase"] = "false";
    meta.init_from_pb(pb);
    return meta;
}

// Rows long enough to carry windows of the promised length, and varied so their byte pairs do
// not collapse to a handful.
std::vector<std::string> rows(size_t n) {
    std::vector<std::string> out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        out.push_back("rpc error: code = Unavailable desc = transport closing conn " +
                      std::to_string(i * 7919));
    }
    return out;
}

std::vector<doris::Slice> slices_of(const std::vector<std::string>& v) {
    std::vector<doris::Slice> s;
    s.reserve(v.size());
    for (const std::string& r : v) {
        s.emplace_back(r.data(), r.size());
    }
    return s;
}

size_t term_count(SniiIndexColumnWriter* writer) {
    return writer->term_buffer_for_test()->finalize_sorted().size();
}

} // namespace

// Held back, then all of them: nothing written before the rate is settled, and everything
// written once it is.
TEST(GramDensityCalibrationTest, RowsAreHeldBackUntilTheRateIsSolvedAndThenAllIndexed) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    const int64_t saved_budget = config::gram_index_density_sample_bytes;
    config::enable_gram_index_adaptive_density = true;
    config::gram_index_density_sample_bytes = 4096;
    Defer restore([&] {
        config::enable_gram_index_adaptive_density = saved;
        config::gram_index_density_sample_bytes = saved_budget;
    });

    // The term buffer may be drained only once, so the two halves of the property need a
    // writer each: one stopped while still calibrating, one carried past the budget.
    TabletIndex meta = make_index_meta();
    {
        SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
        ASSERT_TRUE(writer.init().ok());
        const std::vector<std::string> first = rows(4);
        const std::vector<doris::Slice> s = slices_of(first);
        ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());
        EXPECT_EQ(term_count(&writer), 0U)
                << "a row cut before the rate is known could not be cut at the rate the "
                   "segment will record";
    }
    {
        SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
        ASSERT_TRUE(writer.init().ok());
        const std::vector<std::string> data = rows(200);
        const std::vector<doris::Slice> s = slices_of(data);
        ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());
        EXPECT_GT(term_count(&writer), 0U) << "the held-back rows must be indexed, not dropped";
    }
}

// The recorded scheme carries the rate the rows were actually cut at, which is what the query
// side rebuilds its grams from. A solved rate that never reached the metadata would make every
// row of the segment unreachable.
TEST(GramDensityCalibrationTest, TheRecordedSchemeCarriesTheSolvedRate) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    const int64_t saved_budget = config::gram_index_density_sample_bytes;
    config::enable_gram_index_adaptive_density = true;
    config::gram_index_density_sample_bytes = 4096;
    Defer restore([&] {
        config::enable_gram_index_adaptive_density = saved;
        config::gram_index_density_sample_bytes = saved_budget;
    });

    TabletIndex meta = make_index_meta();
    SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(writer.init().ok());
    ASSERT_TRUE(writer.gram_scheme_for_test().has_value());
    EXPECT_EQ(writer.gram_scheme_for_test()->density_permille, 500)
            << "before any row it is still the configured rate";

    const std::vector<std::string> data = rows(200);
    const std::vector<doris::Slice> s = slices_of(data);
    ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());

    EXPECT_NE(writer.gram_scheme_for_test()->density_permille, 500)
            << "this corpus does not need a rate of 0.5 to find 12-byte literals; a scheme "
               "still holding the configured one means the solve never reached it";
    EXPECT_GE(writer.gram_scheme_for_test()->density_permille, gram::kMinSolvedDensityPermille);
    EXPECT_LE(writer.gram_scheme_for_test()->density_permille, gram::kMaxSolvedDensityPermille);
}

// With the feature off nothing is held back and the configured rate stands, so an index built
// before this existed is reproduced exactly.
TEST(GramDensityCalibrationTest, TheConfiguredRateStandsWhenTheSolveIsOff) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    config::enable_gram_index_adaptive_density = false;
    Defer restore([&] { config::enable_gram_index_adaptive_density = saved; });

    TabletIndex meta = make_index_meta();
    SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(writer.init().ok());
    const std::vector<std::string> data = rows(4);
    const std::vector<doris::Slice> s = slices_of(data);
    ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());

    EXPECT_GT(term_count(&writer), 0U) << "nothing is held back when there is nothing to solve";
    EXPECT_EQ(writer.gram_scheme_for_test()->density_permille, 500);
}

} // namespace doris::segment_v2
