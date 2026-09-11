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
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/gram/gram_density.h"
#include "storage/index/inverted/gram/gram_extractor.h"
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

        // Variants for the tests below: a dense scheme, a bare sparse one (every optional
        // property left to its default), a sparse one whose max_gram exceeds the promised
        // literal length, and a folding one that is otherwise the base scheme.
        std::vector<TIndexPolicy> policies {tokenizer, analyzer};
        const auto add = [&](int64_t id, const std::string& name,
                             std::map<std::string, std::string> props) {
            TIndexPolicy t;
            t.id = id;
            t.name = name + "_tokenizer";
            t.type = TIndexPolicyType::TOKENIZER;
            t.properties["type"] = "ngram";
            for (auto& [k, v] : props) {
                t.properties[k] = v;
            }
            TIndexPolicy a;
            a.id = id + 1;
            a.name = name + "_analyzer";
            a.type = TIndexPolicyType::ANALYZER;
            a.properties["tokenizer"] = t.name;
            policies.push_back(t);
            policies.push_back(a);
        };
        add(9403, "dens_cal_dense", {{"mode", "dense"}, {"min_gram", "3"}});
        add(9405, "dens_cal_bare", {{"mode", "sparse"}});
        add(9407, "dens_cal_long",
            {{"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "16"}, {"density", "0.500"}});
        add(9409, "dens_cal_lc",
            {{"mode", "sparse"},
             {"min_gram", "3"},
             {"max_gram", "4"},
             {"density", "0.500"},
             {"lower_case", "true"}});
        _manager.apply_policy_changes(policies, {});
    }
    ~ScopedGramPolicies() { ExecEnv::GetInstance()->_index_policy_mgr = _previous; }

private:
    IndexPolicyMgr _manager;
    IndexPolicyMgr* _previous = nullptr;
};

TabletIndex make_index_meta(const std::string& analyzer = "dens_cal_analyzer") {
    TabletIndex meta;
    TabletIndexPB pb;
    pb.set_index_id(4401);
    pb.set_index_name("idx_dens");
    pb.set_index_type(IndexType::INVERTED);
    (*pb.mutable_properties())["analyzer"] = analyzer;
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

// A dense scheme cuts every position whatever the rate, so nothing is held back for it: its
// rows are cut as they arrive and no sample is charged.
TEST(GramDensityCalibrationTest, DenseSchemesAreNotCalibrated) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    config::enable_gram_index_adaptive_density = true;
    Defer restore([&] { config::enable_gram_index_adaptive_density = saved; });

    TabletIndex meta = make_index_meta("dens_cal_dense_analyzer");
    SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(writer.init().ok());
    ASSERT_TRUE(writer.gram_scheme_for_test().has_value());
    EXPECT_FALSE(writer.density_calibrating_for_test());
    const std::vector<std::string> data = rows(4);
    const std::vector<doris::Slice> s = slices_of(data);
    ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());
    EXPECT_GT(term_count(&writer), 0U) << "dense rows are cut as they arrive";
    EXPECT_EQ(writer.density_sample_bytes_for_test(), 0);
}

// The shipped defaults collect evidence: a tokenizer with nothing but mode=sparse gets
// max_gram 4, the promise stays at the configured 12 bytes and a solve really happens. A
// tokenizer whose max_gram exceeds the promise has the promise raised to it, since a literal
// shorter than a gram can never hold a whole one.
TEST(GramDensityCalibrationTest, DefaultsCollectEvidenceAndALongMaxGramRaisesThePromise) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    const int64_t saved_budget = config::gram_index_density_sample_bytes;
    config::enable_gram_index_adaptive_density = true;
    config::gram_index_density_sample_bytes = 4096;
    Defer restore([&] {
        config::enable_gram_index_adaptive_density = saved;
        config::gram_index_density_sample_bytes = saved_budget;
    });
    {
        TabletIndex meta = make_index_meta("dens_cal_bare_analyzer");
        SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer.gram_scheme_for_test().has_value());
        EXPECT_EQ(writer.gram_scheme_for_test()->max_len, 4U);
        EXPECT_EQ(writer.gram_scheme_for_test()->density_permille, 250U);
        EXPECT_TRUE(writer.density_calibrating_for_test());
        EXPECT_EQ(writer.density_promise_bytes_for_test(), 12U);
        // Rows of one repeated byte carry a single pair hash; picking the printable byte whose
        // self-pair hashes highest forces the solve to the ceiling, which the configured
        // default is not, so a scheme still at the default would mean no evidence reached it.
        unsigned char b = '!';
        for (int c = '!'; c <= '~'; ++c) {
            const auto cand = static_cast<unsigned char>(c);
            if (gram::boundary_hash16(cand, cand) > gram::boundary_hash16(b, b)) {
                b = cand;
            }
        }
        ASSERT_GE(gram::boundary_hash16(b, b), 32768U);
        const std::vector<std::string> flat(200, std::string(40, static_cast<char>(b)));
        const std::vector<doris::Slice> s = slices_of(flat);
        ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());
        EXPECT_FALSE(writer.density_calibrating_for_test());
        EXPECT_EQ(writer.gram_scheme_for_test()->density_permille, gram::kMaxSolvedDensityPermille)
                << "the solve ran on the defaults and reached the ceiling this corpus demands";
    }
    {
        TabletIndex meta = make_index_meta("dens_cal_long_analyzer");
        SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
        ASSERT_TRUE(writer.init().ok());
        EXPECT_TRUE(writer.density_calibrating_for_test());
        EXPECT_EQ(writer.density_promise_bytes_for_test(), 16U)
                << "max_gram 16 cannot keep a 12-byte promise; the promise is raised to it";
    }
}

// Empty rows retain a vector element and a string header each; charged for that, a run of
// them reaches the sample cap instead of growing the sample without bound.
TEST(GramDensityCalibrationTest, EmptyRowsStillReachTheSampleCap) {
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
    ASSERT_TRUE(writer.density_calibrating_for_test());
    const std::vector<std::string> empty(400, "");
    const std::vector<doris::Slice> s = slices_of(empty);
    ASSERT_TRUE(writer.add_values("c", s.data(), s.size()).ok());
    EXPECT_FALSE(writer.density_calibrating_for_test())
            << "400 empty rows retain more than 4096 bytes of vector state";
    EXPECT_EQ(writer.gram_scheme_for_test()->density_permille, 500)
            << "no window of the promised length: the configured rate stands unchanged";
}

// The solver and the sample see the bytes the extractor will see: a CHAR value ends at its
// first NUL, and its padding is neither evidence nor retained.
TEST(GramDensityCalibrationTest, CharPaddingIsNeitherEvidenceNorRetained) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    config::enable_gram_index_adaptive_density = true;
    Defer restore([&] { config::enable_gram_index_adaptive_density = saved; });

    TabletIndex meta = make_index_meta();
    SniiIndexColumnWriter writer(nullptr, &meta, FieldType::OLAP_FIELD_TYPE_CHAR);
    ASSERT_TRUE(writer.init().ok());
    std::string padded = "rpc error: code = Unavailable";
    const auto logical = static_cast<int64_t>(padded.size());
    padded.resize(64, '\0');
    const doris::Slice slice(padded.data(), padded.size());
    ASSERT_TRUE(writer.add_values("c", &slice, 1).ok());
    EXPECT_EQ(writer.density_sample_bytes_for_test(),
              logical + static_cast<int64_t>(sizeof(std::pair<uint32_t, std::string>)));
}

// A folding scheme folds its evidence: upper-case rows through it solve to what their
// lower-case twins solve to through the otherwise identical plain scheme.
TEST(GramDensityCalibrationTest, FoldingSchemesFoldTheEvidence) {
    ScopedGramPolicies policies;
    const bool saved = config::enable_gram_index_adaptive_density;
    const int64_t saved_budget = config::gram_index_density_sample_bytes;
    config::enable_gram_index_adaptive_density = true;
    config::gram_index_density_sample_bytes = 4096;
    Defer restore([&] {
        config::enable_gram_index_adaptive_density = saved;
        config::gram_index_density_sample_bytes = saved_budget;
    });

    std::vector<std::string> lower = rows(200);
    for (std::string& row : lower) {
        for (char& ch : row) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
    }
    std::vector<std::string> upper = lower;
    for (std::string& row : upper) {
        for (char& ch : row) {
            if (ch >= 'a' && ch <= 'z') {
                ch = static_cast<char>(ch - 'a' + 'A');
            }
        }
    }
    TabletIndex folding_meta = make_index_meta("dens_cal_lc_analyzer");
    SniiIndexColumnWriter folding(nullptr, &folding_meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(folding.init().ok());
    const std::vector<doris::Slice> u = slices_of(upper);
    ASSERT_TRUE(folding.add_values("c", u.data(), u.size()).ok());

    TabletIndex plain_meta = make_index_meta();
    SniiIndexColumnWriter plain(nullptr, &plain_meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(plain.init().ok());
    const std::vector<doris::Slice> l = slices_of(lower);
    ASSERT_TRUE(plain.add_values("c", l.data(), l.size()).ok());

    ASSERT_FALSE(folding.density_calibrating_for_test());
    ASSERT_FALSE(plain.density_calibrating_for_test());
    EXPECT_EQ(folding.gram_scheme_for_test()->density_permille,
              plain.gram_scheme_for_test()->density_permille);
}

// The coverage share and its neighbours are validated where they are set: a negative or
// impossible value is refused instead of reaching the writer's checked cast on every load.
TEST(GramDensityCalibrationTest, ConfigValidatorsRejectImpossibleValues) {
    const auto coverage = std::to_string(config::gram_index_density_coverage_permille);
    const auto literal = std::to_string(config::gram_index_min_literal_bytes);
    const auto sample = std::to_string(config::gram_index_density_sample_bytes);
    EXPECT_FALSE(config::set_config("gram_index_density_coverage_permille", "-1").ok());
    EXPECT_FALSE(config::set_config("gram_index_density_coverage_permille", "0").ok());
    EXPECT_FALSE(config::set_config("gram_index_density_coverage_permille", "1001").ok());
    EXPECT_TRUE(config::set_config("gram_index_density_coverage_permille", "950").ok());
    EXPECT_FALSE(config::set_config("gram_index_min_literal_bytes", "0").ok());
    EXPECT_TRUE(config::set_config("gram_index_min_literal_bytes", "12").ok());
    EXPECT_FALSE(config::set_config("gram_index_density_sample_bytes", "-1").ok());
    EXPECT_TRUE(config::set_config("gram_index_density_sample_bytes", "4194304").ok());
    EXPECT_TRUE(config::set_config("gram_index_density_coverage_permille", coverage).ok());
    EXPECT_TRUE(config::set_config("gram_index_min_literal_bytes", literal).ok());
    EXPECT_TRUE(config::set_config("gram_index_density_sample_bytes", sample).ok());
}

} // namespace doris::segment_v2
