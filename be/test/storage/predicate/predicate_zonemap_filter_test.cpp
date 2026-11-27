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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "core/field.h"
#include "exprs/hybrid_set.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "storage/olap_define.h"
#include "storage/predicate/accept_null_predicate.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/predicate/in_list_predicate.h"
#include "storage/predicate/null_predicate.h"
#include "storage/predicate/shared_predicate.h"

namespace doris {
namespace {

using segment_v2::ZoneMap;

// Non-null values in [min, max]. `has_null` says whether the zone also holds NULL rows.
ZoneMap int_zone(int32_t min, int32_t max, bool has_null = false) {
    ZoneMap zone_map;
    zone_map.min_value = Field::create_field<TYPE_INT>(min);
    zone_map.max_value = Field::create_field<TYPE_INT>(max);
    zone_map.has_null = has_null;
    zone_map.has_not_null = true;
    return zone_map;
}

// A zone map that only records whether NULL and non-NULL rows exist. Used for null predicates,
// which never read min or max.
ZoneMap null_flags_zone(bool has_null, bool has_not_null) {
    ZoneMap zone_map;
    zone_map.min_value = Field::create_field<TYPE_INT>(0);
    zone_map.max_value = Field::create_field<TYPE_INT>(0);
    zone_map.has_null = has_null;
    zone_map.has_not_null = has_not_null;
    return zone_map;
}

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> int_pred(int32_t value, bool opposite = false) {
    return std::make_shared<ComparisonPredicateBase<TYPE_INT, PT>>(
            0, "c", Field::create_field<TYPE_INT>(value), opposite);
}

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> int_list_pred(const std::vector<int32_t>& values,
                                               bool opposite = false) {
    auto set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(false);
    for (int32_t v : values) {
        set->insert(&v);
    }
    return std::make_shared<InListPredicateBase<TYPE_INT, PT, 1>>(0, "c", set, opposite);
}

// A predicate that cannot read zone maps at all, standing in for LIKE and bloom filters.
class NoZoneMapPredicate : public ColumnPredicate {
public:
    NoZoneMapPredicate() : ColumnPredicate(0, "c", PrimitiveType::TYPE_INT, false) {}

    PredicateType type() const override { return PredicateType::EQ; }
    bool support_zonemap() const override { return false; }
    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        return std::make_shared<NoZoneMapPredicate>();
    }

private:
    uint16_t _evaluate_inner(const IColumn& column, uint16_t* sel, uint16_t size) const override {
        return size;
    }
};

std::unique_ptr<SingleColumnBlockPredicate> wrap(const std::shared_ptr<ColumnPredicate>& pred) {
    return SingleColumnBlockPredicate::create_unique(pred);
}

} // namespace

// Every operator has to name the zone it rules out and the zone it accepts whole.
TEST(PredicateZoneMapFilterTest, ComparisonAnswersBothEndsPerOperator) {
    const auto zone = int_zone(10, 20);
    const auto single = int_zone(7, 7);

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::EQ>(30)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::EQ>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::EQ>(7)->evaluate_zonemap_filter(single));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::NE>(7)->evaluate_zonemap_filter(single));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::NE>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::NE>(30)->evaluate_zonemap_filter(zone));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::LT>(10)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::LT>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::LT>(21)->evaluate_zonemap_filter(zone));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::LE>(9)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::LE>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::LE>(20)->evaluate_zonemap_filter(zone));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::GT>(20)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::GT>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::GT>(9)->evaluate_zonemap_filter(zone));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::GE>(21)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::GE>(15)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::GE>(10)->evaluate_zonemap_filter(zone));
}

// A NULL row never passes a comparison, so it takes kAllMatch away but leaves kNoMatch alone.
TEST(PredicateZoneMapFilterTest, ANullRowOnlyTakesAllMatchAway) {
    const auto with_null = int_zone(10, 20, true);

    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::NE>(30)->evaluate_zonemap_filter(with_null));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::LT>(21)->evaluate_zonemap_filter(with_null));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::GE>(10)->evaluate_zonemap_filter(with_null));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::EQ>(30)->evaluate_zonemap_filter(with_null));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::LT>(10)->evaluate_zonemap_filter(with_null));

    // Every row is NULL, so no comparison passes.
    const auto all_null = null_flags_zone(true, false);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::EQ>(0)->evaluate_zonemap_filter(all_null));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::GE>(0)->evaluate_zonemap_filter(all_null));
}

// A delete condition is stored negated. The zone map answer has to flip with it, which is what
// decides whether a page is skipped as fully deleted.
TEST(PredicateZoneMapFilterTest, OppositeSwapsNoMatchAndAllMatch) {
    const auto zone = int_zone(10, 20);
    const auto single = int_zone(7, 7);

    // `delete where c = 30`: nothing is deleted, so every row is kept.
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::EQ>(30, true)->evaluate_zonemap_filter(zone));
    // `delete where c = 7` on a zone holding only 7: no row is kept.
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::EQ>(7, true)->evaluate_zonemap_filter(single));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::NE>(30, true)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::NE>(7, true)->evaluate_zonemap_filter(single));

    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::LT>(10, true)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::LT>(21, true)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::GT>(20, true)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_pred<PredicateType::GE>(10, true)->evaluate_zonemap_filter(zone));

    // Undecided stays undecided either way round.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::EQ>(15, true)->evaluate_zonemap_filter(zone));

    // `opposite` keeps NULL rows, so a zone with NULLs can still be kept whole. This is where it
    // differs from the plain form, which needs `!has_null` before it may answer kAllMatch.
    const auto with_null = int_zone(10, 20, true);
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::EQ>(30, true)->evaluate_zonemap_filter(with_null));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::NE>(30)->evaluate_zonemap_filter(with_null));

    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_list_pred<PredicateType::IN_LIST>({7}, true)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_list_pred<PredicateType::IN_LIST>({7}, true)->evaluate_zonemap_filter(single));
}

// A pass_all zone map carries no min or max, so the two fields hold whatever was on the stack.
// Here they are set to the value the predicate asks about, which is the shape that would make an
// unguarded reader answer kNoMatch or kAllMatch and drop live rows.
TEST(PredicateZoneMapFilterTest, PassAllZoneIsNeverJudged) {
    auto poisoned = int_zone(7, 7);
    poisoned.pass_all = true;

    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::EQ>(7)->evaluate_zonemap_filter(poisoned));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::NE>(7)->evaluate_zonemap_filter(poisoned));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::LT>(7)->evaluate_zonemap_filter(poisoned));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::GT>(7)->evaluate_zonemap_filter(poisoned));

    // Delete conditions read the same entry point, so they are covered by the same guard.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_pred<PredicateType::EQ>(7, true)->evaluate_zonemap_filter(poisoned));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_list_pred<PredicateType::NOT_IN_LIST>({7})->evaluate_zonemap_filter(poisoned));

    // Null predicates read the null flags rather than min and max, but the guard is in the shared
    // entry point, so they stop at kMayMatch too.
    auto all_null = null_flags_zone(true, false);
    all_null.pass_all = true;
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              NullPredicate::create_shared(0, "c", true, PrimitiveType::TYPE_INT)
                      ->evaluate_zonemap_filter(all_null));
}

// A string bound of MAX_ZONE_MAP_INDEX_SIZE bytes was cut to fit, and the max was then bumped by
// one byte to stay an upper bound. Neither bound is a real value from the data, so proving that
// every row matches off them would drop a predicate that still removes rows.
TEST(PredicateZoneMapFilterTest, CutStringBoundsCannotProveAllMatch) {
    const std::string cut(MAX_ZONE_MAP_INDEX_SIZE, 'a');
    const std::string shrt = "aaa";

    auto zone_of = [](const std::string& v) {
        segment_v2::ZoneMap zone_map;
        zone_map.min_value = Field::create_field<TYPE_STRING>(v);
        zone_map.max_value = Field::create_field<TYPE_STRING>(v);
        zone_map.has_not_null = true;
        return zone_map;
    };
    auto str_pred = [](const std::string& v, bool opposite) {
        return std::make_shared<ComparisonPredicateBase<TYPE_STRING, PredicateType::NE>>(
                0, "c", Field::create_field<TYPE_STRING>(v), opposite);
    };

    EXPECT_TRUE(zone_of(cut).has_cut_string_bounds());
    EXPECT_FALSE(zone_of(shrt).has_cut_string_bounds());

    // `c != "zzz"` on a zone holding one short value: the bounds are real, so every row differs.
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              str_pred("zzz", false)->evaluate_zonemap_filter(zone_of(shrt)));
    // Same shape on cut bounds: the value may still sit inside the part that was cut away.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              str_pred("zzz", false)->evaluate_zonemap_filter(zone_of(cut)));

    // `delete where c != "zzz"` reaches kAllMatch through the opposite swap, which needs the
    // same guard.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              str_pred(cut, true)->evaluate_zonemap_filter(zone_of(cut)));

    // Ruling rows out still works: a cut bound is inexact, not unusable.
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              str_pred(cut, false)->evaluate_zonemap_filter(zone_of(cut)));
}

TEST(PredicateZoneMapFilterTest, InListAnswersBothEnds) {
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_list_pred<PredicateType::IN_LIST>({5})->evaluate_zonemap_filter(int_zone(6, 10)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_list_pred<PredicateType::IN_LIST>({5})->evaluate_zonemap_filter(int_zone(1, 10)));
    // The zone holds one value and the list names it.
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_list_pred<PredicateType::IN_LIST>({5})->evaluate_zonemap_filter(int_zone(5, 5)));
    // One value, but not the one in the list.
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_list_pred<PredicateType::IN_LIST>({5})->evaluate_zonemap_filter(int_zone(6, 6)));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              int_list_pred<PredicateType::NOT_IN_LIST>({5})->evaluate_zonemap_filter(
                      int_zone(5, 5)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_list_pred<PredicateType::NOT_IN_LIST>({5})->evaluate_zonemap_filter(
                      int_zone(1, 10)));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_list_pred<PredicateType::NOT_IN_LIST>({5})->evaluate_zonemap_filter(
                      int_zone(6, 10)));

    // A NULL row takes kAllMatch away here as well.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_list_pred<PredicateType::IN_LIST>({5})->evaluate_zonemap_filter(
                      int_zone(5, 5, true)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              int_list_pred<PredicateType::NOT_IN_LIST>({5})->evaluate_zonemap_filter(
                      int_zone(6, 10, true)));
}

TEST(PredicateZoneMapFilterTest, NullPredicateReadsTheNullFlags) {
    auto is_null = NullPredicate::create_shared(0, "c", true, PrimitiveType::TYPE_INT);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              is_null->evaluate_zonemap_filter(null_flags_zone(false, true)));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              is_null->evaluate_zonemap_filter(null_flags_zone(true, false)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              is_null->evaluate_zonemap_filter(null_flags_zone(true, true)));

    auto is_not_null = NullPredicate::create_shared(0, "c", false, PrimitiveType::TYPE_INT);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              is_not_null->evaluate_zonemap_filter(null_flags_zone(true, false)));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              is_not_null->evaluate_zonemap_filter(null_flags_zone(false, true)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              is_not_null->evaluate_zonemap_filter(null_flags_zone(true, true)));
}

TEST(PredicateZoneMapFilterTest, AndGroupFoldsItsChildren) {
    const auto zone = int_zone(10, 20);

    AndBlockColumnPredicate no_match;
    no_match.add_column_predicate(wrap(int_pred<PredicateType::GE>(10)));
    no_match.add_column_predicate(wrap(int_pred<PredicateType::GT>(20)));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, no_match.evaluate_zonemap_filter(zone));

    AndBlockColumnPredicate all_match;
    all_match.add_column_predicate(wrap(int_pred<PredicateType::GE>(10)));
    all_match.add_column_predicate(wrap(int_pred<PredicateType::LE>(20)));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch, all_match.evaluate_zonemap_filter(zone));

    AndBlockColumnPredicate partly_all_match;
    partly_all_match.add_column_predicate(wrap(int_pred<PredicateType::GE>(10)));
    partly_all_match.add_column_predicate(wrap(int_pred<PredicateType::LE>(15)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, partly_all_match.evaluate_zonemap_filter(zone));

    // A child that cannot read zone maps holds the group at kMayMatch instead of dropping it.
    AndBlockColumnPredicate with_unreadable;
    with_unreadable.add_column_predicate(wrap(int_pred<PredicateType::GE>(10)));
    with_unreadable.add_column_predicate(wrap(std::make_shared<NoZoneMapPredicate>()));
    EXPECT_TRUE(with_unreadable.support_zonemap());
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, with_unreadable.evaluate_zonemap_filter(zone));

    // It still rules the zone out on the strength of the child that can be read.
    AndBlockColumnPredicate unreadable_and_no_match;
    unreadable_and_no_match.add_column_predicate(wrap(std::make_shared<NoZoneMapPredicate>()));
    unreadable_and_no_match.add_column_predicate(wrap(int_pred<PredicateType::GT>(20)));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, unreadable_and_no_match.evaluate_zonemap_filter(zone));

    // No child can be read, so there is nothing to ask.
    AndBlockColumnPredicate all_unreadable;
    all_unreadable.add_column_predicate(wrap(std::make_shared<NoZoneMapPredicate>()));
    EXPECT_FALSE(all_unreadable.support_zonemap());
}

TEST(PredicateZoneMapFilterTest, PredicatesThatCannotReadZoneMaps) {
    NoZoneMapPredicate pred;
    EXPECT_FALSE(pred.support_zonemap());
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported, pred.evaluate_zonemap_filter(int_zone(10, 20)));

    // OR groups stay out of the zone map path: this interface only ever sees one column.
    OrBlockColumnPredicate or_group;
    or_group.add_column_predicate(wrap(int_pred<PredicateType::GT>(20)));
    EXPECT_FALSE(or_group.support_zonemap());
}

// The top-N filter swaps in a tighter predicate as the scan runs, so a zone that matches every row
// now may not later. Reporting kAllMatch would drop the predicate for good.
TEST(PredicateZoneMapFilterTest, SharedPredicateNeverReportsAllMatch) {
    auto shared = SharedPredicate::create_shared(0, "c");
    const auto zone = int_zone(10, 20);

    // Before the sorter arms it there is nothing to say.
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, shared->evaluate_zonemap_filter(zone));

    shared->set_nested(int_pred<PredicateType::LE>(1000));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              int_pred<PredicateType::LE>(1000)->evaluate_zonemap_filter(zone));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, shared->evaluate_zonemap_filter(zone));

    // Ruling the zone out stays true however tight the nested predicate gets, so it is forwarded.
    shared->set_nested(int_pred<PredicateType::LT>(10));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, shared->evaluate_zonemap_filter(zone));
}

// NULLS FIRST top-N keeps NULL rows, so a zone holding one can never be ruled out.
TEST(PredicateZoneMapFilterTest, AcceptNullPredicateKeepsZonesHoldingNull) {
    auto nested = int_pred<PredicateType::LT>(10);
    auto accept_null = AcceptNullPredicate::create_shared(nested);

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, nested->evaluate_zonemap_filter(int_zone(10, 20)));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              accept_null->evaluate_zonemap_filter(int_zone(10, 20)));

    // Same zone, but its NULL rows pass, so it has to be read.
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              nested->evaluate_zonemap_filter(int_zone(10, 20, true)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              accept_null->evaluate_zonemap_filter(int_zone(10, 20, true)));

    auto covering = AcceptNullPredicate::create_shared(int_pred<PredicateType::LT>(21));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch, covering->evaluate_zonemap_filter(int_zone(10, 20)));
}

} // namespace doris
