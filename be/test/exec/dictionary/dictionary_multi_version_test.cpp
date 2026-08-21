// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <memory>

#include "core/block/columns_with_type_and_name.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/complex_hash_map_dictionary.h"
#include "exprs/function/dictionary.h"
#include "exprs/function/dictionary_factory.h"

namespace doris {

static DictionaryPtr make_dict(const std::string& name) {
    return create_complex_hash_map_dict_from_column(
            name,
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
}

static void commit_version(DictionaryFactory& f, int64_t dict_id, int64_t version_id) {
    auto dict = make_dict("dict_" + std::to_string(dict_id));
    EXPECT_TRUE(f.refresh_dict(dict_id, version_id, dict));
    // reset GC timer to force GC on every commit
    f._last_gc_time_ms = 0;
    EXPECT_TRUE(f.commit_refresh_dict(dict_id, version_id));
}

// ============ basic multi-version ============

TEST(DictionaryMultiVersionTest, GetAfterMultipleCommits) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    commit_version(f, 1, 3);

    EXPECT_NE(nullptr, f.get(1, 3));
    EXPECT_NE(nullptr, f.get(1, 2));
    EXPECT_NE(nullptr, f.get(1, 1));
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, GetMissingVersion) {
    DictionaryFactory f;
    commit_version(f, 1, 1);
    EXPECT_NE(nullptr, f.get(1, 1));
    EXPECT_EQ(nullptr, f.get(1, 99));
    EXPECT_EQ(nullptr, f.get(999, 1));
}

// ============ count-based GC ============

TEST(DictionaryMultiVersionTest, GCByCount) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 2;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    commit_version(f, 1, 3);
    // max_versions=2, v=1 should be GC'd
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    EXPECT_NE(nullptr, f.get(1, 3));
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, GCByCountOne) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 1;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, GCKeepsLatest) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 1;
    DictionaryFactory f;
    for (int i = 1; i <= 5; i++) {
        commit_version(f, 1, i);
    }
    EXPECT_EQ(nullptr, f.get(1, 4));
    EXPECT_NE(nullptr, f.get(1, 5));
    config::dictionary_max_versions = old_max;
}

// ============ TTL-based GC ============

TEST(DictionaryMultiVersionTest, TTLExpired) {
    auto old_ttl = config::dictionary_version_ttl_seconds;
    auto old_max = config::dictionary_max_versions;
    config::dictionary_version_ttl_seconds = 1;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    // simulate v=1 committed 2 seconds ago
    f._dict_id_to_versioned_map[1][1]->set_commit_time_ms(UnixMillis() - 2000);
    commit_version(f, 1, 2);
    // GC triggered by commit should drop v=1 (expired)
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_version_ttl_seconds = old_ttl;
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, TTLKeepsLatest) {
    auto old_ttl = config::dictionary_version_ttl_seconds;
    auto old_max = config::dictionary_max_versions;
    config::dictionary_version_ttl_seconds = 1;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    f._dict_id_to_versioned_map[1][1]->set_commit_time_ms(UnixMillis() - 2000);
    commit_version(f, 1, 2);
    f._dict_id_to_versioned_map[1][2]->set_commit_time_ms(UnixMillis() - 2000);
    // both expired, but latest must survive
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_version_ttl_seconds = old_ttl;
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, TTLZero) {
    auto old_ttl = config::dictionary_version_ttl_seconds;
    auto old_max = config::dictionary_max_versions;
    config::dictionary_version_ttl_seconds = 0;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    f._dict_id_to_versioned_map[1][1]->set_commit_time_ms(UnixMillis() - 100000);
    commit_version(f, 1, 2);
    // ttl=0, no TTL GC; both kept
    EXPECT_NE(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_version_ttl_seconds = old_ttl;
    config::dictionary_max_versions = old_max;
}

// ============ extreme configs ============

TEST(DictionaryMultiVersionTest, GCMaxVersionsZero) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 0;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    // 0 falls back to 1; only latest kept
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, GCMaxVersionsNegative) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = -5;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    // negative falls back to 1; only latest kept
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, GCMaxVersionsLarge) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 1000000;
    DictionaryFactory f;
    for (int i = 1; i <= 5; i++) {
        commit_version(f, 1, i);
    }
    // large max; all versions kept
    for (int i = 1; i <= 5; i++) {
        EXPECT_NE(nullptr, f.get(1, i));
    }
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, TTLNegative) {
    auto old_ttl = config::dictionary_version_ttl_seconds;
    auto old_max = config::dictionary_max_versions;
    config::dictionary_version_ttl_seconds = -1;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    f._dict_id_to_versioned_map[1][1]->set_commit_time_ms(UnixMillis() - 100000);
    commit_version(f, 1, 2);
    // negative ttl = no TTL GC
    EXPECT_NE(nullptr, f.get(1, 1));
    config::dictionary_version_ttl_seconds = old_ttl;
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, GCIntervalLarge) {
    auto old_interval = config::dictionary_gc_interval_seconds;
    auto old_max = config::dictionary_max_versions;
    config::dictionary_gc_interval_seconds = 1000000;
    config::dictionary_max_versions = 1;
    DictionaryFactory f;
    f._last_gc_time_ms = UnixMillis();
    // manual commit without resetting GC timer
    auto dict1 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 1, dict1));
    EXPECT_TRUE(f.commit_refresh_dict(1, 1));
    auto dict2 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 2, dict2));
    EXPECT_TRUE(f.commit_refresh_dict(1, 2));
    // count-based GC runs on every commit regardless of interval; v=1 dropped
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_gc_interval_seconds = old_interval;
    config::dictionary_max_versions = old_max;
}

// ============ boundary scenarios ============

TEST(DictionaryMultiVersionTest, CommitOverwritesSameVersion) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    // commit same version again should overwrite (idempotent), not fail
    auto dict = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 1, dict));
    EXPECT_TRUE(f.commit_refresh_dict(1, 1));
    // staging cleared after commit
    EXPECT_TRUE(!f._refreshing_dict_map.contains(1));
    // get still works
    EXPECT_NE(nullptr, f.get(1, 1));
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, CommitRejectsOutOfOrder) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 2);
    // commit v=1 when latest=v=2 should fail (out of order)
    auto dict = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 1, dict));
    auto st = f.commit_refresh_dict(1, 1);
    EXPECT_FALSE(st.ok());
    // staging still there (not consumed by failed commit)
    EXPECT_TRUE(f._refreshing_dict_map.contains(1));
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, PartialRollbackRecovery) {
    auto old_max = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    // BE1 commit v=1 success
    commit_version(f, 1, 1);
    // simulate partial rollback: FE abort v=1 after commit succeeded.
    // staging already consumed by commit, abort is silent OK, orphan v=1 remains.
    EXPECT_TRUE(f.abort_refresh_dict(1, 1).ok());
    // orphan v=1 still in versioned_map
    EXPECT_NE(nullptr, f.get(1, 1));
    // next refresh: FE INC to v=1 again, refresh_dict writes new staging v=1
    auto dict_new = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 1, dict_new));
    // commit v=1: should overwrite orphan, not fail with "version <= latest"
    EXPECT_TRUE(f.commit_refresh_dict(1, 1));
    // staging cleared
    EXPECT_TRUE(!f._refreshing_dict_map.contains(1));
    // get works
    EXPECT_NE(nullptr, f.get(1, 1));
    config::dictionary_max_versions = old_max;
}

TEST(DictionaryMultiVersionTest, GetNonExistentDict) {
    DictionaryFactory f;
    EXPECT_EQ(nullptr, f.get(999, 1));
}

TEST(DictionaryMultiVersionTest, DeleteAllVersions) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    commit_version(f, 1, 3);
    EXPECT_TRUE(f.delete_dict(1).ok());
    EXPECT_EQ(nullptr, f.get(1, 1));
    EXPECT_EQ(nullptr, f.get(1, 2));
    EXPECT_EQ(nullptr, f.get(1, 3));
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, DeleteNonExistent) {
    DictionaryFactory f;
    EXPECT_TRUE(f.delete_dict(999).ok());
}

TEST(DictionaryMultiVersionTest, DeleteAfterGC) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 1;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    // v=1 already GC'd
    EXPECT_TRUE(f.delete_dict(1).ok());
    EXPECT_EQ(nullptr, f.get(1, 2));
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, CommitAfterDelete) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    EXPECT_TRUE(f.delete_dict(1).ok());
    // commit new version after delete
    commit_version(f, 1, 2);
    EXPECT_NE(nullptr, f.get(1, 2));
    config::dictionary_max_versions = old;
}

// ============ get_dictionary_status ============

TEST(DictionaryMultiVersionTest, GetStatusReportsLatest) {
    auto old = config::dictionary_max_versions;
    config::dictionary_max_versions = 10;
    DictionaryFactory f;
    commit_version(f, 1, 1);
    commit_version(f, 1, 2);
    commit_version(f, 1, 3);
    std::vector<TDictionaryStatus> result;
    f.get_dictionary_status(result, {});
    EXPECT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].dictionary_id);
    EXPECT_EQ(3, result[0].version_id);
    config::dictionary_max_versions = old;
}

TEST(DictionaryMultiVersionTest, GetStatusEmptyAfterDelete) {
    DictionaryFactory f;
    commit_version(f, 1, 1);
    EXPECT_TRUE(f.delete_dict(1).ok());
    std::vector<TDictionaryStatus> result;
    f.get_dictionary_status(result, {});
    EXPECT_EQ(0, result.size());
}

// ============ staging fallback ============

TEST(DictionaryMultiVersionTest, GetFromStagingFallback) {
    DictionaryFactory f;
    // commit v=1 to committed map
    commit_version(f, 1, 1);
    // refresh v=2 to staging (not committed yet)
    auto dict2 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 2, dict2));
    // get(v=2): not in committed map, should fallback to staging
    EXPECT_NE(nullptr, f.get(1, 2));
}

TEST(DictionaryMultiVersionTest, GetStagingVersionMismatch) {
    DictionaryFactory f;
    commit_version(f, 1, 1);
    // staging has v=3
    auto dict3 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 3, dict3));
    // get(v=2): not in committed map, staging has v=3 (mismatch) -> nullptr
    EXPECT_EQ(nullptr, f.get(1, 2));
}

TEST(DictionaryMultiVersionTest, GetAfterCommitRemovesStaging) {
    DictionaryFactory f;
    commit_version(f, 1, 1);
    // staging v=2
    auto dict2 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 2, dict2));
    EXPECT_NE(nullptr, f.get(1, 2)); // staging fallback
    // commit v=2: staging -> committed map
    EXPECT_TRUE(f.commit_refresh_dict(1, 2));
    // staging should be empty now
    EXPECT_TRUE(!f._refreshing_dict_map.contains(1));
    // get(v=2) should hit committed map
    EXPECT_NE(nullptr, f.get(1, 2));
}

TEST(DictionaryMultiVersionTest, GetAfterAbortRemovesStaging) {
    DictionaryFactory f;
    commit_version(f, 1, 1);
    // staging v=2
    auto dict2 = make_dict("dict_1");
    EXPECT_TRUE(f.refresh_dict(1, 2, dict2));
    EXPECT_NE(nullptr, f.get(1, 2)); // staging fallback
    // abort v=2: staging removed
    EXPECT_TRUE(f.abort_refresh_dict(1, 2));
    // get(v=2): not in committed map, staging gone -> nullptr
    EXPECT_EQ(nullptr, f.get(1, 2));
    // get(v=1): still in committed map
    EXPECT_NE(nullptr, f.get(1, 1));
}

} // namespace doris
