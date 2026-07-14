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

#include "storage/index/snii/query/query_profile.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_query_profile_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

struct Corpus {
    std::vector<std::vector<std::string>> docs;
};

Corpus BuildCorpus() {
    Corpus c;
    c.docs.resize(128);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        std::vector<std::string>& doc = c.docs[d];
        doc.emplace_back("lead");
        doc.emplace_back("quick");
        doc.emplace_back(d % 2 == 0 ? "brown" : "bronze");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        doc.emplace_back(term);
    }
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        const std::vector<std::string>& terms = c.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = doris::snii::format::IndexConfig::kDocsPositionsScoring;
    in.doc_count = static_cast<uint32_t>(c.docs.size());
    in.encoded_norms.assign(c.docs.size(), 1);
    in.terms = buf.finalize_sorted();
    in.target_dict_block_bytes = 512;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(in).ok());
    ASSERT_TRUE(compound.finish().ok());
}

LogicalIndexReader OpenMeteredIndex(io::MeteredFileReader* file, SniiSegmentReader* segment) {
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

void ExpectProfileMatchesMeteredDelta(io::MeteredFileReader* metered,
                                      const std::function<Status(query::QueryProfile*)>& run) {
    metered->reset_metrics();
    query::QueryProfile profile;
    const Status st = run(&profile);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_GT(profile.elapsed_ns, 0U);
    ASSERT_TRUE(profile.has_io_metrics);
    EXPECT_EQ(profile.io_delta.read_at_calls, metered->metrics().read_at_calls);
    EXPECT_EQ(profile.io_delta.serial_rounds, metered->metrics().serial_rounds);
    EXPECT_EQ(profile.io_delta.range_gets, metered->metrics().range_gets);
    EXPECT_EQ(profile.io_delta.remote_bytes, metered->metrics().remote_bytes);
    EXPECT_EQ(profile.io_delta.total_request_bytes, metered->metrics().total_request_bytes);
}

} // namespace

TEST(SniiQueryProfile, ReportsElapsedTimeAndMeteredIoForNativeOperators) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/512);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    std::vector<uint32_t> docs;
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::term_query(idx, "lead", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::boolean_or(idx, {"lead", "missing"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::boolean_and(idx, {"quick", "brown"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::prefix_query(idx, "aa_", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::wildcard_query(idx, "aa_0??", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::regexp_query(idx, "aa_00[0-9]", &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::phrase_query(idx, {"quick", "brown"}, &docs, profile);
    });
    ExpectProfileMatchesMeteredDelta(&metered, [&](query::QueryProfile* profile) {
        return query::phrase_prefix_query(idx, {"quick", "bro"}, &docs, profile);
    });

    std::remove(path.c_str());
}

TEST(SniiQueryProfile, ReportsElapsedTimeForOperatorErrorPath) {
    const Corpus corpus = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/512);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    std::vector<uint32_t> docs;
    query::QueryProfile profile;
    const Status st = query::regexp_query(idx, "(", &docs, &profile);

    EXPECT_FALSE(st.ok());
    EXPECT_GT(profile.elapsed_ns, 0U);
    ASSERT_TRUE(profile.has_io_metrics);
    EXPECT_EQ(profile.io_delta.read_at_calls, 0U);
    EXPECT_EQ(profile.io_delta.serial_rounds, 0U);
    EXPECT_EQ(profile.io_delta.range_gets, 0U);
    EXPECT_EQ(profile.io_delta.remote_bytes, 0U);
    EXPECT_EQ(profile.io_delta.total_request_bytes, 0U);

    std::remove(path.c_str());
}

TEST(SniiQueryProfile, ScopeFinalizesProfileOnEarlyReturn) {
    query::QueryProfile profile;
    { query::QueryProfileScope scope(/*reader=*/nullptr, &profile); }

    EXPECT_GT(profile.elapsed_ns, 0U);
    EXPECT_FALSE(profile.has_io_metrics);
}
