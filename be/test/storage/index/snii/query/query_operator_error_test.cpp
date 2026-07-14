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
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"
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
    return "/tmp/snii_query_operator_error_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

// In-memory FileReader (backed by a flat byte buffer) whose reads can be made
// to fail on demand, so queries can be driven over the I/O-error propagation
// paths after the index has been opened successfully.
class FaultInjectingReader : public io::FileReader {
public:
    explicit FaultInjectingReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    void set_fail_reads(bool v) { fail_reads_ = v; }

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (fail_reads_) {
            return Status::IOError<false>("injected read failure");
        }
        if (out == nullptr) {
            return Status::InvalidArgument<false>("memory reader null out");
        }
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Corruption<false>("memory reader read past end");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

private:
    std::vector<uint8_t> bytes_;
    bool fail_reads_ = false;
};

struct Corpus {
    std::vector<std::vector<std::string>> docs = {
            {"alpha", "beta", "gamma"}, {"alpha", "beta", "delta"}, {"alpha", "bravo", "gamma"},
            {"beta", "gamma"},          {"alphabeta", "beta"},
    };
};

struct EnvGuard {
    std::string name;
    bool had = false;
    std::string old;

    explicit EnvGuard(const char* env_name) : name(env_name) {
        const char* value = std::getenv(env_name);
        had = value != nullptr;
        if (had) {
            old = value;
        }
    }

    ~EnvGuard() {
        if (had) {
            ::setenv(name.c_str(), old.c_str(), 1);
        } else {
            ::unsetenv(name.c_str());
        }
    }
};

void BuildIndexBytes(const Corpus& corpus, doris::snii::format::IndexConfig config,
                     std::vector<uint8_t>* bytes) {
    SpimiTermBuffer buf(/*has_positions=*/config != doris::snii::format::IndexConfig::kDocsOnly);
    for (uint32_t d = 0; d < corpus.docs.size(); ++d) {
        const std::vector<std::string>& terms = corpus.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = config;
    in.doc_count = static_cast<uint32_t>(corpus.docs.size());
    if (config == doris::snii::format::IndexConfig::kDocsPositionsScoring) {
        in.encoded_norms.assign(corpus.docs.size(), 1);
    }
    in.terms = buf.finalize_sorted();
    in.target_dict_block_bytes = 512;

    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        ASSERT_TRUE(compound.add_logical_index(in).ok());
        ASSERT_TRUE(compound.finish().ok());
    }
    {
        io::LocalFileReader file;
        ASSERT_TRUE(file.open(path).ok());
        ASSERT_TRUE(file.read_at(0, file.size(), bytes).ok());
    }
    std::remove(path.c_str());
}

LogicalIndexReader OpenIndex(FaultInjectingReader* file, SniiSegmentReader* segment) {
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

void ExpectIoError(const Status& status) {
    EXPECT_TRUE(status.is<doris::ErrorCode::IO_ERROR>()) << status.to_string();
}

} // namespace

TEST(SniiQueryOperatorBoundaries, RejectNullOutputPointers) {
    LogicalIndexReader idx;
    // A typed null output pointer selects each operator's std::vector<uint32_t>*
    // overload unambiguously (the integrated API also exposes DocIdSink*
    // overloads, so a bare nullptr would be ambiguous).
    auto* null_docs = static_cast<std::vector<uint32_t>*>(nullptr);
    EXPECT_TRUE(
            query::term_query(idx, "alpha", null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            query::boolean_or(idx, {"alpha"}, null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            query::boolean_and(idx, {"alpha"}, null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(query::prefix_query(idx, "al", null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            query::wildcard_query(idx, "a*", null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            query::regexp_query(idx, "a.*", null_docs).is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(query::phrase_query(idx, {"alpha"}, null_docs)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(query::phrase_prefix_query(idx, {"alpha"}, null_docs)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiQueryOperatorBoundaries, EmptyMissingAndSingleTermCasesAreWellDefined) {
    std::vector<uint8_t> bytes;
    BuildIndexBytes(Corpus {}, doris::snii::format::IndexConfig::kDocsPositionsScoring, &bytes);
    FaultInjectingReader file(std::move(bytes));
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment);

    std::vector<uint32_t> got = {99};
    ASSERT_TRUE(query::term_query(idx, "missing", &got).ok());
    EXPECT_TRUE(got.empty());

    got = {99};
    ASSERT_TRUE(query::boolean_or(idx, {}, &got).ok());
    EXPECT_TRUE(got.empty());

    got = {99};
    ASSERT_TRUE(query::boolean_and(idx, {}, &got).ok());
    EXPECT_TRUE(got.empty());

    got = {99};
    ASSERT_TRUE(query::boolean_and(idx, {"alpha", "missing"}, &got).ok());
    EXPECT_TRUE(got.empty());

    got = {99};
    ASSERT_TRUE(query::phrase_query(idx, {}, &got).ok());
    EXPECT_TRUE(got.empty());

    got = {99};
    ASSERT_TRUE(query::phrase_prefix_query(idx, {}, &got).ok());
    EXPECT_TRUE(got.empty());

    ASSERT_TRUE(query::prefix_query(idx, "", &got).ok());
    EXPECT_EQ(got, (std::vector<uint32_t> {0, 1, 2, 3, 4}));

    ASSERT_TRUE(query::phrase_query(idx, {"alpha"}, &got).ok());
    EXPECT_EQ(got, (std::vector<uint32_t> {0, 1, 2}));

    ASSERT_TRUE(query::phrase_prefix_query(idx, {"alpha"}, &got).ok());
    EXPECT_EQ(got, (std::vector<uint32_t> {0, 1, 2, 4}));

    ASSERT_TRUE(query::wildcard_query(idx, "", &got).ok());
    EXPECT_TRUE(got.empty());

    EXPECT_TRUE(query::regexp_query(idx, "[", &got).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

TEST(SniiQueryOperatorBoundaries, PositionQueriesRejectDocsOnlyIndex) {
    std::vector<uint8_t> bytes;
    BuildIndexBytes(Corpus {}, doris::snii::format::IndexConfig::kDocsOnly, &bytes);
    FaultInjectingReader file(std::move(bytes));
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment);

    std::vector<uint32_t> got;
    EXPECT_TRUE(query::phrase_query(idx, {"alpha", "beta"}, &got)
                        .is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
    EXPECT_TRUE(query::phrase_prefix_query(idx, {"alpha", "b"}, &got)
                        .is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>());
}

TEST(SniiQueryOperatorIoErrors, PropagateUnderlyingReadFailures) {
    EnvGuard dict_resident("SNII_DICT_RESIDENT_MAX");
    ::setenv("SNII_DICT_RESIDENT_MAX", "0", 1);

    std::vector<uint8_t> bytes;
    BuildIndexBytes(Corpus {}, doris::snii::format::IndexConfig::kDocsPositionsScoring, &bytes);
    FaultInjectingReader file(std::move(bytes));
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment);
    file.set_fail_reads(true);

    std::vector<uint32_t> got;
    ExpectIoError(query::term_query(idx, "alpha", &got));
    ExpectIoError(query::boolean_or(idx, {"alpha", "beta"}, &got));
    ExpectIoError(query::boolean_and(idx, {"alpha", "beta"}, &got));
    ExpectIoError(query::prefix_query(idx, "al", &got));
    ExpectIoError(query::wildcard_query(idx, "alpha*", &got));
    ExpectIoError(query::regexp_query(idx, "alpha.*", &got));
    ExpectIoError(query::phrase_query(idx, {"alpha", "beta"}, &got));
    ExpectIoError(query::phrase_prefix_query(idx, {"alpha", "b"}, &got));
}
