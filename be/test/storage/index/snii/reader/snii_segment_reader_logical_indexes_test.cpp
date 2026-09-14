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

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
namespace ErrorCode = doris::ErrorCode;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_logical_indexes_test_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

SniiIndexInput MakeInput(uint64_t index_id, const std::string& suffix, IndexConfig config) {
    SpimiTermBuffer buf(/*has_positions=*/has_positions(config));
    const std::vector<std::vector<std::string>> docs = {
            {"alpha", "bravo"}, {"bravo", "charlie"}, {"alpha", "charlie", "delta"}};
    for (uint32_t doc = 0; doc < docs.size(); ++doc) {
        for (uint32_t pos = 0; pos < docs[doc].size(); ++pos) {
            buf.add_token(docs[doc][pos], doc, pos);
        }
    }
    SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = suffix;
    input.config = config;
    input.doc_count = static_cast<uint32_t>(docs.size());
    input.terms = buf.finalize_sorted();
    input.target_dict_block_bytes = 256;
    return input;
}

class SniiSegmentReaderLogicalIndexesTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = TempPath();
        {
            io::LocalFileWriter writer;
            ASSERT_TRUE(writer.open(path_).ok());
            SniiCompoundWriter compound(&writer);
            ASSERT_TRUE(compound.add_logical_index(MakeInput(7, "", IndexConfig::kDocsOnly)).ok());
            ASSERT_TRUE(
                    compound.add_logical_index(MakeInput(9, "body", IndexConfig::kDocsPositions))
                            .ok());
            ASSERT_TRUE(compound.finish().ok());
        }
        ASSERT_TRUE(file_.open(path_).ok());
        ASSERT_TRUE(SniiSegmentReader::open(&file_, &segment_).ok());
    }

    void TearDown() override { std::remove(path_.c_str()); }

    std::string path_;
    io::LocalFileReader file_;
    SniiSegmentReader segment_;
};

} // namespace

TEST_F(SniiSegmentReaderLogicalIndexesTest, ListsEveryLogicalIndex) {
    const std::vector<LogicalIndexMetadataRef>& entries = segment_.logical_indexes();
    ASSERT_EQ(2U, entries.size());
    bool saw_docs_only = false;
    bool saw_positions = false;
    for (const LogicalIndexMetadataRef& entry : entries) {
        EXPECT_EQ(LogicalIndexKind::kInverted, entry.kind);
        EXPECT_GT(entry.core_metadata.length, 0U);
        if (entry.index_id == 7) {
            EXPECT_EQ("", entry.index_suffix);
            saw_docs_only = true;
        } else if (entry.index_id == 9) {
            EXPECT_EQ("body", entry.index_suffix);
            saw_positions = true;
        }
    }
    EXPECT_TRUE(saw_docs_only);
    EXPECT_TRUE(saw_positions);
}

TEST_F(SniiSegmentReaderLogicalIndexesTest, ReadsCoreMetadataOfEachIndex) {
    CoreMetadata docs_only;
    ASSERT_TRUE(segment_.core_metadata_for_index(7, "", &docs_only).ok());
    EXPECT_EQ(IndexConfig::kDocsOnly, docs_only.index_config);
    EXPECT_GT(docs_only.section_refs.dict_region.length, 0U);
    EXPECT_EQ(3U, docs_only.stats.doc_count);

    // This tiny corpus inlines every posting into dictionary blocks, so only the dictionary
    // region is guaranteed to be non-empty.
    CoreMetadata positions;
    ASSERT_TRUE(segment_.core_metadata_for_index(9, "body", &positions).ok());
    EXPECT_EQ(IndexConfig::kDocsPositions, positions.index_config);
    EXPECT_GT(positions.section_refs.dict_region.length, 0U);
    EXPECT_EQ(3U, positions.stats.doc_count);

    SectionRefs refs;
    ASSERT_TRUE(segment_.section_refs_for_index(9, "body", &refs).ok());
    EXPECT_EQ(positions.section_refs.dict_region.offset, refs.dict_region.offset);
    EXPECT_EQ(positions.section_refs.dict_region.length, refs.dict_region.length);
}

TEST_F(SniiSegmentReaderLogicalIndexesTest, MissingIndexIsNotFound) {
    CoreMetadata core;
    const doris::Status status = segment_.core_metadata_for_index(8, "", &core);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>()) << status;
}
