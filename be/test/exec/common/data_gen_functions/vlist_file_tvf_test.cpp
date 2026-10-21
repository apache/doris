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

#include "exec/common/data_gen_functions/vlist_file_tvf.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>

#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "exec/operator/datagen_operator.h"
#include "exec/operator/operator_helper.h"
#include "exprs/function/function_to_file.h"
#include "io/fs/s3_file_system.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_query_context.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
using namespace Aws::S3::Model;

class ListFileS3Client : public Aws::S3::S3Client {
public:
    MOCK_METHOD(HeadObjectOutcome, HeadObject, (const HeadObjectRequest&), (const, override));
    MOCK_METHOD(ListObjectsV2Outcome, ListObjectsV2, (const ListObjectsV2Request&),
                (const, override));
};

namespace {
ListObjectsV2Result page(std::initializer_list<std::pair<std::string, int64_t>> objects,
                         const std::string& token = "") {
    ListObjectsV2Result result;
    result.SetIsTruncated(!token.empty());
    result.SetNextContinuationToken(token);
    for (const auto& [key, size] : objects) {
        Object object;
        object.SetKey(key);
        object.SetSize(size);
        result.AddContents(object);
    }
    return result;
}

std::vector<TScanRangeParams> ranges(std::string uri = "s3://bucket/dir", bool recursive = false) {
    TTVFListFileScanRange params;
    params.resource.properties = {{"AWS_BUCKET", "bucket"}};
    params.uri = std::move(uri);
    params.recursive = recursive;
    TScanRangeParams range;
    range.scan_range.data_gen_scan_range.__set_list_file_params(params);
    return {range};
}

class ListFileTVFTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }

    void SetUp() override {
        descriptor = std::make_unique<MockRowDescriptor>(
                std::vector<DataTypePtr> {std::make_shared<DataTypeString>(),
                                          make_nullable(std::make_shared<DataTypeFile>())},
                &pool);
        const std::array<std::string, 2> names {"path", "file"};
        for (size_t i = 0; i < names.size(); ++i) {
            descriptor->tuple_desc_map[0]->slots()[i]->_col_name = names[i];
        }
        function = std::make_unique<VListFileTVF>(0, descriptor->tuple_desc_map[0]);
        // Replace only remote I/O; parsing, paging, FILE construction and batching stay real.
        s3 = std::make_shared<testing::StrictMock<ListFileS3Client>>();
        auto filesystem = std::shared_ptr<io::S3FileSystem>(
                new io::S3FileSystem(S3Conf {}, "list-file-test"));
        filesystem->_client = std::make_shared<io::ObjClientHolder>(S3ClientConf {});
        filesystem->_client->_client = std::make_shared<S3ObjStorageClient>(s3);
        filesystem->_bucket = "bucket";
        function->_filesystem = filesystem;
    }

    File row(const Block& block, size_t index) {
        return (*block.get_by_position(1).column)[index].get<TYPE_FILE>();
    }

    void check_projection(const std::vector<size_t>& positions) {
        std::vector<DataTypePtr> types;
        for (const auto position : positions) {
            types.push_back(descriptor->tuple_desc_map[0]->slots()[position]->type());
        }
        MockRowDescriptor projected(types, &pool);
        for (size_t i = 0; i < positions.size(); ++i) {
            projected.tuple_desc_map[0]->slots()[i]->_col_name =
                    descriptor->tuple_desc_map[0]->slots()[positions[i]]->col_name();
        }
        function->set_tuple_desc(projected.tuple_desc_map[0]);
        ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
        EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto&) {
            return ListObjectsV2Outcome(page({{"dir/a.txt", 7}}));
        });
        Block block;
        bool eos = false;
        ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
        ASSERT_EQ(block.rows(), 1);
        ASSERT_EQ(block.columns(), positions.size());
        EXPECT_TRUE(eos);
        for (size_t i = 0; i < positions.size(); ++i) {
            const auto value = (*block.get_by_position(i).column)[0];
            switch (positions[i]) {
            case 0:
                EXPECT_EQ(value.get<TYPE_STRING>(), "s3://bucket/dir/a.txt");
                break;
            case 1:
                EXPECT_EQ(value.get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://bucket/dir/a.txt");
                EXPECT_EQ(value.get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 7);
                break;
            }
        }
    }

    void expect_etag_listing(const std::array<std::optional<std::string>, 5>& etags) {
        EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([etags](const auto& request) {
            EXPECT_EQ(request.GetBucket(), "bucket");
            EXPECT_EQ(request.GetPrefix(), "dir/");
            ListObjectsV2Result result;
            for (size_t i = 0; i < etags.size(); ++i) {
                Object object;
                object.SetKey("dir/" + std::to_string(i));
                object.SetSize(7);
                if (etags[i]) {
                    object.SetETag(*etags[i]);
                }
                result.AddContents(std::move(object));
            }
            return ListObjectsV2Outcome(result);
        });
    }

    void expect_etag_head(size_t index, const std::optional<std::string>& etag) {
        EXPECT_CALL(*s3, HeadObject(testing::_)).WillOnce([index, etag](const auto& request) {
            EXPECT_EQ(request.GetBucket(), "bucket");
            EXPECT_EQ(request.GetKey(), "dir/" + std::to_string(index));
            EXPECT_FALSE(request.ChecksumModeHasBeenSet());
            HeadObjectResult result;
            result.SetContentLength(7);
            if (etag) {
                result.SetETag(*etag);
            }
            return HeadObjectOutcome(result);
        });
    }

    void check_etag_consistency(const Block& block, size_t index) {
        const auto value = row(block, index);
        io::FileStat metadata;
        ASSERT_TRUE(function->_filesystem->stat(value[0].get<TYPE_STRING>(), &metadata).ok());
        if (index < 3) {
            EXPECT_TRUE(value[4].is_null());
            EXPECT_FALSE(metadata.checksum.has_value());
            return;
        }
        ASSERT_FALSE(value[4].is_null());
        EXPECT_EQ(value[4].get<TYPE_STRING>(), "ETAG:MiXeD:opaque-2");
        EXPECT_EQ(metadata.checksum, value[4].get<TYPE_STRING>());
    }

    ObjectPool pool;
    MockRuntimeState state;
    std::unique_ptr<MockRowDescriptor> descriptor;
    std::unique_ptr<VListFileTVF> function;
    std::shared_ptr<testing::StrictMock<ListFileS3Client>> s3;
};

TEST_F(ListFileTVFTest, EmitsCompleteFilesAndSkipsDirectoryMarkers) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetBucket(), "bucket");
        EXPECT_EQ(request.GetPrefix(), "dir/");
        EXPECT_EQ(request.GetDelimiter(), "/");
        EXPECT_FALSE(request.ContinuationTokenHasBeenSet());
        return ListObjectsV2Outcome(
                page({{"dir/", 0}, {"dir/empty.txt", 0}, {"dir/image.PNG", 27}, {"dir/raw", 4}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 3);
    EXPECT_TRUE(eos);
    EXPECT_TRUE(block.get_by_position(1).type->is_nullable());
    EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_STRING>(),
              "s3://bucket/dir/empty.txt");
    const auto empty = row(block, 0);
    EXPECT_EQ(empty[0].get<TYPE_STRING>(), "s3://bucket/dir/empty.txt");
    EXPECT_TRUE(empty[1].is_null());
    EXPECT_EQ(empty[2].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ(empty[3].get<TYPE_STRING>(), "text/plain");
    EXPECT_TRUE(empty[4].is_null());
    EXPECT_TRUE(empty[5].is_null());
    EXPECT_EQ(row(block, 1)[3].get<TYPE_STRING>(), "image/png");
    EXPECT_EQ(row(block, 2)[3].get<TYPE_STRING>(), "application/octet-stream");
}

TEST_F(ListFileTVFTest, EmitsListingEtagsWithoutHeadRequests) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto&) {
        ListObjectsV2Result result;
        for (const auto& etag :
             {std::optional<std::string>("\"opaque-2\""), std::optional<std::string>(""),
              std::optional<std::string>(), std::optional<std::string>("\"\""),
              std::optional<std::string>("opaque-2")}) {
            Object object;
            object.SetKey("dir/" + std::to_string(result.GetContents().size()));
            object.SetSize(7);
            if (etag) {
                object.SetETag(*etag);
            }
            result.AddContents(std::move(object));
        }
        return ListObjectsV2Outcome(result);
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 5);
    EXPECT_TRUE(eos);
    ASSERT_FALSE(row(block, 0)[4].is_null());
    EXPECT_EQ(row(block, 0)[4].get<TYPE_STRING>(), "ETAG:opaque-2");
    EXPECT_TRUE(row(block, 1)[4].is_null());
    EXPECT_TRUE(row(block, 2)[4].is_null());
    EXPECT_TRUE(row(block, 3)[4].is_null());
    EXPECT_EQ(row(block, 4)[4].get<TYPE_STRING>(), "ETAG:opaque-2");
}

TEST_F(ListFileTVFTest, ListingAndHeadProduceTheSameFileChecksum) {
    const std::array<std::optional<std::string>, 5> etags {std::nullopt, "", "\"\"",
                                                           "MiXeD:opaque-2", "\"MiXeD:opaque-2\""};
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    expect_etag_listing(etags);
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), etags.size());
    EXPECT_TRUE(eos);
    for (size_t i = 0; i < etags.size(); ++i) {
        expect_etag_head(i, etags[i]);
        check_etag_consistency(block, i);
    }
}

TEST_F(ListFileTVFTest, SplitsPagesIntoBatchesWithoutPrefetching) {
    state._batch_size = 2;
    ASSERT_TRUE(function->set_scan_ranges(ranges("s3://bucket/dir/", true)).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetPrefix(), "dir/");
        EXPECT_FALSE(request.DelimiterHasBeenSet());
        return ListObjectsV2Outcome(
                page({{"dir/a.txt", 1}, {"dir/b.txt", 2}, {"dir/deep/c.txt", 3}}, "next"));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 2);
    EXPECT_FALSE(eos);
    auto previous = block.get_by_position(1).column;
    block.clear_column_data();
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_FALSE(eos);
    EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), "s3://bucket/dir/deep/c.txt");
    EXPECT_EQ(previous->size(), 2);
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetContinuationToken(), "next");
        return ListObjectsV2Outcome(page({{"dir/d.txt", 4}}));
    });
    block.clear_column_data();
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 1);
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, ContinuesPastEmptyAndMarkerOnlyIntermediatePages) {
    ASSERT_TRUE(function->set_scan_ranges(ranges("s3://bucket/dir/", true)).ok());
    testing::InSequence sequence;
    EXPECT_CALL(*s3, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(page({}, "empty"))));
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetContinuationToken(), "empty");
        return ListObjectsV2Outcome(page({{"dir/subdir/", 0}}, "marker"));
    });
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetContinuationToken(), "marker");
        return ListObjectsV2Outcome(page({{"dir/file.csv", 8}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 1);
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, ReusesOutputColumnsAcrossPages) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(page({{"dir/a.txt", 1}}, "next"))))
            .WillOnce(testing::Return(ListObjectsV2Outcome(page({{"dir/b.txt", 2}}))));
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto* path_column = block.get_by_position(0).column.get();
    const auto* file_column = block.get_by_position(1).column.get();
    block.clear_column_data();
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.get_by_position(0).column.get(), path_column);
    EXPECT_EQ(block.get_by_position(1).column.get(), file_column);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), "s3://bucket/dir/b.txt");
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, RestoresReusedColumnsAfterListingError) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(page({{"dir/a.txt", 1}}, "next"))))
            .WillOnce([](const auto&) {
                Aws::S3::S3Error error;
                error.SetMessage("listing failed");
                return ListObjectsV2Outcome(error);
            });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    const auto* path_column = block.get_by_position(0).column.get();
    const auto* file_column = block.get_by_position(1).column.get();
    block.clear_column_data();
    EXPECT_FALSE(function->get_next(&state, &block, &eos).ok());
    ASSERT_NE(block.get_by_position(0).column.get(), nullptr);
    ASSERT_NE(block.get_by_position(1).column.get(), nullptr);
    EXPECT_EQ(block.get_by_position(0).column.get(), path_column);
    EXPECT_EQ(block.get_by_position(1).column.get(), file_column);
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(ListFileTVFTest, UsesUriBucketWithoutResourceBucketConstraint) {
    for (const auto& properties : {std::map<std::string, std::string> {},
                                   std::map<std::string, std::string> {{"AWS_BUCKET", "other"}}}) {
        auto parameters = ranges();
        parameters[0].scan_range.data_gen_scan_range.list_file_params.resource.properties =
                properties;
        ASSERT_TRUE(function->set_scan_ranges(parameters).ok());
        EXPECT_EQ(function->_path.bucket, "bucket");
    }
}

TEST_F(ListFileTVFTest, DirectoryPathCanContainAnotherSchemeDelimiter) {
    ASSERT_TRUE(function->set_scan_ranges(ranges("s3://bucket/http://dir/")).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetPrefix(), "http://dir/");
        return ListObjectsV2Outcome(page({{"http://dir/a.txt", 1}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), "s3://bucket/http://dir/a.txt");
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, HttpUrisUseParsedBucketAndCompleteDirectoryKey) {
    for (const auto* scheme : {"http://", "https://"}) {
        auto listing = std::make_unique<VListFileTVF>(0, descriptor->tuple_desc_map[0]);
        listing->_filesystem = function->_filesystem;
        ASSERT_TRUE(listing->set_scan_ranges(ranges(std::string(scheme) +
                                                    "host/bucket/dir?part#raw://nested"))
                            .ok());
        EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
            EXPECT_EQ(request.GetBucket(), "bucket");
            EXPECT_EQ(request.GetPrefix(), "dir?part#raw://nested/");
            EXPECT_EQ(request.GetDelimiter(), "/");
            return ListObjectsV2Outcome(page({{"dir?part#raw://nested/a.jpg", 7}}));
        });
        Block block;
        bool eos = false;
        ASSERT_TRUE(listing->get_next(&state, &block, &eos).ok());
        ASSERT_EQ(block.rows(), 1);
        EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_STRING>(),
                  "s3://bucket/dir?part#raw://nested/a.jpg");
        EXPECT_EQ(row(block, 0)[3].get<TYPE_STRING>(), "image/jpeg");
        EXPECT_TRUE(eos);
    }
}

TEST_F(ListFileTVFTest, ObjectKeysAndDirectoryPrefixRemainRaw) {
    ASSERT_TRUE(function->set_scan_ranges(ranges("s3://bucket/dir%20%25%3F%23%E4%B8%AD+")).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetPrefix(), "dir%20%25%3F%23%E4%B8%AD+/");
        return ListObjectsV2Outcome(page({{"dir%20%25%3F%23%E4%B8%AD+/file %?#中+.txt", 3}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    const std::string expected = "s3://bucket/dir%20%25%3F%23%E4%B8%AD+/file %?#中+.txt";
    EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_STRING>(), expected);
    EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), expected);
    EXPECT_EQ(row(block, 0)[3].get<TYPE_STRING>(), "text/plain");
}

TEST_F(ListFileTVFTest, AcceptsCanonicalS3BucketProperty) {
    auto parameters = ranges();
    auto& properties =
            parameters[0].scan_range.data_gen_scan_range.list_file_params.resource.properties;
    properties = {{"s3.bucket", "bucket"}};
    ASSERT_TRUE(function->set_scan_ranges(parameters).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetBucket(), "bucket");
        return ListObjectsV2Outcome(page({{"dir/a.csv", 1}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 1);
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, BucketRootFormsListWithoutPrefix) {
    for (const auto* uri : {"s3://bucket", "s3://bucket/", "http://host/bucket",
                            "http://host/bucket/", "https://host/bucket", "https://host/bucket/"}) {
        auto listing = std::make_unique<VListFileTVF>(0, descriptor->tuple_desc_map[0]);
        listing->_filesystem = function->_filesystem;
        ASSERT_TRUE(listing->set_scan_ranges(ranges(uri)).ok()) << uri;
        EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
            EXPECT_TRUE(request.GetPrefix().empty());
            EXPECT_EQ(request.GetDelimiter(), "/");
            return ListObjectsV2Outcome(page({{"root.csv", 2}}));
        });
        Block block;
        bool eos = false;
        ASSERT_TRUE(listing->get_next(&state, &block, &eos).ok());
        EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), "s3://bucket/root.csv");
        EXPECT_TRUE(eos);
    }
}

TEST_F(ListFileTVFTest, ExtraLeadingSlashIsPartOfDirectoryKey) {
    ASSERT_TRUE(function->set_scan_ranges(ranges("s3://bucket//")).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto& request) {
        EXPECT_EQ(request.GetPrefix(), "/");
        return ListObjectsV2Outcome(page({{"/a.csv", 1}}));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(row(block, 0)[0].get<TYPE_STRING>(), "s3://bucket//a.csv");
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, EmptyListingReturnsEmptyFileColumn) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(page({}))));
    Block block;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).type->get_name(), "String");
    EXPECT_EQ(block.get_by_position(1).type->get_name(), "File");
    EXPECT_TRUE(eos);
}

class ToFileWithListedFilesystem : public FunctionToFile {
public:
    explicit ToFileWithListedFilesystem(std::shared_ptr<io::FileSystem> filesystem)
            : _filesystem(std::move(filesystem)) {}
    Status create_filesystem(const TS3Resource&, const std::string&,
                             std::shared_ptr<io::FileSystem>* result) const override {
        *result = _filesystem;
        return Status::OK();
    }
    std::shared_ptr<io::FileSystem> _filesystem;
};

TEST_F(ListFileTVFTest, RawPathsRoundTripThroughToFileAndS3Head) {
    ToFileWithListedFilesystem to_file(function->_filesystem);
    const std::array<std::string, 9> keys {
            "dir/a://b.txt",     "dir/a?b.txt",         "dir/a#b.txt",
            "dir/ a.txt ",       "dir/中文+space .txt", "dir/a%20b.txt",
            "dir/a?b#c://d.txt", "dir/photo?part.jpg",  "dir/photo#part.PNG"};
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([&](const auto&) {
        ListObjectsV2Result result;
        for (const auto& key : keys) {
            Aws::S3::Model::Object object;
            object.SetKey(key);
            object.SetSize(7);
            result.AddContents(std::move(object));
        }
        return ListObjectsV2Outcome(result);
    });
    Block listed;
    bool eos = false;
    ASSERT_TRUE(function->get_next(&state, &listed, &eos).ok());
    ASSERT_EQ(listed.rows(), keys.size());
    auto text = std::make_shared<DataTypeString>();
    auto query = MockQueryContext::create();
    query->set_s3_resources({{"resource", TS3Resource {}}});
    state._query_ctx = query.get();
    auto resource =
            text->create_column_const(listed.rows(), Field::create_field<TYPE_STRING>("resource"));
    auto context =
            FunctionContext::create_context(&state, std::make_shared<DataTypeFile>(), {text, text});
    context->set_constant_cols({std::make_shared<ColumnPtrWrapper>(resource), nullptr});
    ASSERT_TRUE(to_file.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    size_t head_index = 0;
    EXPECT_CALL(*s3, HeadObject(testing::_))
            .Times(keys.size())
            .WillRepeatedly([&](const auto& request) {
                EXPECT_EQ(request.GetBucket(), "bucket");
                EXPECT_EQ(request.GetKey(), keys[head_index++]);
                HeadObjectResult result;
                result.SetContentLength(7);
                return HeadObjectOutcome(result);
            });
    Block output {{resource, text, "resource"},
                  listed.get_by_position(0),
                  {nullptr, std::make_shared<DataTypeFile>(), "result"}};
    ASSERT_TRUE(to_file.execute(context.get(), output, {0, 1}, 2, listed.rows()).ok());
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto value = (*output.get_by_position(2).column)[i].get<TYPE_FILE>();
        EXPECT_EQ(value[0].get<TYPE_STRING>(), "s3://bucket/" + keys[i]);
        EXPECT_EQ(value[3], row(listed, i)[3]) << keys[i];
    }
    EXPECT_EQ(row(listed, 7)[3].get<TYPE_STRING>(), "image/jpeg");
    EXPECT_EQ(row(listed, 8)[3].get<TYPE_STRING>(), "image/png");
    ASSERT_TRUE(to_file.close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    state._query_ctx = nullptr;
}

TEST_F(ListFileTVFTest, CancellationBeforeIoDoesNotFetchPage) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    state.cancel(Status::Cancelled("cancel before list"));
    Block block;
    bool eos = false;
    EXPECT_FALSE(function->get_next(&state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(ListFileTVFTest, CancellationAfterEmptyPageStopsBeforeNextRequest) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([&](const auto&) {
        state.cancel(Status::Cancelled("cancel during list"));
        return ListObjectsV2Outcome(page({}, "next"));
    });
    Block block;
    bool eos = false;
    const auto status = function->get_next(&state, &block, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(ListFileTVFTest, PropagatesListingErrors) {
    ASSERT_TRUE(function->set_scan_ranges(ranges()).ok());
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto&) {
        Aws::S3::S3Error error;
        error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
        error.SetMessage("listing denied");
        return ListObjectsV2Outcome(error);
    });
    Block block;
    bool eos = false;
    const auto status = function->get_next(&state, &block, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("listing denied"), std::string::npos);
}

TEST_F(ListFileTVFTest, ListingUsesBlockingSchedulerAndNumbersKeepsDefault) {
    DataGenSourceOperatorX listing;
    TPlanNode node;
    node.node_type = TPlanNodeType::DATA_GEN_SCAN_NODE;
    node.data_gen_scan_node.func_name = TDataGenFunctionName::LIST_FILE;
    ASSERT_TRUE(listing.init(node, &state).ok());
    EXPECT_TRUE(listing._blockable);
    DataGenSourceOperatorX numbers;
    node.data_gen_scan_node.func_name = TDataGenFunctionName::NUMBERS;
    ASSERT_TRUE(numbers.init(node, &state).ok());
    EXPECT_FALSE(numbers._blockable);
}

TEST_F(ListFileTVFTest, ZeroLimitDoesNotFetchAnyPage) {
    OperatorContext context;
    DataGenSourceOperatorX op;
    op._tuple_id = 0;
    op._tuple_desc = descriptor->tuple_desc_map[0];
    op._function_name = TDataGenFunctionName::LIST_FILE;
    op._limit = 0;
    OperatorHelper::init_local_state(context, op, ranges());
    auto& local = op.get_local_state(&context.state);
    static_cast<VListFileTVF*>(local._table_func.get())->_filesystem = function->_filesystem;
    Block block;
    bool eos = false;
    ASSERT_TRUE(op.get_block(&context.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_EQ(block.columns(), 2);
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, LimitStopsBeforeFetchingFollowingPage) {
    OperatorContext context;
    DataGenSourceOperatorX op;
    op._tuple_id = 0;
    op._tuple_desc = descriptor->tuple_desc_map[0];
    op._function_name = TDataGenFunctionName::LIST_FILE;
    op._limit = 1;
    OperatorHelper::init_local_state(context, op, ranges());
    auto& local = op.get_local_state(&context.state);
    static_cast<VListFileTVF*>(local._table_func.get())->_filesystem = function->_filesystem;
    EXPECT_CALL(*s3, ListObjectsV2(testing::_)).WillOnce([](const auto&) {
        return ListObjectsV2Outcome(page({{"dir/a.txt", 1}, {"dir/b.txt", 2}}, "next"));
    });
    Block block;
    bool eos = false;
    ASSERT_TRUE(op.get_block(&context.state, &block, &eos).ok());
    EXPECT_EQ(block.rows(), 1);
    EXPECT_TRUE(eos);
}

TEST_F(ListFileTVFTest, PrunedPathProjection) {
    check_projection({0});
}

TEST_F(ListFileTVFTest, PrunedFileProjection) {
    check_projection({1});
}

TEST_F(ListFileTVFTest, ReorderedPrunedProjection) {
    check_projection({1, 0});
}

} // namespace
} // namespace doris
