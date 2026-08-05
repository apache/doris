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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/sink/writer/vhive_partition_writer.h"
#include "format/transformer/vfile_format_transformer.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/exec_env.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class RecordingObjStorageClient final : public io::ObjStorageClient {
public:
    io::ObjectStorageUploadResponse create_multipart_upload(
            const io::ObjectStoragePathOptions&) override {
        return {.resp = io::ObjectStorageResponse::OK(), .upload_id = "upload-id"};
    }

    io::ObjectStorageResponse put_object(const io::ObjectStoragePathOptions&,
                                         std::string_view) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageUploadResponse upload_part(const io::ObjectStoragePathOptions&,
                                                std::string_view, int) override {
        return {.resp = io::ObjectStorageResponse::OK(), .etag = "etag"};
    }

    io::ObjectStorageResponse complete_multipart_upload(
            const io::ObjectStoragePathOptions&,
            const std::vector<io::ObjectCompleteMultiPart>&) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse abort_multipart_upload(
            const io::ObjectStoragePathOptions& opts) override {
        ++abort_count;
        aborted_upload_id = opts.upload_id.value_or("");
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageHeadResponse head_object(const io::ObjectStoragePathOptions&) override {
        return {.resp = io::ObjectStorageResponse::OK(), .file_size = 0};
    }

    io::ObjectStorageResponse get_object(const io::ObjectStoragePathOptions&, void*, size_t, size_t,
                                         size_t*) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse list_objects(const io::ObjectStoragePathOptions&,
                                           std::vector<io::FileInfo>*) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects(const io::ObjectStoragePathOptions&,
                                             std::vector<std::string>) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_object(const io::ObjectStoragePathOptions&) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects_recursively(
            const io::ObjectStoragePathOptions&) override {
        return io::ObjectStorageResponse::OK();
    }

    std::string generate_presigned_url(const io::ObjectStoragePathOptions&, int64_t,
                                       const S3ClientConf&) override {
        return {};
    }

    int abort_count = 0;
    std::string aborted_upload_id;
};

class FixedLengthTransformer final : public VFileFormatTransformer {
public:
    explicit FixedLengthTransformer(const VExprContextSPtrs& output_exprs)
            : VFileFormatTransformer(nullptr, output_exprs, false) {}

    Status open() override { return Status::OK(); }
    Status write(const Block&) override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    int64_t written_len() override { return 64; }
};

std::unique_ptr<VHivePartitionWriter> create_closed_hive_writer(
        RuntimeState* state, const VExprContextSPtrs& output_exprs,
        const std::shared_ptr<RecordingObjStorageClient>& client) {
    THiveTableSink hive_sink;
    TDataSink sink;
    sink.__set_type(TDataSinkType::HIVE_TABLE_SINK);
    sink.__set_hive_table_sink(hive_sink);
    VHivePartitionWriter::WriteInfo write_info {.write_path = "s3://bucket/staging",
                                                .original_write_path = "s3://bucket/table",
                                                .target_path = "s3://bucket/table",
                                                .file_type = TFileType::FILE_S3,
                                                .broker_addresses = {}};
    static const std::map<std::string, std::string> hadoop_conf;
    auto writer = std::make_unique<VHivePartitionWriter>(
            sink, "", TUpdateMode::APPEND, output_exprs, std::vector<std::string> {},
            std::move(write_info), "part", 0, TFileFormatType::FORMAT_PARQUET,
            TFileCompressType::PLAIN, nullptr, hadoop_conf);

    auto holder = std::make_shared<io::ObjClientHolder>(S3ClientConf {});
    holder->_client = client;
    io::FileWriterOptions options {.used_by_s3_committer = true};
    auto file_writer =
            std::make_unique<io::S3FileWriter>(holder, "bucket", "table/part.parquet", &options);
    file_writer->_obj_storage_path_opts.upload_id = "upload-id";
    file_writer->_state = io::FileWriter::State::CLOSED;
    writer->_file_writer = std::move(file_writer);
    writer->_file_format_transformer = std::make_unique<FixedLengthTransformer>(output_exprs);
    writer->_state = state;
    EXPECT_TRUE(writer->close(Status::OK()).ok());
    return writer;
}

std::shared_ptr<PipelineFragmentContext> create_fragment_context(TUniqueId query_id) {
    auto query_ctx = MockQueryContext::create(query_id);
    return std::make_shared<PipelineFragmentContext>(query_id, TPipelineFragmentParams(), query_ctx,
                                                     ExecEnv::GetInstance(),
                                                     [](RuntimeState*, Status*) {});
}

ReportStatusRequest report_request(RuntimeState* state, bool done) {
    TNetworkAddress address;
    address.hostname = "external";
    return {.status = Status::OK(),
            .runtime_states = {},
            .done = done,
            .coord_addr = address,
            .query_id = TUniqueId(),
            .fragment_id = 0,
            .fragment_instance_id = TUniqueId(),
            .backend_num = 0,
            .runtime_state = state,
            .load_error_url = "",
            .first_error_msg = "",
            .cancel_fn = [](const Status&) {}};
}

} // namespace

TEST(VHivePartitionWriterReportLifecycleTest,
     PeriodicReportDefersMetadataAndRejectedFinalReportAbortsProviderUpload) {
    MockRuntimeState state;
    VExprContextSPtrs output_exprs;
    auto client = std::make_shared<RecordingObjStorageClient>();
    auto writer = create_closed_hive_writer(&state, output_exprs, client);
    auto context = create_fragment_context(TUniqueId());

    TReportExecStatusParams periodic_params;
    auto periodic_request = report_request(&state, false);
    context->_append_external_file_commit_data(periodic_request, &periodic_params);

    EXPECT_FALSE(periodic_params.__isset.hive_partition_updates);
    EXPECT_EQ(0, client->abort_count);

    TReportExecStatusParams final_params;
    auto final_request = report_request(&state, true);
    context->_append_external_file_commit_data(final_request, &final_params);
    ASSERT_TRUE(final_params.__isset.hive_partition_updates);
    ASSERT_EQ(1, final_params.hive_partition_updates.size());
    ASSERT_TRUE(final_params.hive_partition_updates[0].__isset.s3_mpu_pending_uploads);
    ASSERT_EQ(1, final_params.hive_partition_updates[0].s3_mpu_pending_uploads.size());
    const auto& pending_upload = final_params.hive_partition_updates[0].s3_mpu_pending_uploads[0];
    EXPECT_EQ("bucket", pending_upload.bucket);
    EXPECT_EQ("table/part.parquet", pending_upload.key);
    EXPECT_EQ("upload-id", pending_upload.upload_id);

    // A definite coordinator rejection must consume the same cleanup owner that was retained
    // while the periodic report deliberately withheld the pending-upload record.
    context->_coordinator_callback(final_request);

    EXPECT_EQ(1, client->abort_count);
    EXPECT_EQ("upload-id", client->aborted_upload_id);
}

} // namespace doris
