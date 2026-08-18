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

#include "exec/sink/writer/paimon/paimon_table_writer.h"

#include <gtest/gtest.h>

#include <memory>

#include "runtime/runtime_state.h"

namespace doris {
namespace {

class FakePaimonWriter final : public IPaimonWriter {
public:
    explicit FakePaimonWriter(int* abort_count) : _abort_count(abort_count) {}

    Status write(RuntimeState*, Block&) override { return Status::OK(); }
    Status prepare_commit(std::vector<TPaimonCommitMessage>&) override { return Status::OK(); }
    Status abort() override {
        ++*_abort_count;
        return Status::OK();
    }

private:
    int* _abort_count;
};

class FakePaimonBackend final : public IPaimonWriteBackend {
public:
    explicit FakePaimonBackend(int* close_count, Status prepare_close_status = Status::OK())
            : _close_count(close_count), _prepare_close_status(std::move(prepare_close_status)) {}

    Status open(const TPaimonTableSink&, RuntimeState*, RuntimeProfile*) override {
        return Status::OK();
    }
    Status create_writer(std::unique_ptr<IPaimonWriter>*) override { return Status::OK(); }
    Status prepare_close_for_commit() override { return _prepare_close_status; }
    Status close() override {
        ++*_close_count;
        return Status::OK();
    }
    PaimonBackendType type() const override { return PaimonBackendType::JNI; }

private:
    int* _close_count;
    Status _prepare_close_status;
};

} // namespace

TEST(PaimonPreparedCommitOwnerTest, RejectedReportAbortsAndClosesBackend) {
    int abort_count = 0;
    int close_count = 0;
    {
        PaimonPreparedCommitOwner owner(std::make_unique<FakePaimonWriter>(&abort_count),
                                        std::make_unique<FakePaimonBackend>(&close_count));
        owner.finalize(ExternalFileReportOutcome::REJECTED);
    }
    EXPECT_EQ(1, abort_count);
    EXPECT_EQ(1, close_count);
}

TEST(PaimonPreparedCommitOwnerTest, AcknowledgedReportClosesWithoutAbort) {
    int abort_count = 0;
    int close_count = 0;
    {
        PaimonPreparedCommitOwner owner(std::make_unique<FakePaimonWriter>(&abort_count),
                                        std::make_unique<FakePaimonBackend>(&close_count));
        owner.finalize(ExternalFileReportOutcome::ACKNOWLEDGED);
    }
    EXPECT_EQ(0, abort_count);
    EXPECT_EQ(1, close_count);
}

TEST(PaimonPreparedCommitOwnerTest, AmbiguousReportRetainsOwnerUntilAcknowledged) {
    int abort_count = 0;
    int close_count = 0;
    {
        PaimonPreparedCommitOwner owner(std::make_unique<FakePaimonWriter>(&abort_count),
                                        std::make_unique<FakePaimonBackend>(&close_count));
        owner.finalize(ExternalFileReportOutcome::AMBIGUOUS);
        EXPECT_EQ(0, abort_count);
        EXPECT_EQ(0, close_count);
        owner.finalize(ExternalFileReportOutcome::ACKNOWLEDGED);
    }
    EXPECT_EQ(0, abort_count);
    EXPECT_EQ(1, close_count);
}

TEST(PaimonPreparedCommitOwnerTest, FailedSdkShutdownRejectsCommitBeforeAcknowledgement) {
    int abort_count = 0;
    int close_count = 0;
    PaimonPreparedCommitOwner owner(
            std::make_unique<FakePaimonWriter>(&abort_count),
            std::make_unique<FakePaimonBackend>(
                    &close_count, Status::InternalError("injected SDK shutdown failure")));

    Status status = owner.prepare_for_report();
    owner.finalize(ExternalFileReportOutcome::REJECTED);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(1, abort_count);
    EXPECT_EQ(1, close_count);
}

TEST(PaimonTableWriterTest, RejectsCoordinatorWithoutExternalFileReportAck) {
    TPaimonTableSink paimon_sink;
    TDataSink sink;
    sink.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
    sink.__set_paimon_table_sink(paimon_sink);
    PaimonTableWriter writer(std::move(sink), {}, nullptr, nullptr);
    RuntimeState state;
    RuntimeProfile profile("test");

    Status status = writer.open(&state, &profile);

    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_NE(std::string::npos, status.to_string().find("acknowledges external-file reports"));
}

} // namespace doris
