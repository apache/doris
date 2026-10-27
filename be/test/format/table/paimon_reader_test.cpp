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

#include "format/table/paimon_reader.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <string>

#include "common/status.h"
#include "format/format_common.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

namespace {

TFileScanRangeParams make_local_paimon_scan_params(TFileFormatType::type format_type) {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(format_type);
    return scan_params;
}

TFileRangeDesc make_paimon_range_with_deletion_file(const std::string& deletion_file_path) {
    TFileRangeDesc range;
    range.__set_path("data.parquet");
    range.__set_start_offset(0);
    range.__set_size(0);
    range.__set_fs_name("");
    range.__isset.table_format_params = true;
    range.table_format_params.__set_table_format_type("paimon");

    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path(deletion_file_path);
    deletion_file.__set_offset(0);
    deletion_file.__set_length(16);
    paimon_params.__set_deletion_file(deletion_file);
    range.table_format_params.__set_paimon_params(paimon_params);
    return range;
}

} // namespace

// A missing deletion vector must not report NOT_FOUND: FileScanner would take it for a missing data
// file and skip the whole range, surviving rows included.
TEST(PaimonDeletionVectorTest, V1ParquetReaderFailsOnMissingDeletionVectorFile) {
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("paimon_v1_parquet_dv_test");
    auto scan_params = make_local_paimon_scan_params(TFileFormatType::FORMAT_PARQUET);
    const auto range = make_paimon_range_with_deletion_file(
            "./be/test/exec/test_data/missing_paimon_v1_delete_vector.bin");
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    FileMetaCache meta_cache(1024);
    ShardedKVCache kv_cache(8);

    PaimonParquetReader reader(&profile, scan_params, range, 1024, &ctz, &kv_cache, &io_ctx,
                               &runtime_state, &meta_cache);
    const auto status = reader.TEST_init_deletion_vector();

    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status;
    EXPECT_NE(status.to_string().find(range.table_format_params.paimon_params.deletion_file.path),
              std::string::npos)
            << status;
}

TEST(PaimonDeletionVectorTest, V1OrcReaderFailsOnMissingDeletionVectorFile) {
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("paimon_v1_orc_dv_test");
    auto scan_params = make_local_paimon_scan_params(TFileFormatType::FORMAT_ORC);
    auto range = make_paimon_range_with_deletion_file(
            "./be/test/exec/test_data/missing_paimon_v1_orc_delete_vector.bin");
    range.table_format_params.paimon_params.__set_file_format("orc");
    io::IOContext io_ctx;
    FileMetaCache meta_cache(1024);
    ShardedKVCache kv_cache(8);

    PaimonOrcReader reader(&profile, &runtime_state, scan_params, range, 1024,
                           TimezoneUtils::default_time_zone, &kv_cache, &io_ctx, &meta_cache);
    const auto status = reader.TEST_init_deletion_vector();

    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status;
    EXPECT_NE(status.to_string().find(range.table_format_params.paimon_params.deletion_file.path),
              std::string::npos)
            << status;
}

} // namespace doris
