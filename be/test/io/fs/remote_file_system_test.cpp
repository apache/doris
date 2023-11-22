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

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#include <stdint.h>

#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_builder.h"
#include "runtime/exec_env.h"
#include "util/jni-util.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

#ifndef CHECK_STATUS_OK
#define CHECK_STATUS_OK(stmt)                   \
    do {                                        \
        Status _status_ = (stmt);               \
        ASSERT_TRUE(_status_.ok()) << _status_; \
    } while (false)
#endif

class RemoteFileSystemTest : public testing::Test {
public:
    virtual void SetUp() {
        std::string ak = config::test_s3_ak;
        std::string sk = config::test_s3_sk;
        std::string endpoint = config::test_s3_endpoint;
        std::string region = config::test_s3_region;
        s3_location = config::test_s3_prefix;
        s3_prop.emplace("AWS_ACCESS_KEY", ak);
        s3_prop.emplace("AWS_SECRET_KEY", sk);
        s3_prop.emplace("AWS_ENDPOINT", endpoint);
        s3_prop.emplace("AWS_REGION", region);
    }

    virtual void TearDown() {}

private:
    std::map<std::string, std::string> s3_prop;
    std::string s3_location;
    std::map<std::string, std::string> hdfs_prop;
    TNetworkAddress broker_addr;
};

TEST_F(RemoteFileSystemTest, TestS3FileSystem) {
    std::unique_ptr<ThreadPool> _pool;
    ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
            .set_min_threads(5)
            .set_max_threads(10)
            .build(&_pool);
    ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    S3Conf s3_conf;
    S3URI s3_uri(s3_location);
    CHECK_STATUS_OK(s3_uri.parse());
    CHECK_STATUS_OK(S3ClientFactory::convert_properties_to_s3_conf(s3_prop, s3_uri, &s3_conf));
    std::cout << "the s3 conf is " << s3_conf.to_string() << std::endl;
    std::shared_ptr<io::S3FileSystem> fs;
    CHECK_STATUS_OK(io::S3FileSystem::create(std::move(s3_conf), "", &fs));

    // read file
    std::string file1 = config::test_s3_bucket;
    io::FileReaderSPtr reader;
    auto cur_time = time(nullptr);
    CHECK_STATUS_OK(fs->open_file(file1, &reader));
    size_t buf_size = 4 * 1024 * 1204;
    auto read_buf = std::make_unique<char[]>(buf_size);
    size_t bytes_read = 0;
    size_t file_size = reader->size();
    size_t already_read = 0;
    while (already_read < file_size) {
        std::cout << "begin to download at " << already_read << std::endl;
        CHECK_STATUS_OK(reader->read_at(already_read, {read_buf.get(), buf_size}, &bytes_read));
        already_read += bytes_read;
    }
    auto finish_time = time(nullptr);
    std::cout << "the download file size is " << file_size << " and it costs " << (finish_time - cur_time) << std::endl;
}

} // namespace doris
