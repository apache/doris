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

#include "runtime/minidump.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "env/env.h"
#include "util/file_utils.h"
#include "util/logging.h"
#include "util/uid_util.h"

namespace doris {

class MinidumpTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        UniqueId unique_id = UniqueId::gen_uid();
        _tmp_dir = "/tmp/" + unique_id.to_string();
        config::minidump_dir = _tmp_dir;
        config::max_minidump_file_number = 5;
        _minidump.init();
    }

    virtual void TearDown() {
        _minidump.stop();
        FileUtils::remove_all(_tmp_dir);
    }

    Minidump _minidump;
    std::string _tmp_dir;
};

TEST_F(MinidumpTest, testNormal) {
    std::vector<std::string> files;
    kill(getpid(), SIGUSR1);
    usleep(500000);
    FileUtils::list_files(Env::Default(), config::minidump_dir, &files);
    EXPECT_EQ(1, files.size());

    // kill 5 times
    kill(getpid(), SIGUSR1);
    kill(getpid(), SIGUSR1);
    kill(getpid(), SIGUSR1);
    kill(getpid(), SIGUSR1);
    kill(getpid(), SIGUSR1);
    usleep(500000);
    files.clear();
    FileUtils::list_files(Env::Default(), config::minidump_dir, &files);
    EXPECT_EQ(6, files.size());

    // sleep 10 seconds to wait it clean
    sleep(10);
    files.clear();
    FileUtils::list_files(Env::Default(), config::minidump_dir, &files);
    EXPECT_EQ(5, files.size());
}

} // end namespace doris
