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

#include "exec/broker_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "util/cpu_info.h"
#include "util/stopwatch.hpp"

namespace doris {

class RuntimeState;

class BrokerReaderTest : public testing::Test {
public:
    BrokerReaderTest() { init(); }
    void init();

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    ExecEnv* _env;
    std::map<std::string, std::string> _properties;
    std::vector<TNetworkAddress> _addresses;
};

void BrokerReaderTest::init() {
    _properties["username"] = "root";
    _properties["password"] = "passwd";
    TNetworkAddress addr;
    addr.__set_hostname("host");
    addr.__set_port(9999);
    _addresses.push_back(addr);
}

TEST_F(BrokerReaderTest, normal) {
    std::string path = "hdfs://host:port/dir";
    BrokerReader reader(_env, _addresses, _properties, path, 0);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[128 * 1024];
    MonotonicStopWatch watch;
    watch.start();
    bool eof = false;
    size_t total_size = 0;
    while (!eof) {
        size_t buf_len = 128 * 1024;
        st = reader.read(buf, &buf_len, &eof);
        ASSERT_TRUE(st.ok());
        total_size += buf_len;
    }
    LOG(INFO) << "get from broker " << total_size << " bytes using " << watch.elapsed_time();
}

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    // doris::init_glog("be-test");
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
