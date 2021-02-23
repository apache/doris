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

#include "plugin/plugin_mgr.h"

#include <gtest/gtest.h>

#include "plugin/plugin.h"
#include "plugin/plugin_loader.h"
#include "string"
#include "test_util/test_util.h"

namespace doris {

class DemoPluginHandler {
public:
    const std::string& hello(const std::string& msg) {
        _msg = msg;
        return _msg;
    }

private:
    std::string _msg;
};

int init_plugin(void* ptr) {
    // handler
    void** p = (void**)ptr;
    *p = new DemoPluginHandler();

    return 0;
}

int close_plugin(void* ptr) {
    void** p = (void**)ptr;
    delete (DemoPluginHandler*)(*p);
    LOG(INFO) << "close demo plugin";
    return 1;
}

//declare_builtin_plugin(demo_plugin) {
Plugin demo_plugin = {
        nullptr, &init_plugin, &close_plugin, PLUGIN_DEFAULT_FLAG, nullptr, nullptr,
};

class PluginMgrTest : public testing::Test {
public:
    PluginMgrTest() {
        _path = GetCurrentRunningDir();
        EXPECT_FALSE(_path.empty());
    }

    ~PluginMgrTest() {}

public:
    std::string _path;
};

TEST_F(PluginMgrTest, normal) {
    PluginMgr mgr;

    mgr.register_builtin_plugin("demo", PLUGIN_TYPE_AUDIT, &demo_plugin);

    std::shared_ptr<Plugin> re;
    ASSERT_TRUE(mgr.get_plugin("demo", PLUGIN_TYPE_AUDIT, &re).ok());
    ASSERT_NE(nullptr, re.get());
    ASSERT_EQ("test", ((DemoPluginHandler*)re->handler)->hello("test"));

    ASSERT_TRUE(mgr.get_plugin("demo", &re).ok());
    ASSERT_EQ("test", ((DemoPluginHandler*)re->handler)->hello("test"));

    std::vector<std::shared_ptr<Plugin>> list;
    ASSERT_TRUE(mgr.get_plugin_list(PLUGIN_TYPE_AUDIT, &list).ok());
    ASSERT_EQ(1, list.size());
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
