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

#include "plugin/plugin_loader.h"

#include <gtest/gtest.h>

#include "plugin/plugin.h"
#include "testutil/test_util.h"
#include "util/file_utils.h"

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

class PluginLoaderTest : public testing::Test {
public:
    PluginLoaderTest() {
        _path = GetCurrentRunningDir();
        EXPECT_FALSE(_path.empty());
    }

    ~PluginLoaderTest() {}

public:
    std::string _path;
};

TEST_F(PluginLoaderTest, normal) {
    FileUtils::remove_all(_path + "/plugin_test/target");

    DynamicPluginLoader plugin_loader("PluginExample", PLUGIN_TYPE_STORAGE,
                                      _path + "/plugin_test/source/test.zip",
                                      "libplugin_example.so", _path + "/plugin_test/target");
    EXPECT_TRUE(plugin_loader.install().ok());

    EXPECT_TRUE(FileUtils::is_dir(_path + "/plugin_test/target/PluginExample"));
    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/PluginExample/"));

    std::shared_ptr<Plugin> plugin = plugin_loader.plugin();

    EXPECT_EQ(3, plugin->flags);
    EXPECT_EQ(1, plugin->init(nullptr));
    EXPECT_EQ(2, plugin->close(nullptr));

    EXPECT_TRUE(plugin->flags & PLUGIN_NOT_DYNAMIC_UNINSTALL);
    EXPECT_FALSE(plugin_loader.uninstall().ok());

    EXPECT_TRUE(FileUtils::is_dir(_path + "/plugin_test/target/PluginExample"));
    EXPECT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/PluginExample/"));

    FileUtils::remove_all(_path + "/plugin_test/target");
}

TEST_F(PluginLoaderTest, builtin) {
    Plugin demoPlugin = {
            nullptr, &init_plugin, &close_plugin, PLUGIN_DEFAULT_FLAG, nullptr, nullptr,
    };

    BuiltinPluginLoader plugin_loader("test", PLUGIN_TYPE_AUDIT, &demoPlugin);
    EXPECT_TRUE(plugin_loader.install().ok());

    std::shared_ptr<Plugin> plugin = plugin_loader.plugin();
    EXPECT_EQ(PLUGIN_DEFAULT_FLAG, plugin->flags);

    EXPECT_NE(nullptr, plugin->handler);
    EXPECT_EQ("test", ((DemoPluginHandler*)plugin->handler)->hello("test"));

    EXPECT_TRUE(plugin_loader.uninstall().ok());
}

} // namespace doris
