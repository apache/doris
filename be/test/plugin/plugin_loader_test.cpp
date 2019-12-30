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
#include <libgen.h>

#include "plugin/plugin_loader.h"
#include "plugin/plugin.h"

#include "util/file_utils.h"

namespace doris {

class PluginLoaderTest : public testing::Test {
public:
    PluginLoaderTest() {
        char buf[1024];
        readlink("/proc/self/exe", buf, 1023);
        char* dir_path = dirname(buf);
        _path = std::string(dir_path);
    }

    ~PluginLoaderTest() { }
    
public:
    std::string _path;
};

TEST_F(PluginLoaderTest, normal) {
    FileUtils::remove_all(_path + "/plugin_test/target");

    DynamicPluginLoader plugin_loader("PluginExample", PLUGIN_TYPE_STORAGE, _path + "/plugin_test/source/test.zip",
                                      "libplugin_example.so", _path + "/plugin_test/target");
    ASSERT_TRUE(plugin_loader.install().ok());
    
    ASSERT_TRUE(FileUtils::is_dir(_path + "/plugin_test/target/PluginExample"));
    ASSERT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/PluginExample/"));
    
    std::shared_ptr<Plugin> plugin = plugin_loader.plugin();
    
    ASSERT_EQ(3, plugin->flags);
    ASSERT_EQ(1, plugin->init(nullptr));
    ASSERT_EQ(2, plugin->close(nullptr));
    
    ASSERT_TRUE(plugin->flags | PLUGIN_NOT_DYNAMIC_UNINSTALL);
    ASSERT_FALSE(plugin_loader.uninstall().ok());

    ASSERT_TRUE(FileUtils::is_dir(_path + "/plugin_test/target/PluginExample"));
    ASSERT_TRUE(FileUtils::check_exist(_path + "/plugin_test/target/PluginExample/"));

    FileUtils::remove_all(_path + "/plugin_test/target");
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
