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

suite("test_install_plugin_command", "nereids_p0") {
    def pluginFile = new File(context.config.dataPath, "plugin_test/auditdemo.zip").getAbsolutePath()
    def pluginSource = "\"${pluginFile}\""
    def pluginName = "audit_plugin_demo"
    def pluginProperties = "\"md5sum\"=\"f7280e8be71fd565a18c80d02b8c446f\""

    try {
        // Install the plugin
        checkNereidsExecute """
            INSTALL PLUGIN FROM ${pluginSource} 
            PROPERTIES (${pluginProperties})
        """

    } finally {
        checkNereidsExecute """
            UNINSTALL PLUGIN ${pluginName}
        """
    }
}
