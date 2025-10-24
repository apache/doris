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

import org.apache.doris.regression.suite.Suite

Suite.metaClass.is_cluster_newer_than = { int mainVersion, int midVersion, int thirdVersion ->
    def result = sql_return_maparray "show frontends"
    def version = result[0]["Version"]
    // doris-0.0.0-354efae
    def pattern = /-(\d+\.\d+\.\d+(?:\.\d+)?)-/
    def matcher = (version =~ pattern) 
    if (matcher.find() == false) {
        throw new IllegalArgumentException("invalid version string: ${version}")
    }

    def versionParts = matcher.group(1).split("\\.")*.toInteger()
    if (versionParts.size() < 3) {
        throw new IllegalArgumentException("invalid version string: ${version}")
    }
    if (version.toLowerCase().contains("cloud") && versionParts[0] > 0) {
        versionParts[0] -= 1
    } 

    if ( versionParts[0] == 0 ) {
        // trunk
        return true
    }

    if (versionParts[0] > mainVersion) {
        return true
    } else if (versionParts[0] < mainVersion) {
        return false
    }

    if (versionParts[1] > midVersion) {
        return true
    } else if (versionParts[1] < midVersion) {
        return false
    }

    return versionParts[2] >= thirdVersion
}
