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
    def version = get_cluster_version()
    def versionParts = version.split(/\./)[0..2].collect { it.toInteger() }
    if ( versionParts[0] == 0 ) {
        // trunk
        return true
    } 
    
    if (mainVersion > versionParts[0]) {
        return true
    } else if (mainVersion < versionParts[0]) {
        return false
    }

    if (midVersion > versionParts[1]) {
        return true
    } else if (midVersion < versionParts[1]) {
        return false
    }

    return thirdVersion >= versionParts[2]
}

Suite.metaClass.get_cluster_version = {
    def result = sql_return_maparray "show frontends"
    def version = result["Version"]
    // doris-0.0.0-354efae
    def pattern = /-(\d+\.\d+\.\d+(?:\.\d+)?)-/
    def matcher = (version =~ pattern) 
    if (matcher.find()) {
        return matcher.group(1)
    } else {
        throw new IllegalArgumentException("invalid version string: ${version}")
    }
}
