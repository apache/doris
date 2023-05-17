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

package org.apache.doris.planner.external;

import org.apache.doris.datasource.hive.AcidInfo;

import org.apache.hadoop.fs.Path;

import java.util.List;

public class HiveSplit extends FileSplit {

    public HiveSplit(Path path, long start, long length, long fileLength,
            long modificationTime, String[] hosts, List<String> partitionValues, AcidInfo acidInfo) {
        super(path, start, length, fileLength, modificationTime, hosts, partitionValues);
        this.acidInfo = acidInfo;
    }

    public HiveSplit(Path path, long start, long length, long fileLength, String[] hosts, AcidInfo acidInfo) {
        super(path, start, length, fileLength, hosts, null);
        this.acidInfo = acidInfo;
    }

    @Override
    public Object getInfo() {
        return acidInfo;
    }

    private AcidInfo acidInfo;

    public boolean isACID() {
        return acidInfo != null;
    }
}
