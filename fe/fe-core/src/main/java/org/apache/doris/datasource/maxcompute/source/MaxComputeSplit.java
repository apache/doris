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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.thrift.TFileType;

import lombok.Getter;

import java.util.List;

@Getter
public class MaxComputeSplit extends FileSplit {
    public String scanSerialize;
    public String sessionId;

    public enum SplitType {
        ROW_OFFSET,
        BYTE_SIZE
    }

    public SplitType splitType;

    public MaxComputeSplit(LocationPath path, long start, long length, long fileLength,
            long modificationTime, String[] hosts, List<String> partitionValues) {
        super(path, start, length, fileLength, modificationTime, hosts, partitionValues);
        // MC always use FILE_NET type
        this.locationType = TFileType.FILE_NET;
    }

}
