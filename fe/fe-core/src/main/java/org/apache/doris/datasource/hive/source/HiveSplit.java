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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.SplitCreator;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.spi.Split;

import java.util.List;

public class HiveSplit extends FileSplit {

    private HiveSplit(LocationPath path, long start, long length, long fileLength,
            long modificationTime, String[] hosts, List<String> partitionValues, AcidInfo acidInfo,
            String inputFormat, String serde) {
        super(path, start, length, fileLength, modificationTime, hosts, partitionValues);
        this.acidInfo = acidInfo;
        this.inputFormat = inputFormat;
        this.serde = serde;
    }

    @Override
    public Object getInfo() {
        return acidInfo;
    }

    private AcidInfo acidInfo;
    private String inputFormat;
    private String serde;

    public boolean isACID() {
        return acidInfo != null;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getSerde() {
        return serde;
    }

    public static class HiveSplitCreator implements SplitCreator {

        private AcidInfo acidInfo;
        private String inputFormat;
        private String serde;

        public HiveSplitCreator(AcidInfo acidInfo) {
            this(acidInfo, null, null);
        }

        public HiveSplitCreator(AcidInfo acidInfo, String inputFormat, String serde) {
            this.acidInfo = acidInfo;
            this.inputFormat = inputFormat;
            this.serde = serde;
        }

        @Override
        public Split create(LocationPath path, long start, long length, long fileLength,
                long fileSplitSize,
                long modificationTime, String[] hosts,
                List<String> partitionValues) {
            HiveSplit split =  new HiveSplit(path, start, length, fileLength, modificationTime,
                    hosts, partitionValues, acidInfo, inputFormat, serde);
            split.setTargetSplitSize(fileSplitSize);
            return split;
        }
    }
}
