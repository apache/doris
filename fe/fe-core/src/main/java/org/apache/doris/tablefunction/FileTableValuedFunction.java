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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TFileType;

import java.util.Map;

public class FileTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "file";
    public static final String STORAGE_TYPE = "storage_type";

    private ExternalFileTableValuedFunction delegateTvf;

    public FileTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        String storageType = getOrDefaultAndRemove(properties, STORAGE_TYPE, "");
        switch (storageType) {
            case S3TableValuedFunction.NAME:
                delegateTvf = new S3TableValuedFunction(properties);
                break;
            case HdfsTableValuedFunction.NAME:
                delegateTvf = new HdfsTableValuedFunction(properties);
                break;
            case LocalTableValuedFunction.NAME:
                delegateTvf = new LocalTableValuedFunction(properties);
                break;
            default:
                throw new AnalysisException("Could not find storage_type: " + storageType);
        }
    }

    @Override
    public TFileType getTFileType() {
        return delegateTvf.getTFileType();
    }

    @Override
    public String getFilePath() {
        return delegateTvf.getFilePath();
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return delegateTvf.getBrokerDesc();
    }

    @Override
    public String getTableName() {
        return delegateTvf.getTableName();
    }
}
