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
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.thrift.TFileType;

import org.apache.http.client.methods.HttpPut;

import java.util.Map;

/**
 * The Implement of table valued function
 * stream(xxx).
 */
public class StreamTableValuedFunction extends ExternalFileTableValuedFunction{
    public static final String NAME = "stream";

    private final HttpPut requset;

    public StreamTableValuedFunction(Map<String, String> params){
        requset = getHttpRequest(params);

    }

    private HttpPut getHttpRequest(Map<String, String> params){
        // todo:
       return new HttpPut();
    }
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public String getFilePath() {
        return requset.toString();
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("StreamTvfBroker", StorageType.STREAM,locationProperties);
    }

    @Override
    public String getTableName() {
        return "StreamTableValuedFunction";
    }
}
