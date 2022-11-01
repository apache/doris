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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.URI;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.InternalService.PFormatType;
import org.apache.doris.proto.InternalService.PStorageType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTVFunctionName;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function——S3(path, AK, SK, format).
 */
public class S3TableValuedFunction extends ExternalFileTableValuedFunction {

    public static final String NAME = "s3";
    private String path;
    private URI uri;
    private String s3AK;
    private String s3SK;
    // format改为enum
    private PFormatType format;
    private TNetworkAddress address;


    public S3TableValuedFunction(List<String> params) throws UserException {
        if (params.size() != 4) {
            throw new UserException(
                    "s3 table function only support 4 params: S3(path, AK, SK, format)");
        }
        // uri = URI.create(params.get(0));
        path = params.get(0);
        s3AK = params.get(1);
        s3SK = params.get(2);
        String formatString = params.get(3);
        switch (formatString.toLowerCase()) {
            case "csv":
                format = PFormatType.CSV;
                break;
            // case "csv_with_names":
            //     format = FileFormatType.CSV_WITH_NAMES;
            //     break;
            // case "csv_with_names_and_types":
            //     format = FileFormatType.CSV_WITH_NAMES_AND_TYPES;
            //     break;
            default:
                throw new UserException("un support format " + formatString + " ,only support csv.");

        }
    }

    @Override
    public PFetchTableSchemaRequest getFetchTableStructureRequest() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("path", path);
        properties.put("AWS_ACCESS_KEY", s3AK);
        properties.put("AWS_SECRET_KEY", s3SK);
        PFetchTableSchemaRequest request = InternalService.PFetchTableSchemaRequest.newBuilder()
                .setStorageType(PStorageType.S3_STORAGE)
                .setFormatType(format)
                .putAllProperties(properties)
                .build();
        return request;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return null;
    }

    @Override
    public TTVFunctionName getFunctionName() {
        return TTVFunctionName.S3;
    }

    @Override
    public String getTableName() {
        return "S3TableValuedFunction";
    }
}
