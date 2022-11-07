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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * The Implement of table valued function——S3(path, AK, SK, format).
 */
public class S3TableValuedFunction extends ExternalFileTableValuedFunction {
    public static final Logger LOG = LogManager.getLogger(S3TableValuedFunction.class);
    public static final String NAME = "s3";
    public static final String S3_AK = "AWS_ACCESS_KEY";
    public static final String S3_SK = "AWS_SECRET_KEY";
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";
    public static final String S3_REGION = "AWS_REGION";
    public static final String USE_PATH_STYLE = "use_path_style";

    private S3URI s3uri;
    private String s3AK;
    private String s3SK;

    public S3TableValuedFunction(List<String> params) throws UserException {
        if (params.size() != 4) {
            throw new UserException(
                    "s3 table function only support 4 params now: S3(path, AK, SK, format)");
        }

        s3uri = S3URI.create(params.get(0));
        s3AK = params.get(1);
        s3SK = params.get(2);

        String formatString = params.get(3).toLowerCase();
        switch (formatString) {
            case "csv":
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "csv_with_names":
                this.headerType = FeConstants.csv_with_names;
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "csv_with_names_and_types":
                this.headerType = FeConstants.csv_with_names_and_types;
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            default:
                throw new AnalysisException("format:" + formatString + " is not supported.");
        }

        // set S3 location properties
        locationProperties = Maps.newHashMap();
        locationProperties.put(S3_ENDPOINT, s3uri.getBucketScheme());
        locationProperties.put(S3_AK, s3AK);
        locationProperties.put(S3_SK, s3SK);
        locationProperties.put(S3_REGION, "");
        locationProperties.put(USE_PATH_STYLE, "true");

        parseFile();
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
    }

    @Override
    public String getFilePath() {
        // must be "s3://..."
        return NAME + S3URI.SCHEME_DELIM + s3uri.getKey();
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("S3TvfBroker", StorageType.S3, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "S3TableValuedFunction";
    }
}
