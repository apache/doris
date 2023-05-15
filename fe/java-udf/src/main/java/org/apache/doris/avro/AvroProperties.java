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

package org.apache.doris.avro;

public class AvroProperties {

    protected static final String COLUMNS_TYPE_DELIMITER = "#";
    protected static final String FIELDS_DELIMITER = ",";

    protected static final String COLUMNS_TYPES = "columns_types";
    protected static final String REQUIRED_FIELDS = "required_fields";
    protected static final String FILE_TYPE = "file_type";
    protected static final String URI = "uri";
    protected static final String S3_BUCKET = "s3.virtual.bucket";
    protected static final String S3_KEY = "s3.virtual.key";
    protected static final String S3_ACCESS_KEY = "s3.access_key";
    protected static final String S3_SECRET_KEY = "s3.secret_key";
    protected static final String S3_ENDPOINT = "s3.endpoint";
    protected static final String S3_REGION = "s3.region";

}
