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

package org.apache.doris.spark.util;

public abstract class ErrorMessages {
    public static final String PARSE_NUMBER_FAILED_MESSAGE = "Parse '{}' to number failed. Original string is '{}'.";
    public static final String PARSE_BOOL_FAILED_MESSAGE = "Parse '{}' to boolean failed. Original string is '{}'.";
    public static final String CONNECT_FAILED_MESSAGE = "Connect to doris {} failed.";
    public static final String ILLEGAL_ARGUMENT_MESSAGE = "argument '{}' is illegal, value is '{}'.";
    public static final String SHOULD_NOT_HAPPEN_MESSAGE = "Should not come here.";
    public static final String DORIS_INTERNAL_FAIL_MESSAGE = "Doris server '{}' internal failed, status is '{}', error message is '{}'";
}
