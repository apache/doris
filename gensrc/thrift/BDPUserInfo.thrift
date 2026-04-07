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

namespace cpp doris
namespace java org.apache.doris.thrift

struct TBDPUserInfo {
   1: required string source
   2: required binary scrambledPassword
   3: optional string erp
   4: optional string hadoopUserName
   5: optional string userToken
   6: optional string catalog
   7: optional string db
}

struct TBDPAuthContext {
   1: required string source
   2: required string erp
   3: required string hadoopUserName
   4: required string userToken
   5: optional bool viewBased
}
