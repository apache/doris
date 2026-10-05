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

suite("json_constant") {
    qt_json1 "SELECT /*+ set_var(enable_fold_constant_by_be=0) */ json_array(array(cast(1 as decimal), cast(1.2 as decimal)));"
    qt_json2 "SELECT /*+ set_var(enable_fold_constant_by_be=1) */ json_array(array(cast(1 as decimal), cast(1.2 as decimal)));"
}
