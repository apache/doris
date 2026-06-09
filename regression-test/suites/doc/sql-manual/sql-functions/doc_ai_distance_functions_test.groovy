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

suite("doc_ai_distance_functions_test") {
    qt_cosine_distance '''
        SELECT COSINE_DISTANCE([1, 2], [2, 3]), COSINE_DISTANCE([3, 6], [4, 7]);
    '''

    qt_l2_distance '''
        SELECT L2_DISTANCE([4, 5], [6, 8]), L2_DISTANCE([3, 6], [4, 5]);
    '''
}
