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

suite("test_decimalv3_cast") {
    qt_cast0 """ select cast('0.00164999999999998' as decimalv3(9,0)); """
    qt_cast1 """ select cast('0.00164999999999998' as decimalv3(10,0)); """
    qt_cast2 """ select cast('0.000000000001234567890' as decimalv3(18,0)); """
    qt_cast3 """ select cast('0.000000000001234567890' as decimalv3(19,0)); """
    qt_cast4 """ select cast('0.00000000000000000000000000000012345678901' as decimalv3(38,0)); """
}
