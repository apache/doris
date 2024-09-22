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

#ifndef _SCHEMA_SCANNER_HELPER_H_

#include <stdint.h>

#include <string>
#include <vector>

// this is a util class which can be used by all shema scanner
// all common functions are added in this class.
namespace doris {

namespace vectorized {
class Block;
} // namespace vectorized
class SchemaScannerHelper {
public:
    static void insert_string_value(int col_index, std::string str_val, vectorized::Block* block);
    static void insert_datetime_value(int col_index, const std::vector<void*>& datas,
                                      vectorized::Block* block);

    static void insert_int64_value(int col_index, int64_t int_val, vectorized::Block* block);
    static void insert_double_value(int col_index, double double_val, vectorized::Block* block);
};

} // namespace doris
#endif
