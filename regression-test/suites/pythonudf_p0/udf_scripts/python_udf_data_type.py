# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


def row_to_csv_all_impl(
    bool_col, tinyint_col, smallint_col, int_col, bigint_col, largeint_col,
    float_col, double_col, decimal32_col, decimal64_col, decimal128_col,
    date_col, datetime_col, char_col, varchar_col, string_col
):
    cols = [
        bool_col, tinyint_col, smallint_col, int_col, bigint_col, largeint_col,
        float_col, double_col, decimal32_col, decimal64_col, decimal128_col,
        date_col, datetime_col, char_col, varchar_col, string_col
    ]
    
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    return ','.join(safe_str(col) for col in cols)