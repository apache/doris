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

suite("test_jsonb_keys_invalid_path", "p0") {
    test {
        sql """
            SELECT json_keys(CAST('{"a":{"b":1}}' AS JSONB), '\$**.a');
        """
        exception "In this situation, path expressions may not contain the * and ** tokens or an array range."
    }

    test {
        sql """
            SELECT json_keys(j, p)
            FROM (
                SELECT CAST('{"a":{"b":1}}' AS JSONB) AS j, '\$**.a' AS p
            ) t;
        """
        exception "In this situation, path expressions may not contain the * and ** tokens or an array range."
    }
}
