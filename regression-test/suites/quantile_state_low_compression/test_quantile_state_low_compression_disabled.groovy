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

suite("test_quantile_state_low_compression_disabled", "quantile_state_low_compression") {
    test {
        sql "SELECT quantile_state_to_base64(to_quantile_state(1.0, 32)) FROM numbers(\"number\" = \"1\")"
        exception "enable_low_compression_quantile_state"
    }
    test {
        sql "SELECT quantile_state_to_base64(to_quantile_state(1.0, nullable(cast(32 AS float)))) FROM numbers(\"number\" = \"1\")"
        exception "enable_low_compression_quantile_state"
    }
    test {
        sql "SELECT quantile_state_to_base64(to_quantile_state(number, nullable(cast(32 AS float)))) FROM numbers(\"number\" = \"1\")"
        exception "enable_low_compression_quantile_state"
    }
    test {
        sql "SELECT quantile_state_to_base64(to_quantile_state(number, 127)) FROM numbers(\"number\" = \"1\")"
        exception "enable_low_compression_quantile_state"
    }
    qt_high_compression """
        SELECT quantile_percent(to_quantile_state(number, 2048), 0.99)
        FROM numbers("number" = "1")
    """
    qt_invalid_compression_fallback """
        SELECT quantile_state_to_base64(to_quantile_state(number, 31)) =
               quantile_state_to_base64(to_quantile_state(number, 2048))
        FROM numbers("number" = "1")
    """
}
