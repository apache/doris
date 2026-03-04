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

suite("test_quantile_state_function") {
    qt_sql_quantile_state_base64_1 """
        select quantile_state_to_base64(quantile_state_empty())
    """

    qt_sql_quantile_state_base64_2 """
        select quantile_state_from_base64(null)
    """

    qt_sql_quantile_state_base64_3 """
        select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(quantile_state_empty())
            )
        ) = quantile_state_to_base64(quantile_state_empty())
    """

    qt_sql_quantile_state_base64_4 """
        select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(to_quantile_state(1.0, 2048))
            )
        ) = quantile_state_to_base64(to_quantile_state(1.0, 2048))
    """

    qt_sql_quantile_state_base64_5 """
        select quantile_state_to_base64(to_quantile_state(1.0, 2048))
    """

    qt_sql_quantile_state_base64_6 """
        select quantile_state_from_base64('invalid')
    """

    qt_sql_quantile_state_base64_7 """
        select quantile_state_from_base64('not_base64!')
    """

    qt_sql_quantile_state_base64_8 """
        select quantile_state_from_base64('')
    """

    qt_sql_quantile_state_base64_9 """
        select length(quantile_state_to_base64(to_quantile_state(1.0, 2048))) > 0
    """

    qt_sql_quantile_state_base64_10 """
        select quantile_state_from_base64(quantile_state_to_base64(null))
    """

    qt_sql_quantile_state_base64_11 """
        select quantile_state_to_base64(to_quantile_state(10.0, 2048))
    """

    qt_sql_quantile_state_base64_12 """
         select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(
                    quantile_state_from_base64(
                        quantile_state_to_base64(to_quantile_state(10.0, 2048))
                    )
                )
            )
        ) = quantile_state_to_base64(to_quantile_state(10.0, 2048))
    """
}
