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

import java.sql.SQLException

suite("test_custom_analyzer2", "p0") {
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS basic_tokenizer1
        PROPERTIES
        (
            "type" = "basic"
        );
    """

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS basic_tokenizer2
        PROPERTIES
        (
            "type" = "basic",
            "extra_chars" = "()\\"."
        );
    """

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS basic_tokenizer3
        PROPERTIES
        (
            "type" = "basic",
            "extra_chars" = "() "
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS basic_analyzer1
        PROPERTIES
        (
            "tokenizer" = "basic_tokenizer1"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS basic_analyzer2
        PROPERTIES
        (
            "tokenizer" = "basic_tokenizer2"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS basic_analyzer3
        PROPERTIES
        (
            "tokenizer" = "basic_tokenizer3"
        );
    """

    sql """ select sleep(10) """

    qt_tokenize_sql """ select tokenize("Found OpenMP: TRUE (found version \\"5.1\\")", '"analyzer"="basic_analyzer1"'); """
    qt_tokenize_sql """ select tokenize("Found OpenMP: TRUE (found version \\"5.1\\")", '"analyzer"="basic_analyzer2"'); """
    qt_tokenize_sql """ select tokenize("Found OpenMP: TRUE (found version \\"5.1\\")", '"analyzer"="basic_analyzer3"'); """
}