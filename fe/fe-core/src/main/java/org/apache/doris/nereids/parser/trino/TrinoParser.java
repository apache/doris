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

package org.apache.doris.nereids.parser.trino;

/**
 * Trino Parser, depends on 395 trino-parser, and 4.9.3 antlr-runtime
 */
public class TrinoParser {
    private static final io.trino.sql.parser.ParsingOptions PARSING_OPTIONS =
            new io.trino.sql.parser.ParsingOptions(
                    io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    public static io.trino.sql.tree.Statement parse(String query) {
        io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();
        return sqlParser.createStatement(query, PARSING_OPTIONS);
    }
}
