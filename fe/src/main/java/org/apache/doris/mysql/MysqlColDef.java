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

package org.apache.doris.mysql;

import org.apache.doris.analysis.Expr;

// MySQL protocol field used to describe result field info
public class MysqlColDef {
    // Not used in MySQL, always "def"
    private static final String CATALOG = "def";
    // Only support character set "UTF-8".
    private static final int CHARACTER_SET = 33;
    // NOTE: schema, table, orig_table, flags, decimals not used in PALO, so use static
    private static final String SCHEMA = "";
    private static final String TABLE = "";
    private static final String ORIG_TABLE = "";
    private static final int FLAGS = 0;
    private static final int DECIMALS = 0;

    private String name;
    private String origName;
    private int colLen;
    private int colType;
    private String defaultValue;

    public static MysqlColDef fromExpr(Expr expr, String alias) {
        MysqlColDef def = new MysqlColDef();
        def.name = alias;
        def.origName = alias;
        MysqlColType type = expr.getType().getPrimitiveType().toMysqlType();
        def.colType = type.getCode();
        // TODO(colLen): review colLen.
        def.colLen = 255;
        return def;
    }

    public static MysqlColDef fromName(String alias) {
        MysqlColDef def = new MysqlColDef();
        def.name = alias;
        def.origName = alias;
        def.colType = MysqlColType.MYSQL_TYPE_STRING.getCode();
        // TODO(colLen): review colLen.
        def.colLen = 255;
        return def;
    }

    public void writeTo(MysqlSerializer serializer) {
        serializer.writeLenEncodedString(CATALOG);
        serializer.writeLenEncodedString(SCHEMA);
        serializer.writeLenEncodedString(TABLE);
        serializer.writeLenEncodedString(ORIG_TABLE);
        serializer.writeLenEncodedString(name);
        serializer.writeLenEncodedString(origName);
        // length of the following fields(always 0x0c)
        serializer.writeVInt(0x0c);
        // Character set: two byte integer
        serializer.writeInt2(CHARACTER_SET);
        // Column length: four byte integer
        serializer.writeInt4(colLen);
        // Column type: one byte integer
        serializer.writeInt1(colType);
        serializer.writeInt2(FLAGS);
        serializer.writeInt1(DECIMALS);
        // filler: two byte integer
        serializer.writeInt2(0);
    }
}
