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

package org.apache.doris.protocol.mysql;

/**
 * MySQL Column Definition packet.
 * 
 * <p>This class represents the column definition in a result set. It contains
 * metadata about the column such as name, type, length, and flags.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
 * 
 * @since 2.0.0
 */
public class MysqlColDef {
    
    /** Catalog name (always "def" in MySQL) */
    private static final String CATALOG = "def";
    
    /** Default character set: 33 = UTF-8 */
    private static final int CHARACTER_SET = 33;
    
    /** Default schema (empty) */
    private static final String DEFAULT_SCHEMA = "";
    
    /** Default table (empty) */
    private static final String DEFAULT_TABLE = "";
    
    /** Default flags */
    private static final int DEFAULT_FLAGS = 0;
    
    /** Default decimals */
    private static final int DEFAULT_DECIMALS = 0;
    
    private String schema = DEFAULT_SCHEMA;
    private String table = DEFAULT_TABLE;
    private String origTable = DEFAULT_TABLE;
    private String name;
    private String origName;
    private int colLen = 255;
    private int colType;
    private int flags = DEFAULT_FLAGS;
    private int decimals = DEFAULT_DECIMALS;
    private String defaultValue;

    /**
     * Creates a column definition with name and type code.
     * 
     * @param name column name
     * @param colType MySQL type code (from MysqlColType)
     * @return MysqlColDef instance
     */
    public static MysqlColDef fromNameAndType(String name, int colType) {
        MysqlColDef def = new MysqlColDef();
        def.name = name;
        def.origName = name;
        def.colType = colType;
        return def;
    }

    /**
     * Creates a column definition with string type.
     * 
     * @param name column name
     * @return MysqlColDef instance
     */
    public static MysqlColDef fromName(String name) {
        return fromNameAndType(name, MysqlColType.MYSQL_TYPE_STRING.getCode());
    }

    /**
     * Writes this column definition to the serializer.
     * 
     * @param serializer the serializer to write to
     */
    public void writeTo(MysqlSerializer serializer) {
        serializer.writeLenEncodedString(CATALOG);
        serializer.writeLenEncodedString(schema);
        serializer.writeLenEncodedString(table);
        serializer.writeLenEncodedString(origTable);
        serializer.writeLenEncodedString(name);
        serializer.writeLenEncodedString(origName);
        // length of the following fields (always 0x0c)
        serializer.writeVInt(0x0c);
        // Character set: two byte integer
        serializer.writeInt2(CHARACTER_SET);
        // Column length: four byte integer
        serializer.writeInt4(colLen);
        // Column type: one byte integer
        serializer.writeInt1(colType);
        serializer.writeInt2(flags);
        serializer.writeInt1(decimals);
        // filler: two byte integer
        serializer.writeInt2(0);
    }
    
    // Getters and setters
    
    public String getSchema() {
        return schema;
    }
    
    public MysqlColDef setSchema(String schema) {
        this.schema = schema;
        return this;
    }
    
    public String getTable() {
        return table;
    }
    
    public MysqlColDef setTable(String table) {
        this.table = table;
        return this;
    }
    
    public String getOrigTable() {
        return origTable;
    }
    
    public MysqlColDef setOrigTable(String origTable) {
        this.origTable = origTable;
        return this;
    }
    
    public String getName() {
        return name;
    }
    
    public MysqlColDef setName(String name) {
        this.name = name;
        return this;
    }
    
    public String getOrigName() {
        return origName;
    }
    
    public MysqlColDef setOrigName(String origName) {
        this.origName = origName;
        return this;
    }
    
    public int getColLen() {
        return colLen;
    }
    
    public MysqlColDef setColLen(int colLen) {
        this.colLen = colLen;
        return this;
    }
    
    public int getColType() {
        return colType;
    }
    
    public MysqlColDef setColType(int colType) {
        this.colType = colType;
        return this;
    }
    
    public int getFlags() {
        return flags;
    }
    
    public MysqlColDef setFlags(int flags) {
        this.flags = flags;
        return this;
    }
    
    public int getDecimals() {
        return decimals;
    }
    
    public MysqlColDef setDecimals(int decimals) {
        this.decimals = decimals;
        return this;
    }
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    public MysqlColDef setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
