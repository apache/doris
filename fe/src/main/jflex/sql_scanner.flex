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

package org.apache.doris.analysis;

import java_cup.runtime.Symbol;
import java.io.StringWriter;
import java.lang.Integer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.doris.analysis.SqlParserSymbols;
import org.apache.doris.common.util.SqlUtils;

%%

%class SqlScanner
%cup
%public
%final
%eofval{
    return newToken(SqlParserSymbols.EOF, null);
%eofval}
%unicode
%line
%column
%{
    // Help to judge a integer-literal is bigger than LARGEINT_MAX
    // NOTE: the 'longMin' is not '-2^63' here, to make sure the return value functions
    // like 'long abs(long)' is same with the type of arguments, which is valid.
    private static final BigInteger LONG_MAX = new BigInteger("9223372036854775807"); // 2^63 - 1

    private static final BigInteger LARGEINT_MAX_ABS = new BigInteger("170141183460469231731687303715884105728"); // 2^127

    // map from keyword string to token id
    // we use a linked hash map because the insertion order is important.
    // for example, we want "and" to come after "&&" to make sure error reporting
    // uses "and" as a display name and not "&&"
    private static final Map<String, Integer> keywordMap = new LinkedHashMap<String, Integer>();
    static {
        keywordMap.put("&&", new Integer(SqlParserSymbols.KW_AND));
        keywordMap.put("add", new Integer(SqlParserSymbols.KW_ADD));
        keywordMap.put("admin", new Integer(SqlParserSymbols.KW_ADMIN));
        keywordMap.put("after", new Integer(SqlParserSymbols.KW_AFTER));
        keywordMap.put("aggregate", new Integer(SqlParserSymbols.KW_AGGREGATE));
        keywordMap.put("all", new Integer(SqlParserSymbols.KW_ALL));
        keywordMap.put("alter", new Integer(SqlParserSymbols.KW_ALTER));
        keywordMap.put("and", new Integer(SqlParserSymbols.KW_AND));
        keywordMap.put("anti", new Integer(SqlParserSymbols.KW_ANTI));
        keywordMap.put("as", new Integer(SqlParserSymbols.KW_AS));
        keywordMap.put("asc", new Integer(SqlParserSymbols.KW_ASC));
        keywordMap.put("authors", new Integer(SqlParserSymbols.KW_AUTHORS));
        keywordMap.put("backend", new Integer(SqlParserSymbols.KW_BACKEND));
        keywordMap.put("backends", new Integer(SqlParserSymbols.KW_BACKENDS));
        keywordMap.put("backup", new Integer(SqlParserSymbols.KW_BACKUP));
        keywordMap.put("begin", new Integer(SqlParserSymbols.KW_BEGIN));
        keywordMap.put("between", new Integer(SqlParserSymbols.KW_BETWEEN));
        keywordMap.put("bigint", new Integer(SqlParserSymbols.KW_BIGINT));
        keywordMap.put("boolean", new Integer(SqlParserSymbols.KW_BOOLEAN));
        keywordMap.put("hll", new Integer(SqlParserSymbols.KW_HLL));
        keywordMap.put("both", new Integer(SqlParserSymbols.KW_BOTH));
        keywordMap.put("broker", new Integer(SqlParserSymbols.KW_BROKER));
        keywordMap.put("by", new Integer(SqlParserSymbols.KW_BY));
        keywordMap.put("cancel", new Integer(SqlParserSymbols.KW_CANCEL));
        keywordMap.put("case", new Integer(SqlParserSymbols.KW_CASE));
        keywordMap.put("cast", new Integer(SqlParserSymbols.KW_CAST));
        keywordMap.put("chain", new Integer(SqlParserSymbols.KW_CHAIN));
        keywordMap.put("char", new Integer(SqlParserSymbols.KW_CHAR));
        keywordMap.put("character", new Integer(SqlParserSymbols.KW_CHAR));
        keywordMap.put("charset", new Integer(SqlParserSymbols.KW_CHARSET));
        keywordMap.put("check", new Integer(SqlParserSymbols.KW_CHECK));
        keywordMap.put("cluster", new Integer(SqlParserSymbols.KW_CLUSTER));
        keywordMap.put("clusters", new Integer(SqlParserSymbols.KW_CLUSTERS));
        keywordMap.put("free", new Integer(SqlParserSymbols.KW_FREE));
        keywordMap.put("system", new Integer(SqlParserSymbols.KW_SYSTEM));
        keywordMap.put("link", new Integer(SqlParserSymbols.KW_LINK));
        keywordMap.put("migrate", new Integer(SqlParserSymbols.KW_MIGRATE));
        keywordMap.put("enter", new Integer(SqlParserSymbols.KW_ENTER));
        keywordMap.put("migrations", new Integer(SqlParserSymbols.KW_MIGRATIONS));
        keywordMap.put("collate", new Integer(SqlParserSymbols.KW_COLLATE));
        keywordMap.put("collation", new Integer(SqlParserSymbols.KW_COLLATION));
        keywordMap.put("column", new Integer(SqlParserSymbols.KW_COLUMN));
        keywordMap.put("columns", new Integer(SqlParserSymbols.KW_COLUMNS));
        keywordMap.put("comment", new Integer(SqlParserSymbols.KW_COMMENT));
        keywordMap.put("commit", new Integer(SqlParserSymbols.KW_COMMIT));
        keywordMap.put("committed", new Integer(SqlParserSymbols.KW_COMMITTED));
        keywordMap.put("config", new Integer(SqlParserSymbols.KW_CONFIG));
        keywordMap.put("connection", new Integer(SqlParserSymbols.KW_CONNECTION));
        keywordMap.put("connection_id", new Integer(SqlParserSymbols.KW_CONNECTION_ID));
        keywordMap.put("consistent", new Integer(SqlParserSymbols.KW_CONSISTENT));
        keywordMap.put("count", new Integer(SqlParserSymbols.KW_COUNT));
        keywordMap.put("create", new Integer(SqlParserSymbols.KW_CREATE));
        keywordMap.put("cross", new Integer(SqlParserSymbols.KW_CROSS));
        keywordMap.put("current", new Integer(SqlParserSymbols.KW_CURRENT));
        keywordMap.put("current_user", new Integer(SqlParserSymbols.KW_CURRENT_USER));
        keywordMap.put("data", new Integer(SqlParserSymbols.KW_DATA));
        keywordMap.put("database", new Integer(SqlParserSymbols.KW_DATABASE));
        keywordMap.put("databases", new Integer(SqlParserSymbols.KW_DATABASES));
        keywordMap.put("date", new Integer(SqlParserSymbols.KW_DATE));
        keywordMap.put("datetime", new Integer(SqlParserSymbols.KW_DATETIME));
        keywordMap.put("decimal", new Integer(SqlParserSymbols.KW_DECIMAL));
        keywordMap.put("decommission", new Integer(SqlParserSymbols.KW_DECOMMISSION));
        keywordMap.put("default", new Integer(SqlParserSymbols.KW_DEFAULT));
        keywordMap.put("delete", new Integer(SqlParserSymbols.KW_DELETE));
        keywordMap.put("desc", new Integer(SqlParserSymbols.KW_DESC));
        keywordMap.put("describe", new Integer(SqlParserSymbols.KW_DESCRIBE));
        keywordMap.put("distinct", new Integer(SqlParserSymbols.KW_DISTINCT));
        keywordMap.put("distinctpc", new Integer(SqlParserSymbols.KW_DISTINCTPC));
        keywordMap.put("distinctpc", new Integer(SqlParserSymbols.KW_DISTINCTPC));
        keywordMap.put("distinctpcsa", new Integer(SqlParserSymbols.KW_DISTINCTPCSA));
        keywordMap.put("distinctpcsa", new Integer(SqlParserSymbols.KW_DISTINCTPCSA));
        keywordMap.put("distributed", new Integer(SqlParserSymbols.KW_DISTRIBUTED));
        keywordMap.put("distribution", new Integer(SqlParserSymbols.KW_DISTRIBUTION));
        keywordMap.put("buckets", new Integer(SqlParserSymbols.KW_BUCKETS));
        keywordMap.put("div", new Integer(SqlParserSymbols.KW_DIV));
        keywordMap.put("double", new Integer(SqlParserSymbols.KW_DOUBLE));
        keywordMap.put("drop", new Integer(SqlParserSymbols.KW_DROP));
        keywordMap.put("dropp", new Integer(SqlParserSymbols.KW_DROPP));
        keywordMap.put("duplicate", new Integer(SqlParserSymbols.KW_DUPLICATE));
        keywordMap.put("else", new Integer(SqlParserSymbols.KW_ELSE));
        keywordMap.put("end", new Integer(SqlParserSymbols.KW_END));
        keywordMap.put("engine", new Integer(SqlParserSymbols.KW_ENGINE));
        keywordMap.put("engines", new Integer(SqlParserSymbols.KW_ENGINES));
        keywordMap.put("errors", new Integer(SqlParserSymbols.KW_ERRORS));
        keywordMap.put("events", new Integer(SqlParserSymbols.KW_EVENTS));
        keywordMap.put("explain", new Integer(SqlParserSymbols.KW_DESCRIBE));
        keywordMap.put("export", new Integer(SqlParserSymbols.KW_EXPORT));
        keywordMap.put("exists", new Integer(SqlParserSymbols.KW_EXISTS));
        keywordMap.put("external", new Integer(SqlParserSymbols.KW_EXTERNAL));
        keywordMap.put("extract", new Integer(SqlParserSymbols.KW_EXTRACT));
        keywordMap.put("false", new Integer(SqlParserSymbols.KW_FALSE));
        keywordMap.put("file", new Integer(SqlParserSymbols.KW_FILE));
        keywordMap.put("first", new Integer(SqlParserSymbols.KW_FIRST));
        keywordMap.put("float", new Integer(SqlParserSymbols.KW_FLOAT));
        keywordMap.put("follower", new Integer(SqlParserSymbols.KW_FOLLOWER));
        keywordMap.put("following", new Integer(SqlParserSymbols.KW_FOLLOWING));
        keywordMap.put("for", new Integer(SqlParserSymbols.KW_FOR));
        keywordMap.put("format", new Integer(SqlParserSymbols.KW_FORMAT));
        keywordMap.put("path", new Integer(SqlParserSymbols.KW_PATH));
        keywordMap.put("from", new Integer(SqlParserSymbols.KW_FROM));
        keywordMap.put("frontend", new Integer(SqlParserSymbols.KW_FRONTEND));
        keywordMap.put("frontends", new Integer(SqlParserSymbols.KW_FRONTENDS));
        keywordMap.put("full", new Integer(SqlParserSymbols.KW_FULL));
        keywordMap.put("function", new Integer(SqlParserSymbols.KW_FUNCTION));
        keywordMap.put("global", new Integer(SqlParserSymbols.KW_GLOBAL));
        keywordMap.put("grant", new Integer(SqlParserSymbols.KW_GRANT));
        keywordMap.put("grants", new Integer(SqlParserSymbols.KW_GRANTS));
        keywordMap.put("group", new Integer(SqlParserSymbols.KW_GROUP));
        keywordMap.put("hash", new Integer(SqlParserSymbols.KW_HASH));
        keywordMap.put("having", new Integer(SqlParserSymbols.KW_HAVING));
        keywordMap.put("help", new Integer(SqlParserSymbols.KW_HELP));
        keywordMap.put("hll_union", new Integer(SqlParserSymbols.KW_HLL_UNION));
        keywordMap.put("bitmap_union", new Integer(SqlParserSymbols.KW_BITMAP_UNION));
        keywordMap.put("hub", new Integer(SqlParserSymbols.KW_HUB)); 
        keywordMap.put("identified", new Integer(SqlParserSymbols.KW_IDENTIFIED));
        keywordMap.put("if", new Integer(SqlParserSymbols.KW_IF));
        keywordMap.put("in", new Integer(SqlParserSymbols.KW_IN));
        keywordMap.put("index", new Integer(SqlParserSymbols.KW_INDEX));
        keywordMap.put("indexes", new Integer(SqlParserSymbols.KW_INDEXES));
        keywordMap.put("infile", new Integer(SqlParserSymbols.KW_INFILE));
        keywordMap.put("inner", new Integer(SqlParserSymbols.KW_INNER));
        keywordMap.put("insert", new Integer(SqlParserSymbols.KW_INSERT));
        keywordMap.put("int", new Integer(SqlParserSymbols.KW_INT));
        keywordMap.put("integer", new Integer(SqlParserSymbols.KW_INT));
        keywordMap.put("intermediate", new Integer(SqlParserSymbols.KW_INTERMEDIATE));
        keywordMap.put("interval", new Integer(SqlParserSymbols.KW_INTERVAL));
        keywordMap.put("into", new Integer(SqlParserSymbols.KW_INTO));
        keywordMap.put("is", new Integer(SqlParserSymbols.KW_IS));
        keywordMap.put("isnull", new Integer(SqlParserSymbols.KW_ISNULL));
        keywordMap.put("isolation", new Integer(SqlParserSymbols.KW_ISOLATION));
        keywordMap.put("join", new Integer(SqlParserSymbols.KW_JOIN));
        keywordMap.put("key", new Integer(SqlParserSymbols.KW_KEY));
        keywordMap.put("kill", new Integer(SqlParserSymbols.KW_KILL));
        keywordMap.put("label", new Integer(SqlParserSymbols.KW_LABEL));
        keywordMap.put("largeint", new Integer(SqlParserSymbols.KW_LARGEINT));
        keywordMap.put("last", new Integer(SqlParserSymbols.KW_LAST));
        keywordMap.put("left", new Integer(SqlParserSymbols.KW_LEFT));
        keywordMap.put("less", new Integer(SqlParserSymbols.KW_LESS));
        keywordMap.put("level", new Integer(SqlParserSymbols.KW_LEVEL));
        keywordMap.put("like", new Integer(SqlParserSymbols.KW_LIKE));
        keywordMap.put("limit", new Integer(SqlParserSymbols.KW_LIMIT));
        keywordMap.put("load", new Integer(SqlParserSymbols.KW_LOAD));
        keywordMap.put("routine", new Integer(SqlParserSymbols.KW_ROUTINE));
        keywordMap.put("pause", new Integer(SqlParserSymbols.KW_PAUSE));
        keywordMap.put("resume", new Integer(SqlParserSymbols.KW_RESUME));
        keywordMap.put("stop", new Integer(SqlParserSymbols.KW_STOP));
        keywordMap.put("task", new Integer(SqlParserSymbols.KW_TASK));
        keywordMap.put("local", new Integer(SqlParserSymbols.KW_LOCAL));
        keywordMap.put("location", new Integer(SqlParserSymbols.KW_LOCATION));
        keywordMap.put("max", new Integer(SqlParserSymbols.KW_MAX));
        keywordMap.put("maxvalue", new Integer(SqlParserSymbols.KW_MAX_VALUE));
        keywordMap.put("merge", new Integer(SqlParserSymbols.KW_MERGE));
        keywordMap.put("min", new Integer(SqlParserSymbols.KW_MIN));
        keywordMap.put("modify", new Integer(SqlParserSymbols.KW_MODIFY));
        keywordMap.put("name", new Integer(SqlParserSymbols.KW_NAME));
        keywordMap.put("names", new Integer(SqlParserSymbols.KW_NAMES));
        keywordMap.put("negative", new Integer(SqlParserSymbols.KW_NEGATIVE));
        keywordMap.put("no", new Integer(SqlParserSymbols.KW_NO));
        keywordMap.put("not", new Integer(SqlParserSymbols.KW_NOT));
        keywordMap.put("null", new Integer(SqlParserSymbols.KW_NULL));
        keywordMap.put("nulls", new Integer(SqlParserSymbols.KW_NULLS));
        keywordMap.put("observer", new Integer(SqlParserSymbols.KW_OBSERVER));
        keywordMap.put("offset", new Integer(SqlParserSymbols.KW_OFFSET));
        keywordMap.put("on", new Integer(SqlParserSymbols.KW_ON));
        keywordMap.put("only", new Integer(SqlParserSymbols.KW_ONLY));
        keywordMap.put("open", new Integer(SqlParserSymbols.KW_OPEN));
        keywordMap.put("or", new Integer(SqlParserSymbols.KW_OR));
        keywordMap.put("order", new Integer(SqlParserSymbols.KW_ORDER));
        keywordMap.put("outer", new Integer(SqlParserSymbols.KW_OUTER));
        keywordMap.put("over", new Integer(SqlParserSymbols.KW_OVER));
        keywordMap.put("partition", new Integer(SqlParserSymbols.KW_PARTITION));
        keywordMap.put("partitions", new Integer(SqlParserSymbols.KW_PARTITIONS));
        keywordMap.put("preceding", new Integer(SqlParserSymbols.KW_PRECEDING));
        keywordMap.put("range", new Integer(SqlParserSymbols.KW_RANGE));
        keywordMap.put("password", new Integer(SqlParserSymbols.KW_PASSWORD));
        keywordMap.put("plugin", new Integer(SqlParserSymbols.KW_PLUGIN));
        keywordMap.put("plugins", new Integer(SqlParserSymbols.KW_PLUGINS));
        keywordMap.put("primary", new Integer(SqlParserSymbols.KW_PRIMARY));
        keywordMap.put("proc", new Integer(SqlParserSymbols.KW_PROC));
        keywordMap.put("procedure", new Integer(SqlParserSymbols.KW_PROCEDURE));
        keywordMap.put("processlist", new Integer(SqlParserSymbols.KW_PROCESSLIST));
        keywordMap.put("properties", new Integer(SqlParserSymbols.KW_PROPERTIES));
        keywordMap.put("property", new Integer(SqlParserSymbols.KW_PROPERTY));
        keywordMap.put("query", new Integer(SqlParserSymbols.KW_QUERY));
        keywordMap.put("quota", new Integer(SqlParserSymbols.KW_QUOTA));
        keywordMap.put("random", new Integer(SqlParserSymbols.KW_RANDOM));
        keywordMap.put("range", new Integer(SqlParserSymbols.KW_RANGE));
        keywordMap.put("read", new Integer(SqlParserSymbols.KW_READ));
        keywordMap.put("release", new Integer(SqlParserSymbols.KW_RELEASE));
        keywordMap.put("real", new Integer(SqlParserSymbols.KW_DOUBLE));
        keywordMap.put("recover", new Integer(SqlParserSymbols.KW_RECOVER));
        keywordMap.put("regexp", new Integer(SqlParserSymbols.KW_REGEXP));
        keywordMap.put("rename", new Integer(SqlParserSymbols.KW_RENAME));
        keywordMap.put("repair", new Integer(SqlParserSymbols.KW_REPAIR));
        keywordMap.put("repeatable", new Integer(SqlParserSymbols.KW_REPEATABLE));
        keywordMap.put("replace", new Integer(SqlParserSymbols.KW_REPLACE));
        keywordMap.put("replace_if_not_null", new Integer(SqlParserSymbols.KW_REPLACE_IF_NOT_NULL));
        keywordMap.put("replica", new Integer(SqlParserSymbols.KW_REPLICA));
        keywordMap.put("repository", new Integer(SqlParserSymbols.KW_REPOSITORY));
        keywordMap.put("repositories", new Integer(SqlParserSymbols.KW_REPOSITORIES));
        keywordMap.put("resource", new Integer(SqlParserSymbols.KW_RESOURCE));
        keywordMap.put("restore", new Integer(SqlParserSymbols.KW_RESTORE));
        keywordMap.put("returns", new Integer(SqlParserSymbols.KW_RETURNS));
        keywordMap.put("revoke", new Integer(SqlParserSymbols.KW_REVOKE));
        keywordMap.put("right", new Integer(SqlParserSymbols.KW_RIGHT));
        keywordMap.put("rlike", new Integer(SqlParserSymbols.KW_REGEXP));
        keywordMap.put("role", new Integer(SqlParserSymbols.KW_ROLE));
        keywordMap.put("roles", new Integer(SqlParserSymbols.KW_ROLES));
        keywordMap.put("rollback", new Integer(SqlParserSymbols.KW_ROLLBACK));
        keywordMap.put("rollup", new Integer(SqlParserSymbols.KW_ROLLUP));
        keywordMap.put("row", new Integer(SqlParserSymbols.KW_ROW));
        keywordMap.put("rows", new Integer(SqlParserSymbols.KW_ROWS));
        keywordMap.put("schemas", new Integer(SqlParserSymbols.KW_SCHEMAS));
        keywordMap.put("select", new Integer(SqlParserSymbols.KW_SELECT));
        keywordMap.put("semi", new Integer(SqlParserSymbols.KW_SEMI));
        keywordMap.put("serializable", new Integer(SqlParserSymbols.KW_SERIALIZABLE));
        keywordMap.put("session", new Integer(SqlParserSymbols.KW_SESSION));
        keywordMap.put("set", new Integer(SqlParserSymbols.KW_SET));
        keywordMap.put("show", new Integer(SqlParserSymbols.KW_SHOW));
        keywordMap.put("smallint", new Integer(SqlParserSymbols.KW_SMALLINT));
        keywordMap.put("snapshot", new Integer(SqlParserSymbols.KW_SNAPSHOT));
        keywordMap.put("soname", new Integer(SqlParserSymbols.KW_SONAME));
        keywordMap.put("split", new Integer(SqlParserSymbols.KW_SPLIT));
        keywordMap.put("start", new Integer(SqlParserSymbols.KW_START));
        keywordMap.put("status", new Integer(SqlParserSymbols.KW_STATUS));
        keywordMap.put("storage", new Integer(SqlParserSymbols.KW_STORAGE));
        keywordMap.put("string", new Integer(SqlParserSymbols.KW_STRING));
        keywordMap.put("sum", new Integer(SqlParserSymbols.KW_SUM));
        keywordMap.put("superuser", new Integer(SqlParserSymbols.KW_SUPERUSER));
        keywordMap.put("sync", new Integer(SqlParserSymbols.KW_SYNC));
        keywordMap.put("table", new Integer(SqlParserSymbols.KW_TABLE));
        keywordMap.put("tables", new Integer(SqlParserSymbols.KW_TABLES));
        keywordMap.put("tablet", new Integer(SqlParserSymbols.KW_TABLET));
        keywordMap.put("terminated", new Integer(SqlParserSymbols.KW_TERMINATED));
        keywordMap.put("than", new Integer(SqlParserSymbols.KW_THAN));
        keywordMap.put("then", new Integer(SqlParserSymbols.KW_THEN));
        keywordMap.put("timestamp", new Integer(SqlParserSymbols.KW_TIMESTAMP));
        keywordMap.put("tinyint", new Integer(SqlParserSymbols.KW_TINYINT));
        keywordMap.put("to", new Integer(SqlParserSymbols.KW_TO));
        keywordMap.put("transaction", new Integer(SqlParserSymbols.KW_TRANSACTION));
        keywordMap.put("triggers", new Integer(SqlParserSymbols.KW_TRIGGERS));
        keywordMap.put("trim", new Integer(SqlParserSymbols.KW_TRIM));
        keywordMap.put("true", new Integer(SqlParserSymbols.KW_TRUE));
        keywordMap.put("truncate", new Integer(SqlParserSymbols.KW_TRUNCATE));
        keywordMap.put("type", new Integer(SqlParserSymbols.KW_TYPE));
        keywordMap.put("types", new Integer(SqlParserSymbols.KW_TYPES));
        keywordMap.put("unbounded", new Integer(SqlParserSymbols.KW_UNBOUNDED));
        keywordMap.put("uncommitted", new Integer(SqlParserSymbols.KW_UNCOMMITTED));
        keywordMap.put("union", new Integer(SqlParserSymbols.KW_UNION));
        keywordMap.put("unique", new Integer(SqlParserSymbols.KW_UNIQUE));
        keywordMap.put("unsigned", new Integer(SqlParserSymbols.KW_UNSIGNED));
        keywordMap.put("use", new Integer(SqlParserSymbols.KW_USE));
        keywordMap.put("user", new Integer(SqlParserSymbols.KW_USER));
        keywordMap.put("using", new Integer(SqlParserSymbols.KW_USING));
        keywordMap.put("value", new Integer(SqlParserSymbols.KW_VALUE));
        keywordMap.put("values", new Integer(SqlParserSymbols.KW_VALUES));
        keywordMap.put("varchar", new Integer(SqlParserSymbols.KW_VARCHAR));
        keywordMap.put("variables", new Integer(SqlParserSymbols.KW_VARIABLES));
        keywordMap.put("view", new Integer(SqlParserSymbols.KW_VIEW));
        keywordMap.put("warnings", new Integer(SqlParserSymbols.KW_WARNINGS));
        keywordMap.put("whitelist", new Integer(SqlParserSymbols.KW_WHITELIST));
        keywordMap.put("when", new Integer(SqlParserSymbols.KW_WHEN));
        keywordMap.put("where", new Integer(SqlParserSymbols.KW_WHERE));
        keywordMap.put("with", new Integer(SqlParserSymbols.KW_WITH));
        keywordMap.put("work", new Integer(SqlParserSymbols.KW_WORK));
        keywordMap.put("write", new Integer(SqlParserSymbols.KW_WRITE));
        keywordMap.put("||", new Integer(SqlParserSymbols.KW_OR));
   }
    
  // map from token id to token description
  public static final Map<Integer, String> tokenIdMap =
      new HashMap<Integer, String>();
  static {
    Iterator<Map.Entry<String, Integer>> it = keywordMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>) it.next();
      tokenIdMap.put(pairs.getValue(), pairs.getKey().toUpperCase());
    }

    // add non-keyword tokens
    tokenIdMap.put(new Integer(SqlParserSymbols.IDENT), "IDENTIFIER");
    tokenIdMap.put(new Integer(SqlParserSymbols.COMMA), "COMMA");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITNOT), "~");
    tokenIdMap.put(new Integer(SqlParserSymbols.LPAREN), "(");
    tokenIdMap.put(new Integer(SqlParserSymbols.RPAREN), ")");
    tokenIdMap.put(new Integer(SqlParserSymbols.LBRACKET), "[");
    tokenIdMap.put(new Integer(SqlParserSymbols.RBRACKET), "]");
    tokenIdMap.put(new Integer(SqlParserSymbols.SEMICOLON), ";");
    tokenIdMap.put(new Integer(SqlParserSymbols.FLOATINGPOINT_LITERAL),
        "FLOATING POINT LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.INTEGER_LITERAL), "INTEGER LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.LARGE_INTEGER_LITERAL), "INTEGER LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.DECIMAL_LITERAL), "DECIMAL LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.NOT), "!");
    tokenIdMap.put(new Integer(SqlParserSymbols.LESSTHAN), "<");
    tokenIdMap.put(new Integer(SqlParserSymbols.GREATERTHAN), ">");
    tokenIdMap.put(new Integer(SqlParserSymbols.UNMATCHED_STRING_LITERAL),
        "UNMATCHED STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.MOD), "%");
    tokenIdMap.put(new Integer(SqlParserSymbols.ADD), "+");
    tokenIdMap.put(new Integer(SqlParserSymbols.DIVIDE), "/");
    tokenIdMap.put(new Integer(SqlParserSymbols.EQUAL), "=");
    tokenIdMap.put(new Integer(SqlParserSymbols.STAR), "*");
    tokenIdMap.put(new Integer(SqlParserSymbols.AT), "@");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITOR), "|");
    tokenIdMap.put(new Integer(SqlParserSymbols.DOTDOTDOT), "...");
    tokenIdMap.put(new Integer(SqlParserSymbols.DOT), ".");
    tokenIdMap.put(new Integer(SqlParserSymbols.STRING_LITERAL), "STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.EOF), "EOF");
    tokenIdMap.put(new Integer(SqlParserSymbols.SUBTRACT), "-");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITAND), "&");
    tokenIdMap.put(new Integer(SqlParserSymbols.error), "ERROR");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITXOR), "^");
    tokenIdMap.put(new Integer(SqlParserSymbols.NUMERIC_OVERFLOW), "NUMERIC OVERFLOW");
  }

  public static boolean isKeyword(Integer tokenId) {
    String token = tokenIdMap.get(tokenId);
    return keywordMap.containsKey(token);
    /* return keywordMap.containsKey(token.toLowerCase()); */
  }

  public static boolean isKeyword(String str) {
	    return keywordMap.containsKey(str.toLowerCase());
  }

  private Symbol newToken(int id, Object value) {
    return new Symbol(id, yyline+1, yycolumn+1, value);
  }

  private static String escapeBackSlash(String str) {
      StringWriter writer = new StringWriter();
      int strLen = str.length();
      for (int i = 0; i < strLen; ++i) {
          char c = str.charAt(i);
          if (c == '\\' && (i + 1) < strLen) {
              switch (str.charAt(i + 1)) {
              case 'n':
                  writer.append('\n');
                  break;
              case 't':
                  writer.append('\t');
                  break;
              case 'r':
                  writer.append('\r');
                  break;
              case 'b':
                  writer.append('\b');
                  break;
              case '0':
                  writer.append('\0'); // Ascii null
                  break;
              case 'Z': // ^Z must be escaped on Win32
                  writer.append('\032');
                  break;
              case '_':
              case '%':
                  writer.append('\\'); // remember prefix for wildcard
                  /* Fall through */
              default:
                  writer.append(str.charAt(i + 1));
                  break;
              }
              i++;
          } else {
              writer.append(c);
          }
      }

      return writer.toString();
  }
%}

LineTerminator = \r|\n|\r\n
NonTerminator = [^\r\n]
Whitespace = {LineTerminator} | [ \t\f]

IdentifierOrKwContents = [:digit:]*[:jletter:][:jletterdigit:]* | "&&" | "||"

IdentifierOrKw = \`{IdentifierOrKwContents}\` | {IdentifierOrKwContents}
IntegerLiteral = [:digit:][:digit:]*
QuotedIdentifier = \`(\`\`|[^\`])*\`
SingleQuoteStringLiteral = \'(\\.|[^\\\'])*\'
DoubleQuoteStringLiteral = \"(\\.|[^\\\"])*\"

FLit1 = [0-9]+ \. [0-9]*
FLit2 = \. [0-9]+
FLit3 = [0-9]+
Exponent = [eE] [+-]? [0-9]+
DoubleLiteral = ({FLit1}|{FLit2}|{FLit3}) {Exponent}?

EolHintBegin = "--" " "* "+"
CommentedHintBegin = "/*" " "* "+"
CommentedHintEnd = "*/"

// Both types of plan hints must appear within a single line.
HintContent = " "* "+" [^\r\n]*

Comment = {TraditionalComment} | {EndOfLineComment}

// Match anything that has a comment end (*/) in it.
ContainsCommentEnd = [^]* "*/" [^]*
// Match anything that has a line terminator in it.
ContainsLineTerminator = [^]* {LineTerminator} [^]*

// A traditional comment is anything that starts and ends like a comment and has neither a
// plan hint inside nor a CommentEnd (*/).
TraditionalComment = "/*" !({HintContent}|{ContainsCommentEnd}) "*/"
// Similar for a end-of-line comment.
EndOfLineComment = "--" !({HintContent}|{ContainsLineTerminator}) {LineTerminator}?

// This additional state is needed because newlines signal the end of a end-of-line hint
// if one has been started earlier. Hence we need to discern between newlines within and
// outside of end-of-line hints.
%state EOLHINT

%%

"..." { return newToken(SqlParserSymbols.DOTDOTDOT, null); }

// single-character tokens
"," { return newToken(SqlParserSymbols.COMMA, null); }
"." { return newToken(SqlParserSymbols.DOT, null); }
"*" { return newToken(SqlParserSymbols.STAR, null); }
"@" { return newToken(SqlParserSymbols.AT, null); }
"(" { return newToken(SqlParserSymbols.LPAREN, null); }
")" { return newToken(SqlParserSymbols.RPAREN, null); }
";" { return newToken(SqlParserSymbols.SEMICOLON, null); }
"[" { return newToken(SqlParserSymbols.LBRACKET, null); }
"]" { return newToken(SqlParserSymbols.RBRACKET, null); }
"/" { return newToken(SqlParserSymbols.DIVIDE, null); }
"%" { return newToken(SqlParserSymbols.MOD, null); }
"+" { return newToken(SqlParserSymbols.ADD, null); }
"-" { return newToken(SqlParserSymbols.SUBTRACT, null); }
"&" { return newToken(SqlParserSymbols.BITAND, null); }
"|" { return newToken(SqlParserSymbols.BITOR, null); }
"^" { return newToken(SqlParserSymbols.BITXOR, null); }
"~" { return newToken(SqlParserSymbols.BITNOT, null); }
"=" { return newToken(SqlParserSymbols.EQUAL, null); }
":=" { return newToken(SqlParserSymbols.SET_VAR, null); }
"!" { return newToken(SqlParserSymbols.NOT, null); }
"<" { return newToken(SqlParserSymbols.LESSTHAN, null); }
">" { return newToken(SqlParserSymbols.GREATERTHAN, null); }
"\"" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"'" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"`" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }

{QuotedIdentifier} {
    // Remove the quotes
    String trimmedIdent = yytext().substring(1, yytext().length() - 1);
    return newToken(SqlParserSymbols.IDENT, SqlUtils.escapeUnquote(trimmedIdent));
}

{IdentifierOrKw} {
  String text = yytext();
  Integer kw_id = keywordMap.get(text.toLowerCase());
  /* Integer kw_id = keywordMap.get(text); */
  if (kw_id != null) {
    return newToken(kw_id.intValue(), text);
  } else {
    return newToken(SqlParserSymbols.IDENT, text);
  }
}

{SingleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL,
                  escapeBackSlash(yytext().substring(1, yytext().length()-1)));
}

{DoubleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL,
                  escapeBackSlash(yytext().substring(1, yytext().length()-1)));
}

{CommentedHintBegin} {
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_START, null);
}

{CommentedHintEnd} {
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_END, null);
}

{EolHintBegin} {
  yybegin(EOLHINT);
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_START, null);
}

<EOLHINT> {LineTerminator} {
  yybegin(YYINITIAL);
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_END, null);
}

{IntegerLiteral} {
    BigInteger val = null;
    try {
        val = new BigInteger(yytext());
    } catch (NumberFormatException e) {
        return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
    }

    // Note: val is positive, because we do not recognize minus charactor in 'IntegerLiteral'
    // -2^63 will be recognize as largeint(__int128)
    if (val.compareTo(LONG_MAX) <= 0) {
        return newToken(SqlParserSymbols.INTEGER_LITERAL, val.longValue());
    }
    if (val.compareTo(LARGEINT_MAX_ABS) <= 0) {
        return newToken(SqlParserSymbols.LARGE_INTEGER_LITERAL, val.toString());
    }
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
}

{DoubleLiteral} {
  BigDecimal decimal_val;
  try {
    decimal_val = new BigDecimal(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }

  return newToken(SqlParserSymbols.DECIMAL_LITERAL, decimal_val);
}

{Comment} { /* ignore */ }
{Whitespace} { /* ignore */ }
