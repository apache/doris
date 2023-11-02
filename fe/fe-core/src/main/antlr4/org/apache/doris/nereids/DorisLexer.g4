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

// Copied from Apache Spark and modified for Apache Doris

lexer grammar DorisLexer;

@members {
  /**
   * When true, parser should throw ParseExcetion for unclosed bracketed comment.
   */
  public boolean has_unclosed_bracketed_comment = false;

  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  /**
   * This method will be called when we see '/*' and try to match it as a bracketed comment.
   * If the next character is '+', it should be parsed as hint later, and we cannot match
   * it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method will be called when the character stream ends and try to find out the
   * unclosed bracketed comment.
   * If the method be called, it means the end of the entire character stream match,
   * and we set the flag and fail later.
   */
  public void markUnclosedComment() {
    has_unclosed_bracketed_comment = true;
  }
}

SEMICOLON: ';';

LEFT_PAREN: '(';
RIGHT_PAREN: ')';
COMMA: ',';
DOT: '.';
LEFT_BRACKET: '[';
RIGHT_BRACKET: ']';
LEFT_BRACE: '{';
RIGHT_BRACE: '}';

// TODO: add a doc to list reserved words

//============================
// Start of the keywords list
//============================
//--DORIS-KEYWORD-LIST-START
ACCOUNT_LOCK: 'ACCOUNT_LOCK';
ACCOUNT_UNLOCK: 'ACCOUNT_UNLOCK';
ADD: 'ADD';
ADDDATE:'ADDDATE';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
AGG_STATE: 'AGG_STATE';
AGGREGATE: 'AGGREGATE';
ALIAS: 'ALIAS';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
ANALYZED: 'ANALYZED';
AND: 'AND';
ANTI: 'ANTI';
APPEND: 'APPEND';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
AUTHORS: 'AUTHORS';
AUTO: 'AUTO';
AUTO_INCREMENT: 'AUTO_INCREMENT';
BACKEND: 'BACKEND';
BACKENDS: 'BACKENDS';
BACKUP: 'BACKUP';
BEGIN: 'BEGIN';
BETWEEN: 'BETWEEN';
BIGINT: 'BIGINT';
BIN: 'BIN';
BINLOG: 'BINLOG';
BITAND: 'BITAND';
BITMAP: 'BITMAP';
BITMAP_UNION: 'BITMAP_UNION';
BITOR: 'BITOR';
BITXOR: 'BITXOR';
BLOB: 'BLOB';
BOOLEAN: 'BOOLEAN';
BRIEF: 'BRIEF';
BROKER: 'BROKER';
BUCKETS: 'BUCKETS';
BUILD: 'BUILD';
BUILTIN: 'BUILTIN';
BY: 'BY';
CACHED: 'CACHED';
CANCEL: 'CANCEL';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
CHAIN: 'CHAIN';
CHAR: 'CHAR' | 'CHARACTER';
CHARSET: 'CHARSET';
CHECK: 'CHECK';
CLEAN: 'CLEAN';
CLUSTER: 'CLUSTER';
CLUSTERS: 'CLUSTERS';
COLLATE: 'COLLATE';
COLLATION: 'COLLATION';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPACT: 'COMPACT';
COMPLETE: 'COMPLETE';
CONFIG: 'CONFIG';
CONNECTION: 'CONNECTION';
CONNECTION_ID: 'CONNECTION_ID';
CONSISTENT: 'CONSISTENT';
CONVERT: 'CONVERT';
COPY: 'COPY';
COUNT: 'COUNT';
CREATE: 'CREATE';
CREATION: 'CREATION';
CRON: 'CRON';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_CATALOG: 'CURRENT_CATALOG';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATE: 'DATE';
DATE_ADD: 'DATE_ADD';
DATE_CEIL: 'DATE_CEIL';
DATE_DIFF: 'DATE_DIFF';
DATE_FLOOR: 'DATE_FLOOR';
DATE_SUB: 'DATE_SUB';
DATEADD: 'DATEADD';
DATEDIFF: 'DATEDIFF';
DATETIME: 'DATETIME';
DATETIMEV2: 'DATETIMEV2';
DATEV2: 'DATEV2';
DATETIMEV1: 'DATETIMEV1';
DATEV1: 'DATEV1';
DAY: 'DAY';
DAYS_ADD: 'DAYS_ADD';
DAYS_SUB: 'DAYS_SUB';
DECIMAL: 'DECIMAL';
DECIMALV2: 'DECIMALV2';
DECIMALV3: 'DECIMALV3';
DECOMMISSION: 'DECOMMISSION';
DEFAULT: 'DEFAULT';
DEFERRED: 'DEFERRED';
DELETE: 'DELETE';
DEMAND: 'DEMAND';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DIAGNOSE: 'DIAGNOSE';
DISK: 'DISK';
DISTINCT: 'DISTINCT';
DISTINCTPC: 'DISTINCTPC';
DISTINCTPCSA: 'DISTINCTPCSA';
DISTRIBUTED: 'DISTRIBUTED';
DISTRIBUTION: 'DISTRIBUTION';
DIV: 'DIV';
DO: 'DO';
DORIS_INTERNAL_TABLE_ID: 'DORIS_INTERNAL_TABLE_ID';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
DROPP: 'DROPP';
DUPLICATE: 'DUPLICATE';
DYNAMIC: 'DYNAMIC';
ELSE: 'ELSE';
ENABLE: 'ENABLE';
ENCRYPTKEY: 'ENCRYPTKEY';
ENCRYPTKEYS: 'ENCRYPTKEYS';
END: 'END';
ENDS: 'ENDS';
ENGINE: 'ENGINE';
ENGINES: 'ENGINES';
ENTER: 'ENTER';
ERRORS: 'ERRORS';
EVENTS: 'EVENTS';
EVERY: 'EVERY';
EXCEPT: 'EXCEPT';
EXCLUDE: 'EXCLUDE';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPIRED: 'EXPIRED';
EXPLAIN: 'EXPLAIN';
EXPORT: 'EXPORT';
EXTENDED: 'EXTENDED';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
FAILED_LOGIN_ATTEMPTS: 'FAILED_LOGIN_ATTEMPTS';
FALSE: 'FALSE';
FAST: 'FAST';
FEATURE: 'FEATURE';
FIELDS: 'FIELDS';
FILE: 'FILE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FLOAT: 'FLOAT';
FOLLOWER: 'FOLLOWER';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORCE: 'FORCE';
FORMAT: 'FORMAT';
FREE: 'FREE';
FROM: 'FROM';
FRONTEND: 'FRONTEND';
FRONTENDS: 'FRONTENDS';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GLOBAL: 'GLOBAL';
GRANT: 'GRANT';
GRANTS: 'GRANTS';
GRAPH: 'GRAPH';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
GROUPS: 'GROUPS';
HASH: 'HASH';
HAVING: 'HAVING';
HDFS: 'HDFS';
HELP: 'HELP';
HISTOGRAM: 'HISTOGRAM';
HLL: 'HLL';
HLL_UNION: 'HLL_UNION';
HOSTNAME: 'HOSTNAME';
HOUR: 'HOUR';
HUB: 'HUB';
IDENTIFIED: 'IDENTIFIED';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IN: 'IN';
INCREMENTAL: 'INCREMENTAL';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
INFILE: 'INFILE';
INNER: 'INNER';
INSERT: 'INSERT';
INSTALL: 'INSTALL';
INT: 'INT';
INTEGER: 'INTEGER';
INTERMEDIATE: 'INTERMEDIATE';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVERTED: 'INVERTED';
IPV4: 'IPV4';
IPV6: 'IPV6';
IS: 'IS';
IS_NOT_NULL_PRED: 'IS_NOT_NULL_PRED';
IS_NULL_PRED: 'IS_NULL_PRED';
ISNULL: 'ISNULL';
ISOLATION: 'ISOLATION';
JOB: 'JOB';
JOBS: 'JOBS';
JOIN: 'JOIN';
JSON: 'JSON';
JSONB: 'JSONB';
KEY: 'KEY';
KEYS: 'KEYS';
KILL: 'KILL';
LABEL: 'LABEL';
LARGEINT: 'LARGEINT';
LAST: 'LAST';
LATERAL: 'LATERAL';
LDAP: 'LDAP';
LDAP_ADMIN_PASSWORD: 'LDAP_ADMIN_PASSWORD';
LEFT: 'LEFT';
LESS: 'LESS';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LINK: 'LINK';
LIST: 'LIST';
LOAD: 'LOAD';
LOCAL: 'LOCAL';
LOCATION: 'LOCATION';
LOCK: 'LOCK';
LOGICAL: 'LOGICAL';
LOW_PRIORITY: 'LOW_PRIORITY';
MAP: 'MAP';
MATCH: 'MATCH';
MATCH_ALL: 'MATCH_ALL';
MATCH_ANY: 'MATCH_ANY';
MATCH_ELEMENT_EQ: 'ELEMENT_EQ';
MATCH_ELEMENT_GE: 'ELEMENT_GE';
MATCH_ELEMENT_GT: 'ELEMENT_GT';
MATCH_ELEMENT_LE: 'ELEMENT_LE';
MATCH_ELEMENT_LT: 'ELEMENT_LT';
MATCH_PHRASE: 'MATCH_PHRASE';
MATERIALIZED: 'MATERIALIZED';
MAX: 'MAX';
MAXVALUE: 'MAXVALUE';
MEMO:'MEMO';
MERGE: 'MERGE';
MIGRATE: 'MIGRATE';
MIGRATIONS: 'MIGRATIONS';
MIN: 'MIN';
MINUS: 'MINUS';
MINUTE: 'MINUTE';
MODIFY: 'MODIFY';
MONTH: 'MONTH';
MTMV: 'MTMV';
NAME: 'NAME';
NAMES: 'NAMES';
NATURAL: 'NATURAL';
NEGATIVE: 'NEGATIVE';
NEVER: 'NEVER';
NEXT: 'NEXT';
NGRAM_BF: 'NGRAM_BF';
NO: 'NO';
NON_NULLABLE: 'NON_NULLABLE';
NOT: 'NOT';
NULL: 'NULL';
NULLS: 'NULLS';
OBSERVER: 'OBSERVER';
OF: 'OF';
OFFSET: 'OFFSET';
ON: 'ON';
ONLY: 'ONLY';
OPEN: 'OPEN';
OPTIMIZED: 'OPTIMIZED';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
OUTFILE: 'OUTFILE';
OVER: 'OVER';
OVERWRITE: 'OVERWRITE';
PARAMETER: 'PARAMETER';
PARSED: 'PARSED';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PASSWORD: 'PASSWORD';
PASSWORD_EXPIRE: 'PASSWORD_EXPIRE';
PASSWORD_HISTORY: 'PASSWORD_HISTORY';
PASSWORD_LOCK_TIME: 'PASSWORD_LOCK_TIME';
PASSWORD_REUSE: 'PASSWORD_REUSE';
PATH: 'PATH';
PAUSE: 'PAUSE';
PERCENT: 'PERCENT';
PERIOD: 'PERIOD';
PERMISSIVE: 'PERMISSIVE';
PHYSICAL: 'PHYSICAL';
PLAN: 'PLAN';
PLUGIN: 'PLUGIN';
PLUGINS: 'PLUGINS';
POLICY: 'POLICY';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PROC: 'PROC';
PROCEDURE: 'PROCEDURE';
PROCESSLIST: 'PROCESSLIST';
PROFILE: 'PROFILE';
PROPERTIES: 'PROPERTIES';
PROPERTY: 'PROPERTY';
QUANTILE_STATE: 'QUANTILE_STATE';
QUANTILE_UNION: 'QUANTILE_UNION';
QUERY: 'QUERY';
QUOTA: 'QUOTA';
RANDOM: 'RANDOM';
RANGE: 'RANGE';
READ: 'READ';
REAL: 'REAL';
REBALANCE: 'REBALANCE';
RECOVER: 'RECOVER';
RECYCLE: 'RECYCLE';
REFRESH: 'REFRESH';
REGEXP: 'REGEXP';
RELEASE: 'RELEASE';
RENAME: 'RENAME';
REPAIR: 'REPAIR';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
REPLACE_IF_NOT_NULL: 'REPLACE_IF_NOT_NULL';
REPLICA: 'REPLICA';
REPOSITORIES: 'REPOSITORIES';
REPOSITORY: 'REPOSITORY';
RESOURCE: 'RESOURCE';
RESOURCES: 'RESOURCES';
RESTORE: 'RESTORE';
RESTRICTIVE: 'RESTRICTIVE';
RESUME: 'RESUME';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
REWRITTEN: 'REWRITTEN';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROUTINE: 'ROUTINE';
ROW: 'ROW';
ROWS: 'ROWS';
S3: 'S3';
SAMPLE: 'SAMPLE';
SCHEDULER: 'SCHEDULER';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SELECT: 'SELECT';
SEMI: 'SEMI';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHAPE: 'SHAPE';
SHOW: 'SHOW';
SIGNED: 'SIGNED';
SKEW: 'SKEW';
SMALLINT: 'SMALLINT';
SNAPSHOT: 'SNAPSHOT';
SONAME: 'SONAME';
SPLIT: 'SPLIT';
SQL_BLOCK_RULE: 'SQL_BLOCK_RULE';
START: 'START';
STARTS: 'STARTS';
STATS: 'STATS';
STATUS: 'STATUS';
STOP: 'STOP';
STORAGE: 'STORAGE';
STREAM: 'STREAM';
STREAMING: 'STREAMING';
STRING: 'STRING';
STRUCT: 'STRUCT';
SUBDATE: 'SUBDATE';
SUM: 'SUM';
SUPERUSER: 'SUPERUSER';
SWITCH: 'SWITCH';
SYNC: 'SYNC';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TABLET: 'TABLET';
TABLETS: 'TABLETS';
TASK: 'TASK';
TASKS: 'TASKS';
TEMPORARY: 'TEMPORARY';
TERMINATED: 'TERMINATED';
TEXT: 'TEXT';
THAN: 'THAN';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TIMESTAMPADD: 'TIMESTAMPADD';
TIMESTAMPDIFF: 'TIMESTAMPDIFF';
TINYINT: 'TINYINT';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRASH: 'TRASH';
TRIGGERS: 'TRIGGERS';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TYPE: 'TYPE';
TYPECAST: 'TYPE_CAST';
TYPES: 'TYPES';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNINSTALL: 'UNINSTALL';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNLOCK: 'UNLOCK';
UNSIGNED: 'UNSIGNED';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALUE: 'VALUE';
VALUES: 'VALUES';
VARCHAR: 'VARCHAR';
VARIABLES: 'VARIABLES';
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
WARNINGS: 'WARNINGS';
WEEK: 'WEEK';
WHEN: 'WHEN';
WHERE: 'WHERE';
WHITELIST: 'WHITELIST';
WITH: 'WITH';
WORK: 'WORK';
WORKLOAD: 'WORKLOAD';
WRITE: 'WRITE';
YEAR: 'YEAR';
//--DORIS-KEYWORD-LIST-END
//============================
// End of the keywords list
//============================

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
SUBTRACT: '-';
ASTERISK: '*';
SLASH: '/';
MOD: '%';
TILDE: '~';
AMPERSAND: '&';
LOGICALAND: '&&';
LOGICALNOT: '!';
PIPE: '|';
DOUBLEPIPES: '||';
HAT: '^';
COLON: ':';
ARROW: '->';
HINT_START: '/*+';
HINT_END: '*/';
ATSIGN: '@';
DOUBLEATSIGN: '@@';

STRING_LITERAL
    : '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\''
    | '"' ( '\\'. | '""' | ~('"'| '\\') )* '"'
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

LEADING_STRING
    : LEFT_BRACE
    | RIGHT_BRACE
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z$_] // these are the "java letters" below 0x7F
    |   // covers all characters above 0x7F which are not a surrogate
    ~[\u0000-\u007F\uD800-\uDBFF]
     {Character.isJavaIdentifierStart(_input.LA(-1))}?
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? ( BRACKETED_COMMENT | . )*? ('*/' | {markUnclosedComment();} EOF) -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
