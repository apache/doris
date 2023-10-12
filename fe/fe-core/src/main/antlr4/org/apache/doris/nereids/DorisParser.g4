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

parser grammar DorisParser;

options { tokenVocab = DorisLexer; }

@members {
    public boolean doris_legacy_SQL_syntax = true;
}

multiStatements
    : (statement SEMICOLON*)+ EOF
    ;

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : explain? query outFileClause?                                    #statementDefault
    | CREATE ROW POLICY (IF NOT EXISTS)? name=identifier
        ON table=multipartIdentifier
        AS type=(RESTRICTIVE | PERMISSIVE)
        TO (user=userIdentify | ROLE roleName=identifier)
        USING LEFT_PAREN booleanExpression RIGHT_PAREN                 #createRowPolicy
    | explain? INSERT INTO tableName=multipartIdentifier
        (PARTITION partition=identifierList)?  // partition define
        (WITH LABEL labelName=identifier)? cols=identifierList?  // label and columns define
        (LEFT_BRACKET hints=identifierSeq RIGHT_BRACKET)?  // hint define
        query                                                          #insertIntoQuery
    | explain? cte? UPDATE tableName=multipartIdentifier tableAlias
        SET updateAssignmentSeq
        fromClause?
        whereClause                                                    #update
    | explain? cte? DELETE FROM tableName=multipartIdentifier tableAlias
        (PARTITION partition=identifierList)?
        (USING relation (COMMA relation)*)
        whereClause                                                    #delete
    ;

// -----------------Command accessories-----------------

identifierOrText
    : errorCapturingIdentifier
    | STRING_LITERAL
    ;

userIdentify
    : user=identifierOrText (AT (host=identifierOrText | LEFT_PAREN host=identifierOrText RIGHT_PAREN))?
    ;


explain
    : (EXPLAIN planType? | DESC | DESCRIBE)
          level=(VERBOSE | GRAPH | PLAN)?
    ;

planType
    : PARSED
    | ANALYZED
    | REWRITTEN | LOGICAL  // same type
    | OPTIMIZED | PHYSICAL   // same type
    | SHAPE
    | MEMO
    | ALL // default type
    ;

//  -----------------Query-----------------
// add queryOrganization for parse (q1) union (q2) union (q3) order by keys, otherwise 'order' will be recognized to be
// identifier.

outFileClause
    : INTO OUTFILE filePath=constant
        (FORMAT AS format=identifier)?
        (PROPERTIES LEFT_PAREN properties+=tvfProperty (COMMA properties+=tvfProperty)* RIGHT_PAREN)?
    ;

query
    : cte? queryTerm queryOrganization
    ;

queryTerm
    : queryPrimary                                                         #queryTermDefault
    | left=queryTerm operator=(UNION | EXCEPT | INTERSECT)
      setQuantifier? right=queryTerm                                       #setOperation
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

queryPrimary
    : querySpecification                                                   #queryPrimaryDefault
    | LEFT_PAREN query RIGHT_PAREN                                         #subquery
    ;

querySpecification
    : selectClause
      fromClause?
      whereClause?
      aggClause?
      havingClause?
      {doris_legacy_SQL_syntax}? queryOrganization                         #regularQuerySpecification
    ;

cte
    : WITH aliasQuery (COMMA aliasQuery)*
    ;

aliasQuery
    : identifier columnAliases? AS LEFT_PAREN query RIGHT_PAREN
    ;

columnAliases
    : LEFT_PAREN identifier (COMMA identifier)* RIGHT_PAREN
    ;

selectClause
    : SELECT selectHint? DISTINCT? selectColumnClause
    ;

selectColumnClause
    : namedExpressionSeq
    | ASTERISK EXCEPT LEFT_PAREN namedExpressionSeq RIGHT_PAREN
    ;

whereClause
    : WHERE booleanExpression
    ;

fromClause
    : FROM relation (COMMA relation)*
    ;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN joinHint? right=relationPrimary joinCriteria?
    ;

// Just like `opt_plan_hints` in legacy CUP parser.
joinHint
    : LEFT_BRACKET identifier RIGHT_BRACKET                           #bracketJoinHint
    | HINT_START identifier HINT_END                                  #commentJoinHint
    ;

relationHint
    : LEFT_BRACKET identifier (COMMA identifier)* RIGHT_BRACKET       #bracketRelationHint
    | HINT_START identifier (COMMA identifier)* HINT_END              #commentRelationHint
    ;

aggClause
    : GROUP BY groupingElement?
    ;

groupingElement
    : ROLLUP LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | CUBE LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | GROUPING SETS LEFT_PAREN groupingSet (COMMA groupingSet)* RIGHT_PAREN
    | expression (COMMA expression)*
    ;

groupingSet
    : LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    ;

havingClause
    : HAVING booleanExpression
    ;

selectHint: HINT_START hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HINT_END;

hintStatement
    : hintName=identifier LEFT_PAREN parameters+=hintAssignment (COMMA parameters+=hintAssignment)* RIGHT_PAREN
    ;

hintAssignment
    : key=identifierOrText (EQ (constantValue=constant | identifierValue=identifier))?
    ;
    
updateAssignment
    : col=multipartIdentifier EQ (expression | DEFAULT)
    ;
    
updateAssignmentSeq
    : assignments+=updateAssignment (COMMA assignments+=updateAssignment)*
    ;

lateralView
    : LATERAL VIEW functionName=identifier LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
      tableName=identifier AS columnName=identifier
    ;

queryOrganization
    : sortClause? limitClause?
    ;

sortClause
    : ORDER BY sortItem (COMMA sortItem)*
    ;

sortItem
    :  expression ordering = (ASC | DESC)? (NULLS (FIRST | LAST))?
    ;

limitClause
    : (LIMIT limit=INTEGER_VALUE)
    | (LIMIT limit=INTEGER_VALUE OFFSET offset=INTEGER_VALUE)
    | (LIMIT offset=INTEGER_VALUE COMMA limit=INTEGER_VALUE)
    ;

partitionClause
    : PARTITION BY expression (COMMA expression)*
    ;

joinType
    : INNER?
    | CROSS
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT SEMI
    | RIGHT SEMI
    | LEFT ANTI
    | RIGHT ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING identifierList
    ;

identifierList
    : LEFT_PAREN identifierSeq RIGHT_PAREN
    ;

identifierSeq
    : ident+=errorCapturingIdentifier (COMMA ident+=errorCapturingIdentifier)*
    ;

relationPrimary
    : multipartIdentifier specifiedPartition? tableAlias sample? relationHint? lateralView*           #tableName
    | LEFT_PAREN query RIGHT_PAREN tableAlias lateralView*                                    #aliasedQuery
    | tvfName=identifier LEFT_PAREN
      (properties+=tvfProperty (COMMA properties+=tvfProperty)*)?
      RIGHT_PAREN tableAlias                                                                  #tableValuedFunction
    ;

tvfProperty
    : key=tvfPropertyItem EQ value=tvfPropertyItem
    ;

tvfPropertyItem : identifier | constant ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier (DOT parts+=errorCapturingIdentifier)*
    ;

// -----------------Expression-----------------
namedExpression
    : expression (AS? (identifierOrText))?
    ;

namedExpressionSeq
    : namedExpression (COMMA namedExpression)*
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : (LOGICALNOT | NOT) booleanExpression                                          #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                                           #exist
    | (ISNULL | IS_NULL_PRED) LEFT_PAREN valueExpression RIGHT_PAREN                #isnull
    | IS_NOT_NULL_PRED LEFT_PAREN valueExpression RIGHT_PAREN                       #is_not_null_pred
    | valueExpression predicate?                                                    #predicated
    | left=booleanExpression operator=(AND | LOGICALAND) right=booleanExpression    #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=DOUBLEPIPES right=booleanExpression           #doublePipes
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=(LIKE | REGEXP | RLIKE) pattern=valueExpression
    | NOT? kind=(MATCH | MATCH_ANY | MATCH_ALL | MATCH_PHRASE) pattern=valueExpression
    | NOT? kind=IN LEFT_PAREN query RIGHT_PAREN
    | NOT? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | IS NOT? kind=NULL
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(SUBTRACT | PLUS | TILDE) valueExpression                                     #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | MOD) right=valueExpression           #arithmeticBinary
    | left=valueExpression operator=(PLUS | SUBTRACT | DIV | HAT | PIPE | AMPERSAND)
                           right=valueExpression                                             #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    | operator=(BITAND | BITOR | BITXOR) LEFT_PAREN left = valueExpression
                COMMA right = valueExpression RIGHT_PAREN                                    #bitOperation
    ;

datetimeUnit
    : YEAR | MONTH
    | WEEK | DAY
    | HOUR | MINUTE | SECOND
    ;

primaryExpression
    : name=(TIMESTAMPDIFF | DATEDIFF)
            LEFT_PAREN
                unit=datetimeUnit COMMA
                startTimestamp=valueExpression COMMA
                endTimestamp=valueExpression
            RIGHT_PAREN                                                                        #timestampdiff
    | name=(TIMESTAMPADD | DATEADD)
                  LEFT_PAREN
                      unit=datetimeUnit COMMA
                      startTimestamp=valueExpression COMMA
                      endTimestamp=valueExpression
                  RIGHT_PAREN                                                                  #timestampadd
    | name =(ADDDATE | DAYS_ADD | DATE_ADD)
            LEFT_PAREN
                timestamp=valueExpression COMMA
                (INTERVAL unitsAmount=valueExpression unit=datetimeUnit
                | unitsAmount=valueExpression)
            RIGHT_PAREN                                                                        #date_add
    | name=(SUBDATE | DAYS_SUB | DATE_SUB)
            LEFT_PAREN
                timestamp=valueExpression COMMA
                (INTERVAL unitsAmount=valueExpression  unit=datetimeUnit
                | unitsAmount=valueExpression)
            RIGHT_PAREN                                                                        #date_sub
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | name=CAST LEFT_PAREN expression AS dataType RIGHT_PAREN                                  #cast
    | constant                                                                                 #constantDefault
    | ASTERISK                                                                                 #star
    | qualifiedName DOT ASTERISK                                                               #star
    | functionIdentifier LEFT_PAREN ((DISTINCT|ALL)? arguments+=expression
      (COMMA arguments+=expression)* (ORDER BY sortItem (COMMA sortItem)*)?)? RIGHT_PAREN
      (OVER windowSpec)?                                                                        #functionCall
    | LEFT_PAREN query RIGHT_PAREN                                                             #subqueryExpression
    | ATSIGN identifierOrText                                                                        #userVariable
    | DOUBLEATSIGN (kind=(GLOBAL | SESSION) DOT)? identifier                                     #systemVariable
    | identifier                                                                               #columnReference
    | base=primaryExpression DOT fieldName=identifier                                          #dereference
    | LEFT_PAREN expression RIGHT_PAREN                                                        #parenthesizedExpression
    | EXTRACT LEFT_PAREN field=identifier FROM (DATE | TIMESTAMP)?
      source=valueExpression RIGHT_PAREN                                                       #extract
    ;

functionIdentifier 
    : (dbName=identifier DOT)? functionNameIdentifier
    ;

functionNameIdentifier
    : identifier
    | ADD
    | CONNECTION_ID
    | CURRENT_CATALOG
    | CURRENT_USER
    | DATABASE
    | IF
    | LEFT
    | LIKE
    | PASSWORD
    | REGEXP
    | RIGHT
    | SCHEMA
    | TRIM
    | USER
    ;

windowSpec
    // todo: name for windowRef; we haven't support it
    // : name=identifier
    // | LEFT_PAREN name=identifier RIGHT_PAREN
    : LEFT_PAREN
        partitionClause?
        sortClause?
        windowFrame?
        RIGHT_PAREN
    ;

windowFrame
    : frameUnits start=frameBoundary
    | frameUnits BETWEEN start=frameBoundary AND end=frameBoundary
    ;

frameUnits
    : ROWS
    | RANGE
    ;

frameBoundary
    : UNBOUNDED boundType=(PRECEDING | FOLLOWING)
    | boundType=CURRENT ROW
    | expression boundType=(PRECEDING | FOLLOWING)
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

specifiedPartition
    : TEMPORARY? PARTITION (identifier | identifierList)
    | TEMPORARY? PARTITIONS identifierList
    ;

constant
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | type=(DATE | DATEV2 | TIMESTAMP) STRING_LITERAL                                          #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING_LITERAL                                                                           #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE | NSEQ
    ;

booleanValue
    : TRUE | FALSE
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

interval
    : INTERVAL value=expression unit=unitIdentifier
    ;

unitIdentifier
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    ;

dataType
    : primitiveColType (LEFT_PAREN (ASTERISK | INTEGER_VALUE (COMMA INTEGER_VALUE)*) RIGHT_PAREN)?     #primitiveDataType
    ;

primitiveColType:
    | type=TINYINT
    | type=SMALLINT
    | (SIGNED | UNSIGNED)? type=INT
    | type=BIGINT
    | type=LARGEINT
    | type=BOOLEAN
    | type=FLOAT
    | type=DOUBLE
    | type=DATE
    | type=DATETIME
    | type=TIME
    | type=DATEV2
    | type=DATETIMEV2
    | type=BITMAP
    | type=QUANTILE_STATE
    | type=HLL
    | type=AGG_STATE
    | type=STRING
    | type=JSON
    | type=JSONB
    | type=TEXT
    | type=VARCHAR
    | type=CHAR
    | type=DECIMAL
    | type=DECIMALV3
    | type=ALL
    ;

sample
    : TABLESAMPLE LEFT_PAREN sampleMethod? RIGHT_PAREN (REPEATABLE seed=INTEGER_VALUE)?
    ;

sampleMethod
    : percentage=INTEGER_VALUE PERCENT                              #sampleByPercentile
    | INTEGER_VALUE ROWS                                            #sampleByRows
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (SUBTRACT identifier)+    #errorIdent
    |                        #realIdent
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : SUBTRACT? INTEGER_VALUE                    #integerLiteral
    | SUBTRACT? (EXPONENT_VALUE | DECIMAL_VALUE) #decimalLiteral
    ;

// there are 1 kinds of keywords in Doris.
// - Non-reserved keywords:
//     normal version of non-reserved keywords.
// The non-reserved keywords are listed in `nonReserved`.
// TODO: need to stay consistent with the legacy
nonReserved
//--DEFAULT-NON-RESERVED-START
    : ADDDATE
    | AFTER
    | AGG_STATE
    | AGGREGATE
    | ALIAS
    | ANALYZED
    | ARRAY
    | AT
    | AUTHORS
    | BACKENDS
    | BACKUP
    | BEGIN
    | BIN
    | BITAND
    | BITMAP
    | BITMAP_UNION
    | BITOR
    | BITXOR
    | BLOB
    | BOOLEAN
    | BRIEF
    | BROKER
    | BUCKETS
    | BUILD
    | BUILTIN
    | CACHED
    | CATALOG
    | CATALOGS
    | CHAIN
    | CHAR
    | CHARSET
    | CHECK
    | CLUSTER
    | CLUSTERS
    | COLLATION
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPLETE
    | CONFIG
    | CONNECTION
    | CONNECTION_ID
    | CONSISTENT
    | CONVERT
    | COPY
    | COUNT
    | CREATION
    | CRON
    | CURRENT_CATALOG
    | CURRENT_TIMESTAMP
    | DATA
    | DATE
    | DATE_ADD
    | DATE_CEIL
    | DATE_DIFF
    | DATE_FLOOR
    | DATE_SUB
    | DATEADD
    | DATEDIFF
    | DATETIME
    | DATETIMEV2
    | DATEV2
    | DAY
    | DAYS_ADD
    | DAYS_SUB
    | DECIMAL
    | DECIMALV3
    | DEFERRED
    | DEMAND
    | DIAGNOSE
    | DISTINCTPC
    | DISTINCTPCSA
    | DO
    | DYNAMIC
    | ENABLE
    | ENCRYPTKEY
    | ENCRYPTKEYS
    | END
    | ENDS
    | ENGINE
    | ENGINES
    | ERRORS
    | EVENTS
    | EVERY
    | EXCLUDE
    | EXPIRED
    | EXTERNAL
    | FAILED_LOGIN_ATTEMPTS
    | FAST
    | FEATURE
    | FIELDS
    | FILE
    | FILTER
    | FIRST
    | FORMAT
    | FREE
    | FRONTENDS
    | FUNCTION
    | GLOBAL
    | GRAPH
    | GROUPING
    | GROUPS
    | HASH
    | HDFS
    | HELP
    | HISTOGRAM
    | HLL_UNION
    | HOSTNAME
    | HOUR
    | HUB
    | IDENTIFIED
    | IGNORE
    | IMMEDIATE
    | INCREMENTAL
    | INDEXES
    | INVERTED
    | IS_NOT_NULL_PRED
    | IS_NULL_PRED
    | ISNULL
    | ISOLATION
    | JOB
    | JOBS
    | JSON
    | JSONB
    | LABEL
    | LAST
    | LDAP
    | LDAP_ADMIN_PASSWORD
    | LEFT_BRACE
    | LESS
    | LEVEL
    | LINES
    | LINK
    | LOCAL
    | LOCATION
    | LOCK
    | LOGICAL
    | MAP
    | MATERIALIZED
    | MAX
    | MEMO
    | MERGE
    | MIGRATE
    | MIGRATIONS
    | MIN
    | MINUTE
    | MODIFY
    | MONTH
    | MTMV
    | NAME
    | NAMES
    | NEGATIVE
    | NEVER
    | NEXT
    | NGRAM_BF
    | NO
    | NON_NULLABLE
    | NULLS
    | OF
    | OFFSET
    | ONLY
    | OPEN
    | OPTIMIZED
    | PARAMETER
    | PARSED
    | PASSWORD
    | PASSWORD_EXPIRE
    | PASSWORD_HISTORY
    | PASSWORD_LOCK_TIME
    | PASSWORD_REUSE
    | PATH
    | PAUSE
    | PERCENT
    | PERIOD
    | PERMISSIVE
    | PHYSICAL
    | PLAN
    | PLUGIN
    | PLUGINS
    | POLICY
    | PROC
    | PROCESSLIST
    | PROFILE
    | PROPERTIES
    | PROPERTY
    | QUANTILE_STATE
    | QUANTILE_UNION
    | QUERY
    | QUOTA
    | RANDOM
    | RECOVER
    | RECYCLE
    | REFRESH
    | REPEATABLE
    | REPLACE
    | REPLACE_IF_NOT_NULL
    | REPOSITORIES
    | REPOSITORY
    | RESOURCE
    | RESOURCES
    | RESTORE
    | RESTRICTIVE
    | RESUME
    | RETURNS
    | REWRITTEN
    | RIGHT_BRACE
    | RLIKE
    | ROLLBACK
    | ROLLUP
    | ROUTINE
    | S3
    | SAMPLE
    | SCHEDULER
    | SCHEMA
    | SECOND
    | SERIALIZABLE
    | SESSION
    | SHAPE
    | SKEW
    | SNAPSHOT
    | SONAME
    | SPLIT
    | START
    | STARTS
    | STATS
    | STATUS
    | STOP
    | STORAGE
    | STREAM
    | STREAMING
    | STRING
    | STRUCT
    | SUBDATE
    | SUM
    | TABLES
    | TASK
    | TASKS
    | TEMPORARY
    | TEXT
    | THAN
    | TIME
    | TIMESTAMP
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TRANSACTION
    | TRIGGERS
    | TRUNCATE
    | TYPE
    | TYPES
    | UNCOMMITTED
    | UNLOCK
    | USER
    | VALUE
    | VARCHAR
    | VARIABLES
    | VERBOSE
    | VERSION
    | VIEW
    | WARNINGS
    | WEEK
    | WORK
    | YEAR
//--DEFAULT-NON-RESERVED-END
    ;
