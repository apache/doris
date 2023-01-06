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

multiStatements
    : (statement SEMICOLON*)+ EOF
    ;

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : explain? query                           #statementDefault
    | CREATE ROW POLICY (IF NOT EXISTS)? name=identifier
        ON table=multipartIdentifier
        AS type=(RESTRICTIVE | PERMISSIVE)
        TO user=identifier
        USING LEFT_PAREN booleanExpression RIGHT_PAREN                 #createRowPolicy
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
    | ALL // default type
    ;

//  -----------------Query-----------------
query
    : cte? queryTerm queryOrganization
    ;

queryTerm
    : queryPrimary                                                                       #queryTermDefault
    | left=queryTerm operator=(UNION | EXCEPT | INTERSECT)
      setQuantifier? right=queryTerm                                                     #setOperation
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | TABLE multipartIdentifier                                             #table
    | LEFT_PAREN query RIGHT_PAREN                                          #subquery
    ;

querySpecification
    : selectClause
      fromClause?
      whereClause?
      aggClause?
      havingClause?                                                         #regularQuerySpecification
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
    : LEFT_BRACKET identifier RIGHT_BRACKET                           #bracketStyleHint
    | HINT_START identifier HINT_END                                  #commentStyleHint
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
    : key=identifier (EQ (constantValue=constant | identifierValue=identifier))?
    ;

lateralView
    : LATERAL VIEW functionName=identifier LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
      tableName=identifier AS columnName=identifier
    ;
queryOrganization
    : sortClause? limitClause?
    ;

sortClause
    : (ORDER BY sortItem (COMMA sortItem)*)
    ;

sortItem
    :  expression ordering = (ASC | DESC)? (NULLS (FIRST | LAST))?
    ;

limitClause
    : (LIMIT limit=INTEGER_VALUE)
    | (LIMIT limit=INTEGER_VALUE OFFSET offset=INTEGER_VALUE)
    | (LIMIT offset=INTEGER_VALUE COMMA limit=INTEGER_VALUE)
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
    : multipartIdentifier specifiedPartition? tableAlias lateralView*           #tableName
    | LEFT_PAREN query RIGHT_PAREN tableAlias lateralView*                      #aliasedQuery
    | tvfName=identifier LEFT_PAREN
      (properties+=tvfProperty (COMMA properties+=tvfProperty)*)?
      RIGHT_PAREN tableAlias                                                    #tableValuedFunction
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
    : expression (AS? name=errorCapturingIdentifier)?
    | expression (AS? strName=STRING+)?
    ;

namedExpressionSeq
    : namedExpression (COMMA namedExpression)*
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                         #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                           #exist
    | (ISNULL | IS_NULL_PRED) LEFT_PAREN valueExpression RIGHT_PAREN        #isnull
    | IS_NOT_NULL_PRED LEFT_PAREN valueExpression RIGHT_PAREN        #is_not_null_pred
    | valueExpression predicate?                                    #predicated
    | left=booleanExpression operator=(AND | LOGICALAND) right=booleanExpression    #logicalBinary
    | left=booleanExpression operator=(OR | CONCAT_PIPE) right=booleanExpression    #logicalBinary
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=(LIKE | REGEXP | RLIKE) pattern=valueExpression
    | NOT? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | NOT? kind=IN LEFT_PAREN query RIGHT_PAREN
    | IS NOT? kind=NULL
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression       #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS | DIV | HAT | PIPE | AMPERSAND)
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
    | name=(TIMESTAMPADD | ADDDATE | DAYS_ADD | DATE_ADD)
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
    | identifier LEFT_PAREN ((DISTINCT|ALL)? arguments+=expression
      (COMMA arguments+=expression)*)? RIGHT_PAREN                                             #functionCall
    | LEFT_PAREN query RIGHT_PAREN                                                             #subqueryExpression
    | ATSIGN identifier                                                                        #userVariable
    | DOUBLEATSIGN (kind=(GLOBAL | SESSION) DOT)? identifier                                     #systemVariable
    | identifier                                                                               #columnReference
    | base=primaryExpression DOT fieldName=identifier                                          #dereference
    | LEFT_PAREN expression RIGHT_PAREN                                                        #parenthesizedExpression
    | EXTRACT LEFT_PAREN field=identifier FROM (DATE | TIMESTAMP)?
      source=valueExpression RIGHT_PAREN                                                       #extract
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
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
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
    : identifier (LEFT_PAREN INTEGER_VALUE
      (COMMA INTEGER_VALUE)* RIGHT_PAREN)?                      #primitiveDataType
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
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
    : MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? (EXPONENT_VALUE | DECIMAL_VALUE) #decimalLiteral
    ;

// there are 1 kinds of keywords in Doris.
// - Non-reserved keywords:
//     normal version of non-reserved keywords.
// The non-reserved keywords are listed in `nonReserved`.
// TODO: need to stay consistent with the legacy
nonReserved
//--DEFAULT-NON-RESERVED-START
    : ADD
    | AFTER
    | ALL
    | ALTER
    | ANALYZE
    | ANALYZED
    | AND
    | ANY
    | ARCHIVE
    | ARRAY
    | AS
    | ASC
    | AT
    | AUTHORIZATION
    | AVG
    | BETWEEN
    | BOTH
    | BUCKET
    | BUCKETS
    | BY
    | CACHE
    | CASCADE
    | CASE
    | CAST
    | CATALOG
    | CATALOGS
    | CHANGE
    | CHECK
    | CLEAR
    | CLUSTER
    | CLUSTERED
    | CODEGEN
    | COLLATE
    | COLLECTION
    | COLUMN
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMPACT
    | COMPACTIONS
    | COMPUTE
    | CONCATENATE
    | CONSTRAINT
    | COST
    | CREATE
    | CUBE
    | CURRENT
    | CURRENT_DATE
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURRENT_USER
    | DATA
    | DATABASE
    | DATABASES
    | DATE
    | DATE_ADD
    | DATEDIFF
    | DATE_DIFF
    | DAY
    | DBPROPERTIES
    | DEFINED
    | DELETE
    | DELIMITED
    | DESC
    | DESCRIBE
    | DFS
    | DIRECTORIES
    | DIRECTORY
    | DISTINCT
    | DISTRIBUTE
    | DIV
    | DROP
    | ELSE
    | END
    | ESCAPE
    | ESCAPED
    | EXCHANGE
    | EXISTS
    | EXPLAIN
    | EXPORT
    | EXTENDED
    | EXTERNAL
    | EXTRACT
    | FALSE
    | FETCH
    | FILTER
    | FIELDS
    | FILEFORMAT
    | FIRST
    | FOLLOWING
    | FOR
    | FOREIGN
    | FORMAT
    | FORMATTED
    | FROM
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GRAPH
    | GROUP
    | GROUPING
    | HAVING
    | HOUR
    | IF
    | IGNORE
    | IMPORT
    | IN
    | INDEX
    | INDEXES
    | INPATH
    | INPUTFORMAT
    | INSERT
    | INTERVAL
    | INTO
    | IS
    | ITEMS
    | KEYS
    | LAST
    | LAZY
    | LEADING
    | LIKE
    | ILIKE
    | LIMIT
    | LINES
    | LIST
    | LOAD
    | LOCAL
    | LOCATION
    | LOCK
    | LOCKS
    | LOGICAL
    | MACRO
    | MAP
    | MATCHED
    | MERGE
    | MINUTE
    | MONTH
    | MSCK
    | NAMESPACE
    | NAMESPACES
    | NO
    | NOT
    | NULL
    | NULLS
    | OF
    | ONLY
    | OPTIMIZED
    | OPTION
    | OPTIONS
    | OR
    | ORDER
    | OUT
    | OUTER
    | OUTPUTFORMAT
    | OVER
    | OVERLAPS
    | OVERLAY
    | OVERWRITE
    | PARSED
    | PARTITION
    | PARTITIONED
    | PARTITIONS
    | PERCENTILE_CONT
    | PERCENTLIT
    | PHYSICAL
    | PIVOT
    | PLACING
    | PLAN
    | POLICY
    | POSITION
    | PRECEDING
    | PRIMARY
    | PRINCIPALS
    | PROPERTIES
    | PURGE
    | QUERY
    | RANGE
    | RECORDREADER
    | RECORDWRITER
    | RECOVER
    | REDUCE
    | REFERENCES
    | REFRESH
    | RENAME
    | REPAIR
    | REPEATABLE
    | REPLACE
    | RESET
    | RESPECT
    | RESTRICT
    | REVOKE
    | REWRITTEN
    | RLIKE
    | ROLE
    | ROLES
    | ROLLBACK
    | ROLLUP
    | ROW
    | ROWS
    | SCHEMA
    | SCHEMAS
    | SECOND
    | SELECT
    | SEPARATED
    | SERDE
    | SERDEPROPERTIES
    | SESSION_USER
    | SET
    | SETS
    | SHOW
    | SKEWED
    | SOME
    | SORT
    | SORTED
    | START
    | STATISTICS
    | STORED
    | STRATIFY
    | STRUCT
    | SUBSTR
    | SUBSTRING
    | SUM
    | SYNC
    | SYSTEM_TIME
    | SYSTEM_VERSION
    | TABLE
    | TABLES
    | TABLESAMPLE
    | TBLPROPERTIES
    | TEMPORARY
    | TERMINATED
    | THEN
    | TIME
    | TIMESTAMP
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TO
    | TOUCH
    | TRAILING
    | TRANSACTION
    | TRANSACTIONS
    | TRANSFORM
    | TRIM
    | TRUE
    | TRUNCATE
    | TRY_CAST
    | TYPE
    | UNARCHIVE
    | UNBOUNDED
    | UNCACHE
    | UNIQUE
    | UNKNOWN
    | UNLOCK
    | UNSET
    | UPDATE
    | USE
    | USER
    | VALUES
    | VERBOSE
    | VERSION
    | VIEW
    | VIEWS
    | WHEN
    | WHERE
    | WINDOW
    | WITH
    | WITHIN
    | YEAR
    | ZONE
//--DEFAULT-NON-RESERVED-END
    ;
