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
    : statementBase # statementBaseAlias
    | CALL name=multipartIdentifier LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN #callProcedure
    | (ALTER | CREATE (OR REPLACE)? | REPLACE) (PROCEDURE | PROC) name=multipartIdentifier LEFT_PAREN .*? RIGHT_PAREN .*? #createProcedure
    | DROP (PROCEDURE | PROC) (IF EXISTS)? name=multipartIdentifier #dropProcedure
    | SHOW PROCEDURE STATUS (LIKE pattern=valueExpression | whereClause)? #showProcedureStatus
    | SHOW CREATE PROCEDURE name=multipartIdentifier #showCreateProcedure
    ;

statementBase
    : explain? query outFileClause?     #statementDefault
    | supportedDmlStatement             #supportedDmlStatementAlias
    | supportedCreateStatement          #supportedCreateStatementAlias
    | supportedAlterStatement           #supportedAlterStatementAlias
    | materailizedViewStatement         #materailizedViewStatementAlias
    | constraintStatement               #constraintStatementAlias
    | supportedDropStatement            #supportedDropStatementAlias
    | unsupportedStatement              #unsupported
    ;

unsupportedStatement
    : unsupportedSetStatement
    | unsupportedUseStatement
    ;

materailizedViewStatement
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=multipartIdentifier
        (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)? buildMode?
        (REFRESH refreshMethod? refreshTrigger?)?
        ((DUPLICATE)? KEY keys=identifierList)?
        (COMMENT STRING_LITERAL)?
        (PARTITION BY LEFT_PAREN mvPartition RIGHT_PAREN)?
        (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM)
        (BUCKETS (INTEGER_VALUE | AUTO))?)?
        propertyClause?
        AS query                                                                                #createMTMV
    | REFRESH MATERIALIZED VIEW mvName=multipartIdentifier (partitionSpec | COMPLETE | AUTO)    #refreshMTMV
    | ALTER MATERIALIZED VIEW mvName=multipartIdentifier ((RENAME newName=identifier)
        | (REFRESH (refreshMethod | refreshTrigger | refreshMethod refreshTrigger))
        | REPLACE WITH MATERIALIZED VIEW newName=identifier propertyClause?
        | (SET  LEFT_PAREN fileProperties=propertyItemList RIGHT_PAREN))                        #alterMTMV
    | DROP MATERIALIZED VIEW (IF EXISTS)? mvName=multipartIdentifier                            #dropMTMV
    | PAUSE MATERIALIZED VIEW JOB ON mvName=multipartIdentifier                                 #pauseMTMV
    | RESUME MATERIALIZED VIEW JOB ON mvName=multipartIdentifier                                #resumeMTMV
    | CANCEL MATERIALIZED VIEW TASK taskId=INTEGER_VALUE ON mvName=multipartIdentifier          #cancelMTMVTask
    | SHOW CREATE MATERIALIZED VIEW mvName=multipartIdentifier                                  #showCreateMTMV
    ;

constraintStatement
    : ALTER TABLE table=multipartIdentifier
        ADD CONSTRAINT constraintName=errorCapturingIdentifier
        constraint                                                        #addConstraint
    | ALTER TABLE table=multipartIdentifier
        DROP CONSTRAINT constraintName=errorCapturingIdentifier           #dropConstraint
    | SHOW CONSTRAINTS FROM table=multipartIdentifier                     #showConstraint
    ;

supportedDmlStatement
    : explain? cte? INSERT (INTO | OVERWRITE TABLE)
        (tableName=multipartIdentifier | DORIS_INTERNAL_TABLE_ID LEFT_PAREN tableId=INTEGER_VALUE RIGHT_PAREN)
        partitionSpec?  // partition define
        (WITH LABEL labelName=identifier)? cols=identifierList?  // label and columns define
        (LEFT_BRACKET hints=identifierSeq RIGHT_BRACKET)?  // hint define
        query                                                          #insertTable
    | explain? cte? UPDATE tableName=multipartIdentifier tableAlias
        SET updateAssignmentSeq
        fromClause?
        whereClause?                                                   #update
    | explain? cte? DELETE FROM tableName=multipartIdentifier
        partitionSpec? tableAlias
        (USING relations)?
        whereClause?                                                   #delete
    | LOAD LABEL lableName=identifier
        LEFT_PAREN dataDescs+=dataDesc (COMMA dataDescs+=dataDesc)* RIGHT_PAREN
        (withRemoteStorageSystem)?
        (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)?
        (commentSpec)?                                                 #load
    | LOAD mysqlDataDesc
        (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)?
        (commentSpec)?                                                 #mysqlLoad
    | EXPORT TABLE tableName=multipartIdentifier
        (PARTITION partition=identifierList)?
        (whereClause)?
        TO filePath=STRING_LITERAL
        (propertyClause)?
        (withRemoteStorageSystem)?                                     #export
    ;

supportedCreateStatement
    : CREATE (EXTERNAL)? TABLE (IF NOT EXISTS)? name=multipartIdentifier
        ((ctasCols=identifierList)? | (LEFT_PAREN columnDefs (COMMA indexDefs)? COMMA? RIGHT_PAREN))
        (ENGINE EQ engine=identifier)?
        ((AGGREGATE | UNIQUE | DUPLICATE) KEY keys=identifierList
        (CLUSTER BY clusterKeys=identifierList)?)?
        (COMMENT STRING_LITERAL)?
        (partition=partitionTable)?
        (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM)
        (BUCKETS (INTEGER_VALUE | autoBucket=AUTO))?)?
        (ROLLUP LEFT_PAREN rollupDefs RIGHT_PAREN)?
        properties=propertyClause?
        (BROKER extProperties=propertyClause)?
        (AS query)?                                                       #createTable
    | CREATE VIEW (IF NOT EXISTS)? name=multipartIdentifier
        (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)?
        (COMMENT STRING_LITERAL)? AS query                                #createView
    | CREATE (EXTERNAL)? TABLE (IF NOT EXISTS)? name=multipartIdentifier
        LIKE existedTable=multipartIdentifier
        (WITH ROLLUP (rollupNames=identifierList)?)?                      #createTableLike
    | CREATE ROW POLICY (IF NOT EXISTS)? name=identifier
        ON table=multipartIdentifier
        AS type=(RESTRICTIVE | PERMISSIVE)
        TO (user=userIdentify | ROLE roleName=identifier)
        USING LEFT_PAREN booleanExpression RIGHT_PAREN                 #createRowPolicy
    ;

supportedAlterStatement
    : ALTER VIEW name=multipartIdentifier (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)?
        AS query                                                          #alterView
    ;

supportedDropStatement
    : DROP CATALOG RECYCLE BIN WHERE idType=STRING_LITERAL EQ id=INTEGER_VALUE #dropCatalogRecycleBin
    ;

unsupportedSetStatement
    : SET identifier AS DEFAULT STORAGE VAULT                              #setDefaultStorageVault
    | SET PROPERTY (FOR user=identifierOrText)? propertyItemList           #setUserProperties
    | SET (GLOBAL | LOCAL | SESSION)? identifier EQ (expression | DEFAULT) #setSystemVariableWithType
    | SET variable                                                         #setSystemVariableWithoutType
    | SET (CHAR SET | CHARSET) (charsetName=identifierOrText | DEFAULT)    #setCharset
    | SET NAMES EQ expression                                              #setNames
    | SET (GLOBAL | LOCAL | SESSION)? TRANSACTION
        ( transactionAccessMode
        | isolationLevel
        | transactionAccessMode COMMA isolationLevel
        | isolationLevel COMMA transactionAccessMode)                     #setTransaction
    | SET NAMES (charsetName=identifierOrText | DEFAULT) (COLLATE collateName=identifierOrText | DEFAULT)?    #setCollate
    | SET PASSWORD (FOR userIdentify)? EQ (STRING_LITERAL | (PASSWORD LEFT_PAREN STRING_LITERAL RIGHT_PAREN)) #setPassword
    | SET LDAP_ADMIN_PASSWORD EQ (STRING_LITERAL | (PASSWORD LEFT_PAREN STRING_LITERAL RIGHT_PAREN))          #setLdapAdminPassword
    ;

unsupportedUseStatement
    : USE (catalog=identifier DOT)? database=identifier                              #useDatabase
    | USE ((catalog=identifier DOT)? database=identifier)? ATSIGN cluster=identifier #useCloudCluster
    ;

variable
    : (ATSIGN ATSIGN (GLOBAL | LOCAL | SESSION)?)? identifier EQ (expression | DEFAULT) #setSystemVariable
    | ATSIGN identifier EQ expression #setUserVariable
    ;

transactionAccessMode
    : READ (ONLY | WRITE)
    ;

isolationLevel
    : ISOLATION LEVEL ((READ UNCOMMITTED) | (READ COMMITTED) | (REPEATABLE READ) | (SERIALIZABLE))
    ;

constraint
    : PRIMARY KEY slots=identifierList
    | UNIQUE slots=identifierList
    | FOREIGN KEY slots=identifierList
        REFERENCES referenceTable=multipartIdentifier
        referencedSlots=identifierList
    ;

partitionSpec
    : TEMPORARY? (PARTITION | PARTITIONS) partitions=identifierList
    | TEMPORARY? PARTITION partition=errorCapturingIdentifier
	| (PARTITION | PARTITIONS) LEFT_PAREN ASTERISK RIGHT_PAREN // for auto detect partition in overwriting
	// TODO: support analyze external table partition spec https://github.com/apache/doris/pull/24154
	// | PARTITIONS WITH RECENT
    ;

partitionTable
    : ((autoPartition=AUTO)? PARTITION BY (RANGE | LIST)? partitionList=identityOrFunctionList
       (LEFT_PAREN (partitions=partitionsDef)? RIGHT_PAREN))
    ;

identityOrFunctionList
    : LEFT_PAREN identityOrFunction (COMMA partitions+=identityOrFunction)* RIGHT_PAREN
    ;

identityOrFunction
    : (identifier | functionCallExpression)
    ;

dataDesc
    : ((WITH)? mergeType)? DATA INFILE LEFT_PAREN filePaths+=STRING_LITERAL (COMMA filePath+=STRING_LITERAL)* RIGHT_PAREN
        INTO TABLE tableName=multipartIdentifier
        (PARTITION partition=identifierList)?
        (COLUMNS TERMINATED BY comma=STRING_LITERAL)?
        (LINES TERMINATED BY separator=STRING_LITERAL)?
        (FORMAT AS format=identifierOrStringLiteral)?
        (columns=identifierList)?
        (columnsFromPath=colFromPath)?
        (columnMapping=colMappingList)?
        (preFilter=preFilterClause)?
        (where=whereClause)?
        (deleteOn=deleteOnClause)?
        (sequenceColumn=sequenceColClause)?
        (propertyClause)?
    | ((WITH)? mergeType)? DATA FROM TABLE tableName=multipartIdentifier
        INTO TABLE tableName=multipartIdentifier
        (PARTITION partition=identifierList)?
        (columnMapping=colMappingList)?
        (where=whereClause)?
        (deleteOn=deleteOnClause)?
        (propertyClause)?
    ;

// -----------------Command accessories-----------------
buildMode
    : BUILD (IMMEDIATE | DEFERRED)
    ;

refreshTrigger
    : ON MANUAL
    | ON SCHEDULE refreshSchedule
    | ON COMMIT
    ;

refreshSchedule
    : EVERY INTEGER_VALUE refreshUnit = identifier (STARTS STRING_LITERAL)?
    ;

refreshMethod
    : COMPLETE | AUTO
    ;

mvPartition
    : partitionKey = identifier
    | partitionExpr = functionCallExpression
    ;

identifierOrStringLiteral
    : identifier
    | STRING_LITERAL
    ;

identifierOrText
    : errorCapturingIdentifier
    | STRING_LITERAL
    | LEADING_STRING
    ;

userIdentify
    : user=identifierOrText (ATSIGN (host=identifierOrText | LEFT_PAREN host=identifierOrText RIGHT_PAREN))?
    ;


explain
    : (EXPLAIN planType? | DESC | DESCRIBE)
          level=(VERBOSE | TREE | GRAPH | PLAN)?
          PROCESS?
    ;

planType
    : PARSED
    | ANALYZED
    | REWRITTEN | LOGICAL  // same type
    | OPTIMIZED | PHYSICAL   // same type
    | SHAPE
    | MEMO
    | DISTRIBUTED
    | ALL // default type
    ;

mergeType
    : APPEND
    | DELETE
    | MERGE
    ;

preFilterClause
    : PRECEDING FILTER expression
    ;

deleteOnClause
    : DELETE ON expression
    ;

sequenceColClause
    : ORDER BY identifier
    ;

colFromPath
    : COLUMNS FROM PATH AS identifierList
    ;

colMappingList
    : SET LEFT_PAREN mappingSet+=mappingExpr (COMMA mappingSet+=mappingExpr)* RIGHT_PAREN
    ;

mappingExpr
    : (mappingCol=identifier EQ expression)
    ;

withRemoteStorageSystem
    : resourceDesc
    | WITH S3 LEFT_PAREN
        brokerProperties=propertyItemList
        RIGHT_PAREN
    | WITH HDFS LEFT_PAREN
        brokerProperties=propertyItemList
        RIGHT_PAREN
    | WITH LOCAL LEFT_PAREN
        brokerProperties=propertyItemList
        RIGHT_PAREN
    | WITH BROKER brokerName=identifierOrText
        (LEFT_PAREN
        brokerProperties=propertyItemList
        RIGHT_PAREN)?
    ;

resourceDesc
    : WITH RESOURCE resourceName=identifierOrText (LEFT_PAREN propertyItemList RIGHT_PAREN)?
    ;

mysqlDataDesc
    : DATA (LOCAL booleanValue)?
        INFILE filePath=STRING_LITERAL
        INTO TABLE tableName=multipartIdentifier
        (PARTITION partition=identifierList)?
        (COLUMNS TERMINATED BY comma=STRING_LITERAL)?
        (LINES TERMINATED BY separator=STRING_LITERAL)?
        (skipLines)?
        (columns=identifierList)?
        (colMappingList)?
        (propertyClause)?
    ;

skipLines : IGNORE lines=INTEGER_VALUE LINES | IGNORE lines=INTEGER_VALUE ROWS ;

//  -----------------Query-----------------
// add queryOrganization for parse (q1) union (q2) union (q3) order by keys, otherwise 'order' will be recognized to be
// identifier.

outFileClause
    : INTO OUTFILE filePath=constant
        (FORMAT AS format=identifier)?
        (propertyClause)?
    ;

query
    : cte? queryTerm queryOrganization
    ;

queryTerm
    : queryPrimary                                                         #queryTermDefault
    | left=queryTerm operator=(UNION | EXCEPT | MINUS | INTERSECT)
      setQuantifier? right=queryTerm                                       #setOperation
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

queryPrimary
    : querySpecification                                                   #queryPrimaryDefault
    | LEFT_PAREN query RIGHT_PAREN                                         #subquery
    | inlineTable                                                          #valuesTable
    ;

querySpecification
    : selectClause
      intoClause?
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
    : SELECT selectHint? (DISTINCT|ALL)? selectColumnClause
    ;

selectColumnClause
    : namedExpressionSeq
    | ASTERISK EXCEPT LEFT_PAREN namedExpressionSeq RIGHT_PAREN
    ;

whereClause
    : WHERE booleanExpression
    ;

fromClause
    : FROM relations
    ;

// For PL-SQL
intoClause
    : bulkCollectClause? INTO (tableRow | identifier) (COMMA (tableRow | identifier))*
    ;

bulkCollectClause :
       BULK COLLECT
     ;

tableRow :
      identifier LEFT_PAREN INTEGER_VALUE RIGHT_PAREN
    ;

relations
    : relation (COMMA relation)*
    ;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN distributeType? right=relationPrimary joinCriteria?
    ;

// Just like `opt_plan_hints` in legacy CUP parser.
distributeType
    : LEFT_BRACKET identifier RIGHT_BRACKET                           #bracketDistributeType
    | HINT_START identifier HINT_END                                  #commentDistributeType
    ;

relationHint
    : LEFT_BRACKET identifier (COMMA identifier)* RIGHT_BRACKET       #bracketRelationHint
    | HINT_START identifier (COMMA identifier)* HINT_END              #commentRelationHint
    ;

aggClause
    : GROUP BY groupingElement
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
    : hintName=identifier (LEFT_PAREN parameters+=hintAssignment (COMMA? parameters+=hintAssignment)* RIGHT_PAREN)?
    ;

hintAssignment
    : key=identifierOrText (EQ (constantValue=constant | identifierValue=identifier))?
    | constant
    ;
    
updateAssignment
    : col=multipartIdentifier EQ (expression | DEFAULT)
    ;
    
updateAssignmentSeq
    : assignments+=updateAssignment (COMMA assignments+=updateAssignment)*
    ;

lateralView
    : LATERAL VIEW functionName=identifier LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
      tableName=identifier AS columnNames+=identifier (COMMA columnNames+=identifier)*
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

optScanParams
    : ATSIGN funcName=identifier LEFT_PAREN (properties=propertyItemList)? RIGHT_PAREN
    ;

relationPrimary
    : multipartIdentifier optScanParams? materializedViewName? specifiedPartition?
       tabletList? tableAlias sample? tableSnapshot? relationHint? lateralView*           #tableName
    | LEFT_PAREN query RIGHT_PAREN tableAlias lateralView*                 #aliasedQuery
    | tvfName=identifier LEFT_PAREN
      (properties=propertyItemList)?
      RIGHT_PAREN tableAlias                                               #tableValuedFunction
    | LEFT_PAREN relations RIGHT_PAREN                                     #relationList
    ;

materializedViewName
    : INDEX indexName=identifier
    ;

propertyClause
    : PROPERTIES LEFT_PAREN fileProperties=propertyItemList RIGHT_PAREN
    ;

propertyItemList
    : properties+=propertyItem (COMMA properties+=propertyItem)*
    ;

propertyItem
    : key=propertyKey EQ value=propertyValue
    ;

propertyKey : identifier | constant ;

propertyValue : identifier | constant ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier (DOT parts+=errorCapturingIdentifier)*
    ;
    
// ----------------Create Table Fields----------
simpleColumnDefs
    : cols+=simpleColumnDef (COMMA cols+=simpleColumnDef)*
    ;

simpleColumnDef
    : colName=identifier (COMMENT comment=STRING_LITERAL)?
    ;

columnDefs
    : cols+=columnDef (COMMA cols+=columnDef)*
    ;
    
columnDef
    : colName=identifier type=dataType
        KEY?
        (aggType=aggTypeDef)?
        ((GENERATED ALWAYS)? AS LEFT_PAREN generatedExpr=expression RIGHT_PAREN)?
        ((NOT)? nullable=NULL)?
        (AUTO_INCREMENT (LEFT_PAREN autoIncInitValue=number RIGHT_PAREN)?)?
        (DEFAULT (nullValue=NULL | INTEGER_VALUE | DECIMAL_VALUE | PI | stringValue=STRING_LITERAL
           | CURRENT_DATE | defaultTimestamp=CURRENT_TIMESTAMP (LEFT_PAREN defaultValuePrecision=number RIGHT_PAREN)?))?
        (ON UPDATE CURRENT_TIMESTAMP (LEFT_PAREN onUpdateValuePrecision=number RIGHT_PAREN)?)?
        (COMMENT comment=STRING_LITERAL)?
    ;

indexDefs
    : indexes+=indexDef (COMMA indexes+=indexDef)*
    ;
    
indexDef
    : INDEX indexName=identifier cols=identifierList (USING indexType=(BITMAP | INVERTED | NGRAM_BF))? (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)? (COMMENT comment=STRING_LITERAL)?
    ;
    
partitionsDef
    : partitions+=partitionDef (COMMA partitions+=partitionDef)*
    ;
    
partitionDef
    : (lessThanPartitionDef | fixedPartitionDef | stepPartitionDef | inPartitionDef) (LEFT_PAREN partitionProperties=propertyItemList RIGHT_PAREN)?
    ;
    
lessThanPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier VALUES LESS THAN (MAXVALUE | constantSeq)
    ;
    
fixedPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier VALUES LEFT_BRACKET lower=constantSeq COMMA upper=constantSeq RIGHT_PAREN
    ;

stepPartitionDef
    : FROM from=constantSeq TO to=constantSeq INTERVAL unitsAmount=INTEGER_VALUE unit=datetimeUnit?
    ;

inPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier (VALUES IN ((LEFT_PAREN constantSeqs+=constantSeq
        (COMMA constantSeqs+=constantSeq)* RIGHT_PAREN) | constants=constantSeq))?
    ;
    
constantSeq
    : LEFT_PAREN values+=partitionValueDef (COMMA values+=partitionValueDef)* RIGHT_PAREN
    ;
    
partitionValueDef
    : INTEGER_VALUE | STRING_LITERAL | MAXVALUE | NULL
    ;
    
rollupDefs
    : rollups+=rollupDef (COMMA rollups+=rollupDef)*
    ;
    
rollupDef
    : rollupName=identifier rollupCols=identifierList (DUPLICATE KEY dupKeys=identifierList)? properties=propertyClause?
    ;

aggTypeDef
    : MAX | MIN | SUM | REPLACE | REPLACE_IF_NOT_NULL | HLL_UNION | BITMAP_UNION | QUANTILE_UNION | GENERIC
    ;

tabletList
    : TABLET LEFT_PAREN tabletIdList+=INTEGER_VALUE (COMMA tabletIdList+=INTEGER_VALUE)*  RIGHT_PAREN
    ;
    

inlineTable
    : VALUES rowConstructor (COMMA rowConstructor)*
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
    | lambdaExpression
    ;

lambdaExpression
    : args+=errorCapturingIdentifier ARROW body=booleanExpression
    | LEFT_PAREN
        args+=errorCapturingIdentifier (COMMA args+=errorCapturingIdentifier)+
      RIGHT_PAREN
        ARROW body=booleanExpression
    ;

booleanExpression
    : (LOGICALNOT | NOT) booleanExpression                                         #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                                           #exist
    | (ISNULL | IS_NULL_PRED) LEFT_PAREN valueExpression RIGHT_PAREN                #isnull
    | IS_NOT_NULL_PRED LEFT_PAREN valueExpression RIGHT_PAREN                       #is_not_null_pred
    | valueExpression predicate?                                                    #predicated
    | left=booleanExpression operator=(AND | LOGICALAND) right=booleanExpression    #logicalBinary
    | left=booleanExpression operator=XOR right=booleanExpression                   #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=DOUBLEPIPES right=booleanExpression           #doublePipes
    ;

rowConstructor
    : LEFT_PAREN (rowConstructorItem (COMMA rowConstructorItem)*)? RIGHT_PAREN
    ;

rowConstructorItem
    : namedExpression | DEFAULT
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=(LIKE | REGEXP | RLIKE) pattern=valueExpression
    | NOT? kind=(MATCH | MATCH_ANY | MATCH_ALL | MATCH_PHRASE | MATCH_PHRASE_PREFIX | MATCH_REGEXP | MATCH_PHRASE_EDGE) pattern=valueExpression
    | NOT? kind=IN LEFT_PAREN query RIGHT_PAREN
    | NOT? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | IS NOT? kind=NULL
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(SUBTRACT | PLUS | TILDE) valueExpression                                     #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | MOD | DIV) right=valueExpression     #arithmeticBinary
    | left=valueExpression operator=(PLUS | SUBTRACT | HAT | PIPE | AMPERSAND)
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
    | name=DATE_FLOOR
            LEFT_PAREN
                timestamp=valueExpression COMMA
                (INTERVAL unitsAmount=valueExpression  unit=datetimeUnit
                | unitsAmount=valueExpression)
            RIGHT_PAREN                                                                        #dateFloor 
    | name=DATE_CEIL
            LEFT_PAREN
                timestamp=valueExpression COMMA
                (INTERVAL unitsAmount=valueExpression  unit=datetimeUnit
                | unitsAmount=valueExpression)
            RIGHT_PAREN                                                                        #dateCeil
    | name =(ARRAY_RANGE | SEQUENCE)
            LEFT_PAREN
                start=valueExpression COMMA
                end=valueExpression COMMA
                (INTERVAL unitsAmount=valueExpression unit=datetimeUnit
                | unitsAmount=valueExpression)
            RIGHT_PAREN                                                                        #arrayRange
    | name=CURRENT_DATE                                                                        #currentDate
    | name=CURRENT_TIME                                                                        #currentTime
    | name=CURRENT_TIMESTAMP                                                                   #currentTimestamp
    | name=LOCALTIME                                                                           #localTime
    | name=LOCALTIMESTAMP                                                                      #localTimestamp
    | name=CURRENT_USER                                                                        #currentUser
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | name=CAST LEFT_PAREN expression AS castDataType RIGHT_PAREN                              #cast
    | constant                                                                                 #constantDefault
    | interval                                                                                 #intervalLiteral
    | ASTERISK                                                                                 #star
    | qualifiedName DOT ASTERISK                                                               #star
    | CHAR LEFT_PAREN
                arguments+=expression (COMMA arguments+=expression)*
                (USING charSet=identifierOrText)?
          RIGHT_PAREN                                                                          #charFunction
    | CONVERT LEFT_PAREN argument=expression USING charSet=identifierOrText RIGHT_PAREN        #convertCharSet
    | CONVERT LEFT_PAREN argument=expression COMMA castDataType RIGHT_PAREN                    #convertType
    | functionCallExpression                                                                   #functionCall
    | value=primaryExpression LEFT_BRACKET index=valueExpression RIGHT_BRACKET                 #elementAt
    | value=primaryExpression LEFT_BRACKET begin=valueExpression
      COLON (end=valueExpression)? RIGHT_BRACKET                                               #arraySlice
    | LEFT_PAREN query RIGHT_PAREN                                                             #subqueryExpression
    | ATSIGN identifierOrText                                                                  #userVariable
    | DOUBLEATSIGN (kind=(GLOBAL | SESSION) DOT)? identifier                                   #systemVariable
    | BINARY? identifier                                                                       #columnReference
    | base=primaryExpression DOT fieldName=identifier                                          #dereference
    | LEFT_PAREN expression RIGHT_PAREN                                                        #parenthesizedExpression
    | KEY (dbName=identifier DOT)? keyName=identifier                                          #encryptKey
    | EXTRACT LEFT_PAREN field=identifier FROM (DATE | TIMESTAMP)?
      source=valueExpression RIGHT_PAREN                                                       #extract
    | primaryExpression COLLATE (identifier | STRING_LITERAL | DEFAULT)                        #collate
    ;

castDataType
    : dataType
    |(SIGNED|UNSIGNED) (INT|INTEGER)?
    ;

functionCallExpression
    : functionIdentifier
              LEFT_PAREN (
                  (DISTINCT|ALL)?
                  arguments+=expression (COMMA arguments+=expression)*
                  (ORDER BY sortItem (COMMA sortItem)*)?
              )? RIGHT_PAREN
            (OVER windowSpec)?
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
    | type=(DATE | DATEV1 | DATEV2 | TIMESTAMP) STRING_LITERAL                                 #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | BINARY? STRING_LITERAL                                                                   #stringLiteral
    | LEFT_BRACKET (items+=constant)? (COMMA items+=constant)* RIGHT_BRACKET                   #arrayLiteral
    | LEFT_BRACE (items+=constant COLON items+=constant)?
       (COMMA items+=constant COLON items+=constant)* RIGHT_BRACE                              #mapLiteral
    | LEFT_BRACE items+=constant (COMMA items+=constant)* RIGHT_BRACE                          #structLiteral
    | PLACEHOLDER						                               #placeholder
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

dataTypeWithNullable
    : dataType ((NOT)? NULL)?
    ;

dataType
    : complex=ARRAY LT dataType GT                                  #complexDataType
    | complex=MAP LT dataType COMMA dataType GT                     #complexDataType
    | complex=STRUCT LT complexColTypeList GT                       #complexDataType
    | AGG_STATE LT functionNameIdentifier
        LEFT_PAREN dataTypes+=dataTypeWithNullable
        (COMMA dataTypes+=dataTypeWithNullable)* RIGHT_PAREN GT     #aggStateDataType
    | primitiveColType (LEFT_PAREN (INTEGER_VALUE | ASTERISK)
      (COMMA INTEGER_VALUE)* RIGHT_PAREN)?                          #primitiveDataType
    ;

primitiveColType:
    | type=TINYINT
    | type=SMALLINT
    | type=(INT | INTEGER)
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
    | type=DATEV1
    | type=DATETIMEV1
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
    | type=DECIMALV2
    | type=DECIMALV3
    | type=IPV4
    | type=IPV6
    | type=VARIANT
    | type=ALL
    ;

complexColTypeList
    : complexColType (COMMA complexColType)*
    ;

complexColType
    : identifier COLON dataType commentSpec?
    ;

commentSpec
    : COMMENT STRING_LITERAL
    ;

sample
    : TABLESAMPLE LEFT_PAREN sampleMethod? RIGHT_PAREN (REPEATABLE seed=INTEGER_VALUE)?
    ;

sampleMethod
    : percentage=INTEGER_VALUE PERCENT                              #sampleByPercentile
    | INTEGER_VALUE ROWS                                            #sampleByRows
    ;

tableSnapshot
    : FOR VERSION AS OF version=INTEGER_VALUE
    | FOR TIME AS OF time=STRING_LITERAL
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (SUBTRACT identifier)+ #errorIdent
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
    | ALWAYS
    | ANALYZED
    | ARRAY
    | ARRAY_RANGE
    | AT
    | AUTHORS
    | AUTO_INCREMENT
    | BACKENDS
    | BACKUP
    | BEGIN
    | BELONG
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
    | BULK
    | CACHE
    | CACHED
    | CALL
    | CATALOG
    | CATALOGS
    | CHAIN
    | CHAR
    | CHARSET
    | CHECK
    | CLUSTER
    | CLUSTERS
    | COLLATION
    | COLLECT
    | COLOCATE
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPLETE
    | COMPRESS_TYPE
    | CONFIG
    | CONNECTION
    | CONNECTION_ID
    | CONSISTENT
    | CONSTRAINTS
    | CONVERT
    | CONVERT_LSC
    | COPY
    | COUNT
    | CREATION
    | CRON
    | CURRENT_CATALOG
    | CURRENT_DATE
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURRENT_USER
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
    | DATETIMEV1
    | DATETIMEV2
    | DATEV1
    | DATEV2
    | DAY
    | DAYS_ADD
    | DAYS_SUB
    | DECIMAL
    | DECIMALV2
    | DECIMALV3
    | DEFERRED
    | DEMAND
    | DIAGNOSE
    | DISTINCTPC
    | DISTINCTPCSA
    | DO
    | DORIS_INTERNAL_TABLE_ID
    | DUAL
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
    | GENERATED
    | GENERIC
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
    | HOTSPOT
    | HOUR
    | HUB
    | IDENTIFIED
    | IGNORE
    | IMMEDIATE
    | INCREMENTAL
    | INDEXES
    | INVERTED
    | IPV4
    | IPV6
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
    | LOCALTIME
    | LOCALTIMESTAMP
    | LOCATION
    | LOCK
    | LOGICAL
    | MANUAL
    | MAP
    | MATCH_ALL
    | MATCH_ANY
    | MATCH_PHRASE
    | MATCH_PHRASE_EDGE
    | MATCH_PHRASE_PREFIX
    | MATCH_REGEXP
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
    | PARTITIONS
    | PATH
    | PAUSE
    | PERCENT
    | PERIOD
    | PERMISSIVE
    | PHYSICAL
    | PI
    | PLAN
    | PLUGIN
    | PLUGINS
    | POLICY
    | PRIVILEGES
    | PROC
    | PROCESS
    | PROCESSLIST
    | PROFILE
    | PROPERTIES
    | PROPERTY
    | QUANTILE_STATE
    | QUANTILE_UNION
    | QUERY
    | QUOTA
    | RANDOM
    | RECENT
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
    | SCHEDULE
    | SCHEDULER
    | SCHEMA
    | SECOND
    | SERIALIZABLE
    | SEQUENCE
    | SESSION
    | SHAPE
    | SKEW
    | SNAPSHOT
    | SONAME
    | SPLIT
    | SQL
    | STAGES
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
    | TREE
    | TRIGGERS
    | TRUNCATE
    | TYPE
    | TYPES
    | UNCOMMITTED
    | UNLOCK
    | UP
    | USER
    | VALUE
    | VARCHAR
    | VARIABLES
    | VARIANT
    | VAULT
    | VERBOSE
    | VERSION
    | VIEW
    | WARM
    | WARNINGS
    | WEEK
    | WORK
    | YEAR
//--DEFAULT-NON-RESERVED-END
    ;
