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
    public boolean ansiSQLSyntax = false;
}

multiStatements
    : SEMICOLON* statement? (SEMICOLON+ statement)* SEMICOLON* EOF
    ;

singleStatement
    : SEMICOLON* statement? SEMICOLON* EOF
    ;

expressionWithEof
    : expression EOF
    ;

statement
    : statementBase # statementBaseAlias
    | CALL name=multipartIdentifier LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN #callProcedure
    | (ALTER | CREATE (OR REPLACE)? | REPLACE) (PROCEDURE | PROC) name=multipartIdentifier LEFT_PAREN .*? RIGHT_PAREN .*? #createProcedure
    | DROP (PROCEDURE | PROC) (IF EXISTS)? name=multipartIdentifier #dropProcedure
    | SHOW (PROCEDURE | FUNCTION) STATUS (LIKE pattern=valueExpression | whereClause)? #showProcedureStatus
    | SHOW CREATE PROCEDURE name=multipartIdentifier #showCreateProcedure
    // FIXME: like should be wildWhere? FRONTEND should not contain FROM backendid
    | ADMIN? SHOW type=(FRONTEND | BACKEND) CONFIG (LIKE pattern=valueExpression)? (FROM backendId=INTEGER_VALUE)? #showConfig
    ;

statementBase
    : explain? query outFileClause?     #statementDefault
    | supportedDmlStatement             #supportedDmlStatementAlias
    | supportedCreateStatement          #supportedCreateStatementAlias
    | supportedAlterStatement           #supportedAlterStatementAlias
    | materializedViewStatement         #materializedViewStatementAlias
    | supportedJobStatement             #supportedJobStatementAlias
    | constraintStatement               #constraintStatementAlias
    | supportedCleanStatement           #supportedCleanStatementAlias
    | supportedDescribeStatement        #supportedDescribeStatementAlias
    | supportedDropStatement            #supportedDropStatementAlias
    | supportedSetStatement             #supportedSetStatementAlias
    | supportedUnsetStatement           #supportedUnsetStatementAlias
    | supportedRefreshStatement         #supportedRefreshStatementAlias
    | supportedShowStatement            #supportedShowStatementAlias
    | supportedLoadStatement            #supportedLoadStatementAlias
    | supportedCancelStatement          #supportedCancelStatementAlias
    | supportedRecoverStatement         #supportedRecoverStatementAlias
    | supportedAdminStatement           #supportedAdminStatementAlias
    | supportedUseStatement             #supportedUseStatementAlias
    | supportedOtherStatement           #supportedOtherStatementAlias
    | supportedKillStatement            #supportedKillStatementAlias
    | supportedStatsStatement           #supportedStatsStatementAlias
    | supportedTransactionStatement     #supportedTransactionStatementAlias
    | supportedGrantRevokeStatement     #supportedGrantRevokeStatementAlias
    ;

materializedViewStatement
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
    | DROP MATERIALIZED VIEW (IF EXISTS)? mvName=multipartIdentifier
        (ON tableName=multipartIdentifier)?                                                     #dropMV
    | PAUSE MATERIALIZED VIEW JOB ON mvName=multipartIdentifier                                 #pauseMTMV
    | RESUME MATERIALIZED VIEW JOB ON mvName=multipartIdentifier                                #resumeMTMV
    | CANCEL MATERIALIZED VIEW TASK taskId=INTEGER_VALUE ON mvName=multipartIdentifier          #cancelMTMVTask
    | SHOW CREATE MATERIALIZED VIEW mvName=multipartIdentifier                                  #showCreateMTMV
    ;
supportedJobStatement
    : CREATE JOB label=multipartIdentifier ON SCHEDULE
        (
            (EVERY timeInterval=INTEGER_VALUE timeUnit=identifier
            (STARTS (startTime=STRING_LITERAL | CURRENT_TIMESTAMP))?
            (ENDS endsTime=STRING_LITERAL)?)
            |
            (AT (atTime=STRING_LITERAL | CURRENT_TIMESTAMP)))
        commentSpec?
        DO supportedDmlStatement                                                                                                             #createScheduledJob
   | PAUSE JOB WHERE (jobNameKey=identifier) EQ (jobNameValue=STRING_LITERAL)                                                                #pauseJob
   | DROP JOB (IF EXISTS)? WHERE (jobNameKey=identifier) EQ (jobNameValue=STRING_LITERAL)                                                    #dropJob
   | RESUME JOB WHERE (jobNameKey=identifier) EQ (jobNameValue=STRING_LITERAL)                                                               #resumeJob
   | CANCEL TASK WHERE (jobNameKey=identifier) EQ (jobNameValue=STRING_LITERAL) AND (taskIdKey=identifier) EQ (taskIdValue=INTEGER_VALUE)    #cancelJobTask
   ;
constraintStatement
    : ALTER TABLE table=multipartIdentifier
        ADD CONSTRAINT constraintName=errorCapturingIdentifier
        constraint                                                        #addConstraint
    | ALTER TABLE table=multipartIdentifier
        DROP CONSTRAINT constraintName=errorCapturingIdentifier           #dropConstraint
    | SHOW CONSTRAINTS FROM table=multipartIdentifier                     #showConstraint
    ;

optSpecBranch
    : ATSIGN BRANCH LEFT_PAREN name=identifier RIGHT_PAREN
    ;

supportedDmlStatement
    : explain? cte? INSERT (INTO | OVERWRITE TABLE)
        (tableName=multipartIdentifier (optSpecBranch)? | DORIS_INTERNAL_TABLE_ID LEFT_PAREN tableId=INTEGER_VALUE RIGHT_PAREN)
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
    | LOAD LABEL lableName=multipartIdentifier
        LEFT_PAREN dataDescs+=dataDesc (COMMA dataDescs+=dataDesc)* RIGHT_PAREN
        (withRemoteStorageSystem)?
        propertyClause?
        (commentSpec)?                                                 #load
    | EXPORT TABLE tableName=multipartIdentifier
        (PARTITION partition=identifierList)?
        (whereClause)?
        TO filePath=STRING_LITERAL
        (propertyClause)?
        (withRemoteStorageSystem)?                                     #export
    | replayCommand                                                    #replay
    | COPY INTO selectHint? name=multipartIdentifier columns=identifierList? FROM
            (stageAndPattern | (LEFT_PAREN SELECT selectColumnClause
                FROM stageAndPattern whereClause? RIGHT_PAREN))
            properties=propertyClause?                                 #copyInto
    | TRUNCATE TABLE multipartIdentifier specifiedPartition?  FORCE?   #truncateTable
    ;

supportedCreateStatement
    : CREATE (EXTERNAL | TEMPORARY)? TABLE (IF NOT EXISTS)? name=multipartIdentifier
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
    | CREATE (OR REPLACE)? VIEW (IF NOT EXISTS)? name=multipartIdentifier
        (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)?
        (COMMENT STRING_LITERAL)? AS query                                #createView
    | CREATE FILE name=STRING_LITERAL
        ((FROM | IN) database=identifier)? properties=propertyClause            #createFile
    | CREATE (EXTERNAL | TEMPORARY)? TABLE (IF NOT EXISTS)? name=multipartIdentifier
        LIKE existedTable=multipartIdentifier
        (WITH ROLLUP (rollupNames=identifierList)?)?                      #createTableLike
    | CREATE ROLE (IF NOT EXISTS)? name=identifierOrText (COMMENT STRING_LITERAL)?    #createRole
    | CREATE WORKLOAD GROUP (IF NOT EXISTS)?
        name=identifierOrText (FOR computeGroup=identifierOrText)? properties=propertyClause? #createWorkloadGroup
    | CREATE CATALOG (IF NOT EXISTS)? catalogName=identifier
        (WITH RESOURCE resourceName=identifier)?
        (COMMENT STRING_LITERAL)? properties=propertyClause?                    #createCatalog
    | CREATE ROW POLICY (IF NOT EXISTS)? name=identifier
        ON table=multipartIdentifier
        AS type=(RESTRICTIVE | PERMISSIVE)
        TO (user=userIdentify | ROLE roleName=identifierOrText)
        USING LEFT_PAREN booleanExpression RIGHT_PAREN                    #createRowPolicy
    | CREATE STORAGE POLICY (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                              #createStoragePolicy
    | BUILD INDEX name=identifier ON tableName=multipartIdentifier
        partitionSpec?                                                          #buildIndex
    | CREATE INDEX (IF NOT EXISTS)? name=identifier
        ON tableName=multipartIdentifier identifierList
        (USING (BITMAP | NGRAM_BF | INVERTED))?
        properties=propertyClause? (COMMENT STRING_LITERAL)?                    #createIndex
    | CREATE WORKLOAD POLICY (IF NOT EXISTS)? name=identifierOrText
        (CONDITIONS LEFT_PAREN workloadPolicyConditions RIGHT_PAREN)?
        (ACTIONS LEFT_PAREN workloadPolicyActions RIGHT_PAREN)?
        properties=propertyClause?                                              #createWorkloadPolicy
    | CREATE SQL_BLOCK_RULE (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                        #createSqlBlockRule
    | CREATE ENCRYPTKEY (IF NOT EXISTS)? multipartIdentifier AS STRING_LITERAL  #createEncryptkey
    | CREATE statementScope?
            (TABLES | AGGREGATE)? FUNCTION (IF NOT EXISTS)?
            functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN
            RETURNS returnType=dataType (INTERMEDIATE intermediateType=dataType)?
            properties=propertyClause?                                              #createUserDefineFunction
    | CREATE statementScope? ALIAS FUNCTION (IF NOT EXISTS)?
            functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN
            WITH PARAMETER LEFT_PAREN parameters=identifierSeq? RIGHT_PAREN
            AS expression                                                           #createAliasFunction
    | CREATE USER (IF NOT EXISTS)? grantUserIdentify
            (SUPERUSER | DEFAULT ROLE role=STRING_LITERAL)?
            passwordOption commentSpec?                                             #createUser
    | CREATE (DATABASE | SCHEMA) (IF NOT EXISTS)? name=multipartIdentifier
              properties=propertyClause?                                            #createDatabase
    | CREATE (READ ONLY)? REPOSITORY name=identifier WITH storageBackend            #createRepository
    | CREATE EXTERNAL? RESOURCE (IF NOT EXISTS)?
            name=identifierOrText properties=propertyClause?                        #createResource
    | CREATE DICTIONARY (IF NOT EXISTS)? name = multipartIdentifier
		USING source = multipartIdentifier
		LEFT_PAREN dictionaryColumnDefs RIGHT_PAREN
        LAYOUT LEFT_PAREN layoutType=identifier RIGHT_PAREN
        properties=propertyClause?         # createDictionary
    | CREATE STAGE (IF NOT EXISTS)? name=identifier properties=propertyClause?      #createStage
    | CREATE STORAGE VAULT (IF NOT EXISTS)?
        name=identifierOrText properties=propertyClause?                            #createStorageVault
    | CREATE INVERTED INDEX ANALYZER (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                                  #createIndexAnalyzer
    | CREATE INVERTED INDEX TOKENIZER (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                                  #createIndexTokenizer
    | CREATE INVERTED INDEX TOKEN_FILTER (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                                  #createIndexTokenFilter
    ;

dictionaryColumnDefs:
	dictionaryColumnDef (COMMA dictionaryColumnDef)*;

dictionaryColumnDef:
	colName = identifier columnType = (KEY | VALUE) ;

supportedAlterStatement
    : ALTER SYSTEM alterSystemClause                                                        #alterSystem
    | ALTER VIEW name=multipartIdentifier
      (MODIFY commentSpec |
      (LEFT_PAREN cols=simpleColumnDefs RIGHT_PAREN)?
      (COMMENT STRING_LITERAL)? AS query)                                                   #alterView
    | ALTER CATALOG name=identifier RENAME newName=identifier                               #alterCatalogRename
    | ALTER ROLE role=identifierOrText commentSpec                                          #alterRole
    | ALTER STORAGE VAULT name=multipartIdentifier properties=propertyClause                #alterStorageVault
    | ALTER WORKLOAD GROUP name=identifierOrText (FOR computeGroup=identifierOrText)?
        properties=propertyClause?                                                          #alterWorkloadGroup
    | ALTER CATALOG name=identifier SET PROPERTIES
        LEFT_PAREN propertyItemList RIGHT_PAREN                                             #alterCatalogProperties        
    | ALTER WORKLOAD POLICY name=identifierOrText
        properties=propertyClause?                                                          #alterWorkloadPolicy
    | ALTER SQL_BLOCK_RULE name=identifier properties=propertyClause?                       #alterSqlBlockRule
    | ALTER CATALOG name=identifier MODIFY COMMENT comment=STRING_LITERAL                   #alterCatalogComment
    | ALTER DATABASE name=identifier RENAME newName=identifier                              #alterDatabaseRename
    | ALTER STORAGE POLICY name=identifierOrText
        properties=propertyClause                                                           #alterStoragePolicy
    | ALTER TABLE tableName=multipartIdentifier
        alterTableClause (COMMA alterTableClause)*                                          #alterTable
    | ALTER TABLE tableName=multipartIdentifier ADD ROLLUP
        addRollupClause (COMMA addRollupClause)*                                            #alterTableAddRollup
    | ALTER TABLE tableName=multipartIdentifier DROP ROLLUP
        dropRollupClause (COMMA dropRollupClause)*                                          #alterTableDropRollup
    | ALTER TABLE name=multipartIdentifier
        SET LEFT_PAREN propertyItemList RIGHT_PAREN                                         #alterTableProperties
    | ALTER DATABASE name=identifier SET (DATA | REPLICA | TRANSACTION)
            QUOTA (quota=identifier | INTEGER_VALUE)                                        #alterDatabaseSetQuota
    | ALTER DATABASE name=identifier SET PROPERTIES
            LEFT_PAREN propertyItemList RIGHT_PAREN                                         #alterDatabaseProperties
    | ALTER SYSTEM RENAME COMPUTE GROUP name=identifier newName=identifier                  #alterSystemRenameComputeGroup
    | ALTER RESOURCE name=identifierOrText properties=propertyClause?                       #alterResource
    | ALTER REPOSITORY name=identifier properties=propertyClause?                           #alterRepository
    | ALTER ROUTINE LOAD FOR name=multipartIdentifier 
            (loadProperty (COMMA loadProperty)*)?
            properties=propertyClause?
            (FROM type=identifier LEFT_PAREN propertyItemList RIGHT_PAREN)?                 #alterRoutineLoad
    | ALTER COLOCATE GROUP name=multipartIdentifier
        SET LEFT_PAREN propertyItemList RIGHT_PAREN                                         #alterColocateGroup
    | ALTER USER (IF EXISTS)? grantUserIdentify
        passwordOption (COMMENT STRING_LITERAL)?                                            #alterUser
    ;

supportedDropStatement
    : DROP CATALOG RECYCLE BIN WHERE idType=STRING_LITERAL EQ id=INTEGER_VALUE  #dropCatalogRecycleBin
    | DROP ENCRYPTKEY (IF EXISTS)? name=multipartIdentifier                     #dropEncryptkey
    | DROP ROLE (IF EXISTS)? name=identifierOrText                              #dropRole
    | DROP SQL_BLOCK_RULE (IF EXISTS)? identifierSeq                            #dropSqlBlockRule
    | DROP USER (IF EXISTS)? userIdentify                                       #dropUser
    | DROP STORAGE POLICY (IF EXISTS)? name=identifier                          #dropStoragePolicy
    | DROP WORKLOAD GROUP (IF EXISTS)? name=identifierOrText (FOR computeGroup=identifierOrText)?                    #dropWorkloadGroup
    | DROP CATALOG (IF EXISTS)? name=identifier                                 #dropCatalog
    | DROP FILE name=STRING_LITERAL
        ((FROM | IN) database=identifier)? properties=propertyClause            #dropFile
    | DROP WORKLOAD POLICY (IF EXISTS)? name=identifierOrText                   #dropWorkloadPolicy
    | DROP REPOSITORY name=identifier                                           #dropRepository
    | DROP TABLE (IF EXISTS)? name=multipartIdentifier FORCE?                   #dropTable
    | DROP (DATABASE | SCHEMA) (IF EXISTS)? name=multipartIdentifier FORCE?     #dropDatabase
    | DROP statementScope? FUNCTION (IF EXISTS)?
        functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN            #dropFunction
    | DROP INDEX (IF EXISTS)? name=identifier ON tableName=multipartIdentifier  #dropIndex
    | DROP RESOURCE (IF EXISTS)? name=identifierOrText                          #dropResource
    | DROP ROW POLICY (IF EXISTS)? policyName=identifier
        ON tableName=multipartIdentifier
        (FOR (userIdentify | ROLE roleName=identifier))?                        #dropRowPolicy
    | DROP DICTIONARY (IF EXISTS)? name=multipartIdentifier                     #dropDictionary
    | DROP STAGE (IF EXISTS)? name=identifier                                   #dropStage
    | DROP VIEW (IF EXISTS)? name=multipartIdentifier                           #dropView
    | DROP INVERTED INDEX ANALYZER (IF EXISTS)? name=identifier                 #dropIndexAnalyzer
    | DROP INVERTED INDEX TOKENIZER (IF EXISTS)? name=identifier                #dropIndexTokenizer
    | DROP INVERTED INDEX TOKEN_FILTER (IF EXISTS)? name=identifier             #dropIndexTokenFilter
    ;

supportedShowStatement
    : SHOW statementScope? VARIABLES wildWhere?                                     #showVariables
    | SHOW AUTHORS                                                                  #showAuthors
    | SHOW ALTER TABLE (ROLLUP | (MATERIALIZED VIEW) | COLUMN)
        ((FROM | IN) database=multipartIdentifier)? wildWhere?
        sortClause? limitClause?                                                    #showAlterTable
    | SHOW CREATE (DATABASE | SCHEMA) name=multipartIdentifier                      #showCreateDatabase
    | SHOW BACKUP ((FROM | IN) database=identifier)? wildWhere?                     #showBackup
    | SHOW BROKER                                                                   #showBroker
    | SHOW BUILD INDEX ((FROM | IN) database=identifier)?
        wildWhere? sortClause? limitClause?                                         #showBuildIndex
    | SHOW DYNAMIC PARTITION TABLES ((FROM | IN) database=multipartIdentifier)?     #showDynamicPartition
    | SHOW EVENTS ((FROM | IN) database=multipartIdentifier)? wildWhere?            #showEvents
    | SHOW EXPORT ((FROM | IN) database=multipartIdentifier)? wildWhere?
        sortClause? limitClause?                                                    #showExport
    | SHOW LAST INSERT                                                              #showLastInsert
    | SHOW ((CHAR SET) | CHARSET)                                                   #showCharset
    | SHOW DELETE ((FROM | IN) database=multipartIdentifier)?                       #showDelete
    | SHOW CREATE statementScope? FUNCTION functionIdentifier
        LEFT_PAREN functionArguments? RIGHT_PAREN
        ((FROM | IN) database=multipartIdentifier)?                                 #showCreateFunction
    | SHOW FULL? BUILTIN? FUNCTIONS
        ((FROM | IN) database=multipartIdentifier)? (LIKE STRING_LITERAL)?          #showFunctions
    | SHOW GLOBAL FULL? FUNCTIONS (LIKE STRING_LITERAL)?                            #showGlobalFunctions
    | SHOW ALL? GRANTS                                                              #showGrants
    | SHOW GRANTS FOR userIdentify                                                  #showGrantsForUser
    | SHOW CREATE USER userIdentify                                                 #showCreateUser
    | SHOW SNAPSHOT ON repo=identifier wildWhere?                                   #showSnapshot
    | SHOW LOAD PROFILE loadIdPath=STRING_LITERAL? limitClause?                     #showLoadProfile
    | SHOW CREATE REPOSITORY FOR identifier                                         #showCreateRepository
    | SHOW VIEW
        (FROM |IN) tableName=multipartIdentifier
        ((FROM | IN) database=identifier)?                                          #showView
    | SHOW PLUGINS                                                                  #showPlugins
    | SHOW STORAGE (VAULT | VAULTS)                                                 #showStorageVault    
    | SHOW REPOSITORIES                                                             #showRepositories
    | SHOW ENCRYPTKEYS ((FROM | IN) database=multipartIdentifier)?
        (LIKE STRING_LITERAL)?                                                      #showEncryptKeys
    | SHOW BRIEF? CREATE TABLE name=multipartIdentifier                             #showCreateTable
    | SHOW FULL? PROCESSLIST                                                        #showProcessList
    | SHOW TEMPORARY? PARTITIONS FROM tableName=multipartIdentifier
        wildWhere? sortClause? limitClause?                                         #showPartitions
    | SHOW BRIEF? RESTORE ((FROM | IN) database=identifier)? wildWhere?             #showRestore
    | SHOW ROLES                                                                    #showRoles
    | SHOW PARTITION partitionId=INTEGER_VALUE                                      #showPartitionId
    | SHOW PRIVILEGES                                                               #showPrivileges
    | SHOW PROC path=STRING_LITERAL                                                 #showProc
    | SHOW FILE ((FROM | IN) database=multipartIdentifier)?                         #showSmallFiles
    | SHOW STORAGE? ENGINES                                                         #showStorageEngines
    | SHOW CREATE CATALOG name=identifier                                           #showCreateCatalog
    | SHOW CATALOG name=identifier                                                  #showCatalog
    | SHOW CATALOGS wildWhere?                                                      #showCatalogs
    | SHOW PROPERTY (FOR user=identifierOrText)? (LIKE STRING_LITERAL)?             #showUserProperties
    | SHOW ALL PROPERTIES (LIKE STRING_LITERAL)?                                    #showAllProperties
    | SHOW COLLATION wildWhere?                                                     #showCollation
    | SHOW ROW POLICY (FOR (userIdentify | (ROLE role=identifier)))?                #showRowPolicy
    | SHOW STORAGE POLICY (USING (FOR policy=identifierOrText)?)?                   #showStoragePolicy   
    | SHOW SQL_BLOCK_RULE (FOR ruleName=identifier)?                                #showSqlBlockRule
    | SHOW CREATE VIEW name=multipartIdentifier                                     #showCreateView
    | SHOW CREATE STORAGE VAULT identifier                                          #showCreateStorageVault
    | SHOW DATA TYPES                                                               #showDataTypes
    | SHOW DATA (ALL)? (FROM tableName=multipartIdentifier)?
        sortClause? propertyClause?                                                 #showData
    | SHOW CREATE MATERIALIZED VIEW mvName=identifier
        ON tableName=multipartIdentifier                                            #showCreateMaterializedView
    | SHOW (WARNINGS | ERRORS) limitClause?                                         #showWarningErrors
    | SHOW COUNT LEFT_PAREN ASTERISK RIGHT_PAREN (WARNINGS | ERRORS)                #showWarningErrorCount
    | SHOW BACKENDS                                                                 #showBackends
    | SHOW STAGES                                                                   #showStages
    | SHOW REPLICA DISTRIBUTION FROM baseTableRef                                   #showReplicaDistribution
    | SHOW RESOURCES wildWhere? sortClause? limitClause?                            #showResources
    | SHOW STREAM? LOAD ((FROM | IN) database=identifier)? wildWhere?
        sortClause? limitClause?                                                    #showLoad
    | SHOW LOAD WARNINGS ((((FROM | IN) database=identifier)?
        wildWhere? limitClause?) | (ON url=STRING_LITERAL))                         #showLoadWarings
    | SHOW FULL? TRIGGERS ((FROM | IN) database=multipartIdentifier)? wildWhere?    #showTriggers
    | SHOW TABLET DIAGNOSIS tabletId=INTEGER_VALUE                                  #showDiagnoseTablet
    | SHOW OPEN TABLES ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showOpenTables
    | SHOW FRONTENDS name=identifier?                                               #showFrontends
    | SHOW DATABASE databaseId=INTEGER_VALUE                                        #showDatabaseId
    | SHOW FULL? (COLUMNS | FIELDS) (FROM | IN) tableName=multipartIdentifier
        ((FROM | IN) database=multipartIdentifier)? wildWhere?          #showColumns    
    | SHOW TABLE tableId=INTEGER_VALUE                                              #showTableId
    | SHOW TRASH (ON backend=STRING_LITERAL)?                                       #showTrash
    | SHOW TYPECAST ((FROM | IN) database=identifier)?                              #showTypeCast
    | SHOW (CLUSTERS | (COMPUTE GROUPS))                                            #showClusters    
    | SHOW statementScope? STATUS                                                   #showStatus
    | SHOW WHITELIST                                                                #showWhitelist
    | SHOW TABLETS BELONG
        tabletIds+=INTEGER_VALUE (COMMA tabletIds+=INTEGER_VALUE)*                  #showTabletsBelong
    | SHOW DATA SKEW FROM baseTableRef                                              #showDataSkew
    | SHOW TABLE CREATION ((FROM | IN) database=multipartIdentifier)?
        (LIKE STRING_LITERAL)?                                                      #showTableCreation
    | SHOW TABLET STORAGE FORMAT VERBOSE?                                           #showTabletStorageFormat
    | SHOW QUERY PROFILE queryIdPath=STRING_LITERAL? limitClause?                   #showQueryProfile
    | SHOW CONVERT_LSC ((FROM | IN) database=multipartIdentifier)?                  #showConvertLsc
    | SHOW FULL? TABLES ((FROM | IN) database=multipartIdentifier)? wildWhere?      #showTables
    | SHOW FULL? VIEWS ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showViews
    | SHOW TABLE STATUS ((FROM | IN) database=multipartIdentifier)? wildWhere?      #showTableStatus
    | SHOW (DATABASES | SCHEMAS) (FROM catalog=identifier)? wildWhere?              #showDatabases
    | SHOW TABLETS FROM tableName=multipartIdentifier partitionSpec?
        wildWhere? sortClause? limitClause?                                         #showTabletsFromTable
    | SHOW CATALOG RECYCLE BIN (WHERE expression)?                                  #showCatalogRecycleBin
    | SHOW TABLET tabletId=INTEGER_VALUE                                            #showTabletId
    | SHOW DICTIONARIES wildWhere?                                                  #showDictionaries
    | SHOW TRANSACTION ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showTransaction
    | SHOW REPLICA STATUS FROM baseTableRef whereClause?                            #showReplicaStatus
    | SHOW WORKLOAD GROUPS (LIKE STRING_LITERAL)?                                   #showWorkloadGroups
    | SHOW COPY ((FROM | IN) database=identifier)?
        whereClause? sortClause? limitClause?                                       #showCopy
    | SHOW QUERY STATS ((FOR database=identifier)
            | (FROM tableName=multipartIdentifier (ALL VERBOSE?)?))?                #showQueryStats
    | SHOW (KEY | KEYS | INDEX | INDEXES)
        (FROM |IN) tableName=multipartIdentifier
        ((FROM | IN) database=multipartIdentifier)?                                 #showIndex
    | SHOW WARM UP JOB wildWhere?                                                   #showWarmUpJob
    ;

supportedLoadStatement
    : SYNC                                                                          #sync
    | SHOW CREATE LOAD FOR label=multipartIdentifier                                #showCreateLoad    
    | createRoutineLoad                                                             #createRoutineLoadAlias
    | LOAD mysqlDataDesc
        (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)?
        (commentSpec)?                                                              #mysqlLoad
    | SHOW ALL? CREATE ROUTINE LOAD FOR label=multipartIdentifier                   #showCreateRoutineLoad
    | PAUSE ROUTINE LOAD FOR label=multipartIdentifier                              #pauseRoutineLoad
    | PAUSE ALL ROUTINE LOAD                                                        #pauseAllRoutineLoad
    | RESUME ROUTINE LOAD FOR label=multipartIdentifier                             #resumeRoutineLoad
    | RESUME ALL ROUTINE LOAD                                                       #resumeAllRoutineLoad
    | STOP ROUTINE LOAD FOR label=multipartIdentifier                               #stopRoutineLoad
    | SHOW ALL? ROUTINE LOAD ((FOR label=multipartIdentifier) | (LIKE STRING_LITERAL)?)         #showRoutineLoad
    | SHOW ROUTINE LOAD TASK ((FROM | IN) database=identifier)? wildWhere?          #showRoutineLoadTask
    | SHOW INVERTED INDEX ANALYZER                                                  #showIndexAnalyzer
    | SHOW INVERTED INDEX TOKENIZER                                                 #showIndexTokenizer
    | SHOW INVERTED INDEX TOKEN_FILTER                                              #showIndexTokenFilter
    ;

supportedKillStatement
    : KILL (CONNECTION)? INTEGER_VALUE                                              #killConnection
    | KILL QUERY (INTEGER_VALUE | STRING_LITERAL)                                   #killQuery
    ;

supportedOtherStatement
    : HELP mark=identifierOrText                                                    #help
    | UNLOCK TABLES                                                                 #unlockTables
    | INSTALL PLUGIN FROM source=identifierOrText properties=propertyClause?        #installPlugin
    | UNINSTALL PLUGIN name=identifierOrText                                        #uninstallPlugin
    | LOCK TABLES (lockTable (COMMA lockTable)*)?                                   #lockTables
    | RESTORE SNAPSHOT label=multipartIdentifier FROM repo=identifier
        ((ON | EXCLUDE) LEFT_PAREN baseTableRef (COMMA baseTableRef)* RIGHT_PAREN)?
        properties=propertyClause?                                                  #restore
    | WARM UP (CLUSTER | COMPUTE GROUP) destination=identifier WITH
        ((CLUSTER | COMPUTE GROUP) source=identifier |
            (warmUpItem (AND warmUpItem)*)) FORCE?
            properties=propertyClause?                                              #warmUpCluster
    | BACKUP SNAPSHOT label=multipartIdentifier TO repo=identifier
        ((ON | EXCLUDE) LEFT_PAREN baseTableRef (COMMA baseTableRef)* RIGHT_PAREN)?
        properties=propertyClause?                                                  #backup
    | START TRANSACTION (WITH CONSISTENT SNAPSHOT)?                                 #unsupportedStartTransaction
    ;

 warmUpItem
    : TABLE tableName=multipartIdentifier (PARTITION partitionName=identifier)?
    ;

lockTable
    : name=multipartIdentifier (AS alias=identifierOrText)?
        (READ (LOCAL)? | (LOW_PRIORITY)? WRITE)
    ;

createRoutineLoad
    : CREATE ROUTINE LOAD label=multipartIdentifier (ON table=identifier)?
              (WITH (APPEND | DELETE | MERGE))?
              (loadProperty (COMMA loadProperty)*)? propertyClause? FROM type=identifier
              LEFT_PAREN customProperties=propertyItemList RIGHT_PAREN
              commentSpec?
    ;

loadProperty
    : COLUMNS TERMINATED BY STRING_LITERAL                                          #separator
    | importColumnsStatement                                                        #importColumns
    | importPrecedingFilterStatement                                                #importPrecedingFilter
    | importWhereStatement                                                          #importWhere
    | importDeleteOnStatement                                                       #importDeleteOn
    | importSequenceStatement                                                       #importSequence
    | partitionSpec                                                                 #importPartitions
    ;

importSequenceStatement
    : ORDER BY identifier
    ;

importDeleteOnStatement
    : DELETE ON booleanExpression
    ;

importWhereStatement
    : WHERE booleanExpression
    ;

importPrecedingFilterStatement
    : PRECEDING FILTER booleanExpression
    ;

importColumnsStatement
    : COLUMNS LEFT_PAREN importColumnDesc (COMMA importColumnDesc)* RIGHT_PAREN
    ;

importColumnDesc
    : name=identifier (EQ booleanExpression)?
    | LEFT_PAREN name=identifier (EQ booleanExpression)? RIGHT_PAREN
    ;

supportedRefreshStatement
    : REFRESH CATALOG name=identifier propertyClause?                               #refreshCatalog
    | REFRESH DATABASE name=multipartIdentifier propertyClause?                     #refreshDatabase
    | REFRESH TABLE name=multipartIdentifier                                        #refreshTable
    | REFRESH DICTIONARY name=multipartIdentifier                                   #refreshDictionary
    | REFRESH LDAP (ALL | (FOR user=identifierOrText))                              #refreshLdap
    ;

supportedCleanStatement
    : CLEAN ALL PROFILE                                                             #cleanAllProfile
    | CLEAN LABEL label=identifier? (FROM | IN) database=identifier                 #cleanLabel
    | CLEAN QUERY STATS ((FOR database=identifier)
        | ((FROM | IN) table=multipartIdentifier))                                  #cleanQueryStats
    | CLEAN ALL QUERY STATS                                                         #cleanAllQueryStats
    ;

supportedCancelStatement
    : CANCEL LOAD ((FROM | IN) database=identifier)? wildWhere?                     #cancelLoad
    | CANCEL EXPORT ((FROM | IN) database=identifier)? wildWhere?                   #cancelExport
    | CANCEL WARM UP JOB wildWhere?                                                 #cancelWarmUpJob
    | CANCEL DECOMMISSION BACKEND hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #cancelDecommisionBackend
    | CANCEL BACKUP ((FROM | IN) database=identifier)?                              #cancelBackup
    | CANCEL RESTORE ((FROM | IN) database=identifier)?                             #cancelRestore
    | CANCEL BUILD INDEX ON tableName=multipartIdentifier
        (LEFT_PAREN jobIds+=INTEGER_VALUE
            (COMMA jobIds+=INTEGER_VALUE)* RIGHT_PAREN)?                            #cancelBuildIndex
    | CANCEL ALTER TABLE (ROLLUP | (MATERIALIZED VIEW) | COLUMN)
        FROM tableName=multipartIdentifier (LEFT_PAREN jobIds+=INTEGER_VALUE
            (COMMA jobIds+=INTEGER_VALUE)* RIGHT_PAREN)?                            #cancelAlterTable
    ;

supportedAdminStatement
    : ADMIN SHOW REPLICA DISTRIBUTION FROM baseTableRef                             #adminShowReplicaDistribution
    | ADMIN REBALANCE DISK (ON LEFT_PAREN backends+=STRING_LITERAL
        (COMMA backends+=STRING_LITERAL)* RIGHT_PAREN)?                             #adminRebalanceDisk
    | ADMIN CANCEL REBALANCE DISK (ON LEFT_PAREN backends+=STRING_LITERAL
        (COMMA backends+=STRING_LITERAL)* RIGHT_PAREN)?                             #adminCancelRebalanceDisk
    | ADMIN DIAGNOSE TABLET tabletId=INTEGER_VALUE                                  #adminDiagnoseTablet
    | ADMIN SHOW REPLICA STATUS FROM baseTableRef (WHERE STATUS EQ|NEQ STRING_LITERAL)?   #adminShowReplicaStatus
    | ADMIN COMPACT TABLE baseTableRef (WHERE TYPE EQ STRING_LITERAL)?              #adminCompactTable
    | ADMIN CHECK tabletList properties=propertyClause?                             #adminCheckTablets
    | ADMIN SHOW TABLET STORAGE FORMAT VERBOSE?                                     #adminShowTabletStorageFormat
    | ADMIN SET (FRONTEND | (ALL FRONTENDS)) CONFIG
        (LEFT_PAREN propertyItemList RIGHT_PAREN)? ALL?                             #adminSetFrontendConfig
    | ADMIN CLEAN TRASH
        (ON LEFT_PAREN backends+=STRING_LITERAL
              (COMMA backends+=STRING_LITERAL)* RIGHT_PAREN)?                       #adminCleanTrash
    | ADMIN SET REPLICA VERSION PROPERTIES LEFT_PAREN propertyItemList RIGHT_PAREN  #adminSetReplicaVersion
    | ADMIN SET TABLE name=multipartIdentifier STATUS properties=propertyClause?    #adminSetTableStatus
    | ADMIN SET REPLICA STATUS PROPERTIES LEFT_PAREN propertyItemList RIGHT_PAREN   #adminSetReplicaStatus
    | ADMIN REPAIR TABLE baseTableRef                                               #adminRepairTable
    | ADMIN CANCEL REPAIR TABLE baseTableRef                                        #adminCancelRepairTable
    | ADMIN COPY TABLET tabletId=INTEGER_VALUE properties=propertyClause?           #adminCopyTablet
    | ADMIN SET ENCRYPTION ROOT KEY PROPERTIES LEFT_PAREN propertyItemList RIGHT_PAREN   #adminSetEncryptionRootKey
    | ADMIN SET TABLE name=multipartIdentifier
        PARTITION VERSION properties=propertyClause?                                #adminSetPartitionVersion
    ;

supportedRecoverStatement
    : RECOVER DATABASE name=identifier id=INTEGER_VALUE? (AS alias=identifier)?     #recoverDatabase
    | RECOVER TABLE name=multipartIdentifier
        id=INTEGER_VALUE? (AS alias=identifier)?                                    #recoverTable
    | RECOVER PARTITION name=identifier id=INTEGER_VALUE? (AS alias=identifier)?
        FROM tableName=multipartIdentifier                                          #recoverPartition
    ;

baseTableRef
    : multipartIdentifier optScanParams? tableSnapshot? specifiedPartition?
        tabletList? tableAlias sample? relationHint?
    ;

wildWhere
    : LIKE STRING_LITERAL
    | WHERE expression
    ;

supportedTransactionStatement
    : BEGIN (WITH LABEL identifier?)?                                               #transactionBegin
    | COMMIT WORK? (AND NO? CHAIN)? (NO? RELEASE)?                                  #transcationCommit
    | ROLLBACK WORK? (AND NO? CHAIN)? (NO? RELEASE)?                                #transactionRollback
    ;

supportedGrantRevokeStatement
    : GRANT privilegeList ON multipartIdentifierOrAsterisk
        TO (userIdentify | ROLE identifierOrText)                                           #grantTablePrivilege
    | GRANT privilegeList ON
        (RESOURCE | CLUSTER | COMPUTE GROUP | STAGE | STORAGE VAULT | WORKLOAD GROUP)
        identifierOrTextOrAsterisk TO (userIdentify | ROLE identifierOrText)                #grantResourcePrivilege
    | GRANT roles+=identifierOrText (COMMA roles+=identifierOrText)* TO userIdentify        #grantRole
    | REVOKE roles+=identifierOrText (COMMA roles+=identifierOrText)* FROM userIdentify     #revokeRole
    | REVOKE privilegeList ON
        (RESOURCE | CLUSTER | COMPUTE GROUP | STAGE | STORAGE VAULT | WORKLOAD GROUP)
        identifierOrTextOrAsterisk FROM (userIdentify | ROLE identifierOrText)              #revokeResourcePrivilege
    | REVOKE privilegeList ON multipartIdentifierOrAsterisk
        FROM (userIdentify | ROLE identifierOrText)                                         #revokeTablePrivilege
    ;

privilege
    : name=identifier columns=identifierList?
    | ALL
    ;

privilegeList
    : privilege (COMMA privilege)*
    ;

alterSystemClause
    : ADD BACKEND hostPorts+=STRING_LITERAL (COMMA hostPorts+=STRING_LITERAL)*
        properties=propertyClause?                                                  #addBackendClause
    | (DROP | DROPP) BACKEND hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #dropBackendClause
    | DECOMMISSION BACKEND hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #decommissionBackendClause
    | ADD OBSERVER hostPort=STRING_LITERAL                                          #addObserverClause
    | DROP OBSERVER hostPort=STRING_LITERAL                                         #dropObserverClause
    | ADD FOLLOWER hostPort=STRING_LITERAL                                          #addFollowerClause
    | DROP FOLLOWER hostPort=STRING_LITERAL                                         #dropFollowerClause
    | ADD BROKER name=identifierOrText hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #addBrokerClause
    | DROP BROKER name=identifierOrText hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #dropBrokerClause
    | DROP ALL BROKER name=identifierOrText                                         #dropAllBrokerClause
    | SET LOAD ERRORS HUB properties=propertyClause?                                #alterLoadErrorUrlClause
    | MODIFY BACKEND hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*
        SET LEFT_PAREN propertyItemList RIGHT_PAREN                                 #modifyBackendClause
    | MODIFY (FRONTEND | BACKEND) hostPort=STRING_LITERAL
        HOSTNAME hostName=STRING_LITERAL                                            #modifyFrontendOrBackendHostNameClause
    ;

dropRollupClause
    : rollupName=identifier properties=propertyClause?
    ;

addRollupClause
    : rollupName=identifier columns=identifierList
        (DUPLICATE KEY dupKeys=identifierList)? fromRollup?
        properties=propertyClause?
    ;

alterTableClause
    : ADD COLUMN columnDef columnPosition? toRollup? properties=propertyClause?     #addColumnClause
    | ADD COLUMN LEFT_PAREN columnDefs RIGHT_PAREN
        toRollup? properties=propertyClause?                                        #addColumnsClause
    | DROP COLUMN name=identifier fromRollup? properties=propertyClause?            #dropColumnClause
    | MODIFY COLUMN columnDef columnPosition? fromRollup?
    properties=propertyClause?                                                      #modifyColumnClause
    | ORDER BY identifierList fromRollup? properties=propertyClause?                #reorderColumnsClause
    | ADD TEMPORARY? partitionDef
        (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM)
            (BUCKETS (INTEGER_VALUE | autoBucket=AUTO))?)?
        properties=propertyClause?                                                  #addPartitionClause
    | DROP TEMPORARY? PARTITION (IF EXISTS)? partitionName=identifier FORCE?
        (FROM INDEX indexName=identifier)?                                          #dropPartitionClause
    | MODIFY TEMPORARY? PARTITION
        (partitionName=identifier | partitionNames=identifierList
            | LEFT_PAREN ASTERISK RIGHT_PAREN)
        SET LEFT_PAREN partitionProperties=propertyItemList RIGHT_PAREN             #modifyPartitionClause
    | REPLACE partitions=partitionSpec? WITH tempPartitions=partitionSpec?
        FORCE? properties=propertyClause?                                           #replacePartitionClause
    | REPLACE WITH TABLE name=identifier properties=propertyClause?  FORCE?         #replaceTableClause
    | RENAME newName=identifier                                                     #renameClause
    | RENAME ROLLUP name=identifier newName=identifier                              #renameRollupClause
    | RENAME PARTITION name=identifier newName=identifier                           #renamePartitionClause
    | RENAME COLUMN name=identifier newName=identifier                              #renameColumnClause
    | ADD indexDef                                                                  #addIndexClause
    | DROP INDEX (IF EXISTS)? name=identifier                                       #dropIndexClause
    | ENABLE FEATURE name=STRING_LITERAL (WITH properties=propertyClause)?          #enableFeatureClause
    | MODIFY DISTRIBUTION (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM)
        (BUCKETS (INTEGER_VALUE | autoBucket=AUTO))?)?                              #modifyDistributionClause
    | MODIFY COMMENT comment=STRING_LITERAL                                         #modifyTableCommentClause
    | MODIFY COLUMN name=identifier COMMENT comment=STRING_LITERAL                  #modifyColumnCommentClause
    | MODIFY ENGINE TO name=identifier properties=propertyClause?                   #modifyEngineClause
    | ADD TEMPORARY? PARTITIONS
        FROM from=partitionValueList TO to=partitionValueList
        INTERVAL INTEGER_VALUE unit=identifier? properties=propertyClause?          #alterMultiPartitionClause
    | createOrReplaceTagClause                                                      #createOrReplaceTagClauses
    | createOrReplaceBranchClause                                                   #createOrReplaceBranchClauses
    | dropBranchClause                                                              #dropBranchClauses
    | dropTagClause                                                                 #dropTagClauses
    ;

createOrReplaceTagClause
    : CREATE TAG (IF NOT EXISTS)? name=identifier ops=tagOptions
    | (CREATE OR)? REPLACE TAG name=identifier ops=tagOptions
    ;

createOrReplaceBranchClause
    : CREATE BRANCH (IF NOT EXISTS)? name=identifier ops=branchOptions
    | (CREATE OR)? REPLACE BRANCH name=identifier ops=branchOptions
    ;

tagOptions
    : (AS OF VERSION version=INTEGER_VALUE)? (retainTime)?
    ;

branchOptions
    : (AS OF VERSION version=INTEGER_VALUE)? (retainTime)? (retentionSnapshot)?
    ;

retainTime
    : RETAIN timeValueWithUnit
    ;

retentionSnapshot
    : WITH SNAPSHOT RETENTION minSnapshotsToKeep
    | WITH SNAPSHOT RETENTION timeValueWithUnit
    | WITH SNAPSHOT RETENTION minSnapshotsToKeep timeValueWithUnit
    ;

minSnapshotsToKeep
    : value=INTEGER_VALUE SNAPSHOTS
    ;

timeValueWithUnit
    : timeValue=INTEGER_VALUE  timeUnit=(DAYS | HOURS | MINUTES)
    ;

dropBranchClause
    : DROP BRANCH (IF EXISTS)? name=identifier
    ;

dropTagClause
    : DROP TAG (IF EXISTS)? name=identifier
    ;

columnPosition
    : FIRST
    | AFTER position=identifier
    ;

toRollup
    : (TO | IN) rollup=identifier
    ;

fromRollup
    : FROM rollup=identifier
    ;

supportedStatsStatement
    : SHOW AUTO? ANALYZE (jobId=INTEGER_VALUE | tableName=multipartIdentifier)?
        (WHERE (stateKey=identifier) EQ (stateValue=STRING_LITERAL))?           #showAnalyze
    | SHOW QUEUED ANALYZE JOBS tableName=multipartIdentifier?
        (WHERE (stateKey=identifier) EQ (stateValue=STRING_LITERAL))?           #showQueuedAnalyzeJobs
    | SHOW COLUMN HISTOGRAM tableName=multipartIdentifier
        columnList=identifierList                                               #showColumnHistogramStats
    | SHOW COLUMN CACHED? STATS tableName=multipartIdentifier
        columnList=identifierList? partitionSpec?                               #showColumnStats
    | SHOW ANALYZE TASK STATUS jobId=INTEGER_VALUE                              #showAnalyzeTask
    | ANALYZE DATABASE name=multipartIdentifier
        (WITH analyzeProperties)* propertyClause?                               #analyzeDatabase
    | ANALYZE TABLE name=multipartIdentifier partitionSpec?
        columns=identifierList? (WITH analyzeProperties)* propertyClause?       #analyzeTable
    | ALTER TABLE name=multipartIdentifier SET STATS
        LEFT_PAREN propertyItemList RIGHT_PAREN partitionSpec?                  #alterTableStats
    | ALTER TABLE name=multipartIdentifier (INDEX indexName=identifier)?
        MODIFY COLUMN columnName=identifier
        SET STATS LEFT_PAREN propertyItemList RIGHT_PAREN partitionSpec?        #alterColumnStats
    | SHOW INDEX STATS tableName=multipartIdentifier indexId=identifier         #showIndexStats
    | DROP STATS tableName=multipartIdentifier
        columns=identifierList? partitionSpec?                                  #dropStats
    | DROP CACHED STATS tableName=multipartIdentifier                           #dropCachedStats
    | DROP EXPIRED STATS                                                        #dropExpiredStats
    | KILL ANALYZE jobId=INTEGER_VALUE                                          #killAnalyzeJob
    | DROP ANALYZE JOB INTEGER_VALUE                                            #dropAnalyzeJob
    | SHOW TABLE STATS tableName=multipartIdentifier
        partitionSpec? columnList=identifierList?                               #showTableStats
    | SHOW TABLE STATS tableId=INTEGER_VALUE                                    #showTableStats
    ;

analyzeProperties
    : SYNC
    | INCREMENTAL
    | FULL
    | SQL
    | HISTOGRAM
    | (SAMPLE ((ROWS rows=INTEGER_VALUE) | (PERCENT percent=INTEGER_VALUE)) )
    | (BUCKETS bucket=INTEGER_VALUE)
    | (PERIOD periodInSecond=INTEGER_VALUE)
    | (CRON crontabExpr=STRING_LITERAL)
    ;

workloadPolicyActions
    : workloadPolicyAction (COMMA workloadPolicyAction)*
    ;

workloadPolicyAction
    : SET_SESSION_VARIABLE STRING_LITERAL
    | identifier (STRING_LITERAL)?
    ;

workloadPolicyConditions
    : workloadPolicyCondition (COMMA workloadPolicyCondition)*
    ;

workloadPolicyCondition
    : metricName=identifier comparisonOperator (number | STRING_LITERAL)
    ;

storageBackend
    : (BROKER | S3 | HDFS | LOCAL) brokerName=identifier?
        ON LOCATION STRING_LITERAL properties=propertyClause?
    ;

passwordOption
    : (PASSWORD_HISTORY (historyDefault=DEFAULT | historyValue=INTEGER_VALUE))?
        (PASSWORD_EXPIRE (expireDefault=DEFAULT | expireNever=NEVER
            | INTERVAL expireValue=INTEGER_VALUE expireTimeUnit=(DAY | HOUR | SECOND)))?
        (PASSWORD_REUSE INTERVAL (reuseDefault=DEFAULT | reuseValue=INTEGER_VALUE DAY))?
        (FAILED_LOGIN_ATTEMPTS attemptsValue=INTEGER_VALUE)?
        (PASSWORD_LOCK_TIME (lockUnbounded=UNBOUNDED
            | lockValue=INTEGER_VALUE lockTimeUint=(DAY | HOUR | SECOND)))?
        (ACCOUNT_LOCK | ACCOUNT_UNLOCK)?
    ;

functionArguments
    : DOTDOTDOT
    | dataTypeList
    | dataTypeList COMMA DOTDOTDOT
    ;

dataTypeList
    : dataType (COMMA dataType)*
    ;

supportedSetStatement
    : SET (optionWithType | optionWithoutType)
        (COMMA (optionWithType | optionWithoutType))*                   #setOptions
    | SET identifier AS DEFAULT STORAGE VAULT                           #setDefaultStorageVault
    | SET PROPERTY (FOR user=identifierOrText)? propertyItemList        #setUserProperties
    | SET statementScope? TRANSACTION
        ( transactionAccessMode
        | isolationLevel
        | transactionAccessMode COMMA isolationLevel
        | isolationLevel COMMA transactionAccessMode)                   #setTransaction
    ;

optionWithType
    : statementScope identifier EQ (expression | DEFAULT)   #setVariableWithType
    ;

optionWithoutType
    : NAMES EQ expression                                               #setNames
    | (CHAR SET | CHARSET) (charsetName=identifierOrText | DEFAULT)     #setCharset
    | NAMES (charsetName=identifierOrText | DEFAULT)
        (COLLATE collateName=identifierOrText | DEFAULT)?               #setCollate
    | PASSWORD (FOR userIdentify)? EQ (pwd=STRING_LITERAL
        | (isPlain=PASSWORD LEFT_PAREN pwd=STRING_LITERAL RIGHT_PAREN)) #setPassword
    | LDAP_ADMIN_PASSWORD EQ (pwd=STRING_LITERAL
        | (PASSWORD LEFT_PAREN pwd=STRING_LITERAL RIGHT_PAREN))         #setLdapAdminPassword
    | variable                                                          #setVariableWithoutType
    ;

variable
    : (DOUBLEATSIGN (statementScope DOT)?)? identifier EQ (expression | DEFAULT) #setSystemVariable
    | ATSIGN identifier EQ expression #setUserVariable
    ;

transactionAccessMode
    : READ (ONLY | WRITE)
    ;

isolationLevel
    : ISOLATION LEVEL ((READ UNCOMMITTED) | (READ COMMITTED) | (REPEATABLE READ) | (SERIALIZABLE))
    ;

supportedUnsetStatement
    : UNSET statementScope? VARIABLE (ALL | identifier)
    | UNSET DEFAULT STORAGE VAULT
    ;

supportedUseStatement
     : SWITCH catalog=identifier                                                        #switchCatalog
     | USE (catalog=identifier DOT)? database=identifier                                #useDatabase
     | USE ((catalog=identifier DOT)? database=identifier)? ATSIGN cluster=identifier   #useCloudCluster
    ;

stageAndPattern
    : ATSIGN (stage=identifier | TILDE)
        (LEFT_PAREN pattern=STRING_LITERAL RIGHT_PAREN)?
    ;

supportedDescribeStatement
    : explainCommand FUNCTION tvfName=identifier LEFT_PAREN
        (properties=propertyItemList)? RIGHT_PAREN tableAlias   #describeTableValuedFunction
    | explainCommand multipartIdentifier ALL                    #describeTableAll
    | explainCommand multipartIdentifier specifiedPartition?    #describeTable
    | explainCommand DICTIONARY multipartIdentifier             #describeDictionary
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
        INTO TABLE targetTableName=identifier
        (partitionSpec)?
        (COLUMNS TERMINATED BY comma=STRING_LITERAL)?
        (LINES TERMINATED BY separator=STRING_LITERAL)?
        (FORMAT AS format=identifierOrText)?
        (COMPRESS_TYPE AS compressType=identifierOrText)?
        (columns=identifierList)?
        (columnsFromPath=colFromPath)?
        (columnMapping=colMappingList)?
        (preFilter=preFilterClause)?
        (where=whereClause)?
        (deleteOn=deleteOnClause)?
        (sequenceColumn=sequenceColClause)?
        (propertyClause)?
    | ((WITH)? mergeType)? DATA FROM TABLE sourceTableName=identifier
        INTO TABLE targetTableName=identifier
        (PARTITION partition=identifierList)?
        (columnMapping=colMappingList)?
        (where=whereClause)?
        (deleteOn=deleteOnClause)?
        (propertyClause)?
    ;

// -----------------Command accessories-----------------
statementScope
    : (GLOBAL | SESSION | LOCAL)
    ;
    
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

identifierOrText
    : identifier
    | STRING_LITERAL
    ;

identifierOrTextOrAsterisk
    : identifier
    | STRING_LITERAL
    | ASTERISK
    ;

multipartIdentifierOrAsterisk
    : parts+=identifierOrAsterisk (DOT parts+=identifierOrAsterisk)*
    ;

identifierOrAsterisk
    : identifierOrText
    | ASTERISK
    ;

userIdentify
    : user=identifierOrText (ATSIGN (host=identifierOrText
        | LEFT_PAREN host=identifierOrText RIGHT_PAREN))?
    ;

grantUserIdentify
    : userIdentify (IDENTIFIED BY PASSWORD? STRING_LITERAL)?
    ;

explain
    : explainCommand planType?
          level=(VERBOSE | TREE | GRAPH | PLAN | DUMP)?
          PROCESS?
    ;

explainCommand
    : EXPLAIN
    | DESC
    | DESCRIBE
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

replayCommand
    : PLAN REPLAYER replayType;

replayType
    : DUMP query
    | PLAY filePath=STRING_LITERAL;

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
    : DATA LOCAL?
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
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm     #setOperation
    | left=queryTerm operator=(UNION | EXCEPT | MINUS)
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
      qualifyClause?
      ({!ansiSQLSyntax}? queryOrganization | {ansiSQLSyntax}?)         #regularQuerySpecification
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
    : SELECT (DISTINCT|ALL)? selectColumnClause
    ;

selectColumnClause
    : namedExpressionSeq
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
    : LEFT_BRACKET identifier skewHint? RIGHT_BRACKET
    | HINT_START identifier skewHint? HINT_END
    ;

skewHint
    :  LEFT_BRACKET identifier LEFT_PAREN qualifiedName constantList RIGHT_PAREN RIGHT_BRACKET
    ;

constantList
    : LEFT_PAREN values+=constant (COMMA values+=constant)* RIGHT_PAREN
    ;

relationHint
    : LEFT_BRACKET identifier (COMMA identifier)* RIGHT_BRACKET       #bracketRelationHint
    | HINT_START identifier (COMMA identifier)* HINT_END              #commentRelationHint
    ;

expressionWithOrder
    :  expression ordering = (ASC | DESC)?
    ;

aggClause
    : GROUP BY groupingElement
    ;

groupingElement
    : ROLLUP LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | CUBE LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | GROUPING SETS LEFT_PAREN groupingSet (COMMA groupingSet)* RIGHT_PAREN
    | expressionWithOrder (COMMA expressionWithOrder)* (WITH ROLLUP)?
    ;

groupingSet
    : LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    ;

havingClause
    : HAVING booleanExpression
    ;

qualifyClause
    : QUALIFY booleanExpression
    ;

selectHint: hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HINT_END;

hintStatement
    : hintName (LEFT_PAREN parameters+=hintAssignment (COMMA? parameters+=hintAssignment)* RIGHT_PAREN)?
    | (USE_MV | NO_USE_MV) (LEFT_PAREN tableList+=multipartIdentifier (COMMA tableList+=multipartIdentifier)* RIGHT_PAREN)?
    ;

hintName
    : identifier
    | LEADING
    ;

hintAssignment
    : key=identifierOrText skew=skewHint? (EQ (constantValue=constant | identifierValue=identifier))?
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
    : PARTITION BY (LEFT_BRACKET identifier RIGHT_BRACKET)? expression (COMMA expression)*
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
    : ATSIGN funcName=identifier LEFT_PAREN (mapParams=propertyItemList | listParams=identifierSeq)? RIGHT_PAREN
    ;

relationPrimary
    : multipartIdentifier optScanParams? materializedViewName? tableSnapshot? specifiedPartition?
       tabletList? tableAlias sample? relationHint? lateralView*                           #tableName
    | LEFT_PAREN query RIGHT_PAREN tableAlias lateralView*                                 #aliasedQuery
    | tvfName=identifier LEFT_PAREN
      (properties=propertyItemList)?
      RIGHT_PAREN tableAlias                                                               #tableValuedFunction
    | LEFT_PAREN relations RIGHT_PAREN                                                     #relationList
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
        (DEFAULT (nullValue=NULL | SUBTRACT? INTEGER_VALUE | SUBTRACT? DECIMAL_VALUE | PI | E | BITMAP_EMPTY | stringValue=STRING_LITERAL
           | CURRENT_DATE | defaultTimestamp=CURRENT_TIMESTAMP (LEFT_PAREN defaultValuePrecision=number RIGHT_PAREN)?))?
        (ON UPDATE CURRENT_TIMESTAMP (LEFT_PAREN onUpdateValuePrecision=number RIGHT_PAREN)?)?
        (COMMENT comment=STRING_LITERAL)?
    ;

indexDefs
    : indexes+=indexDef (COMMA indexes+=indexDef)*
    ;

indexDef
    : INDEX (ifNotExists=IF NOT EXISTS)? indexName=identifier cols=identifierList (USING indexType=(BITMAP | INVERTED | NGRAM_BF))? (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)? (COMMENT comment=STRING_LITERAL)?
    ;

partitionsDef
    : partitions+=partitionDef (COMMA partitions+=partitionDef)*
    ;

partitionDef
    : (lessThanPartitionDef | fixedPartitionDef | stepPartitionDef | inPartitionDef) (LEFT_PAREN partitionProperties=propertyItemList RIGHT_PAREN)?
    ;

lessThanPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier VALUES LESS THAN (MAXVALUE | partitionValueList)
    ;

fixedPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier VALUES LEFT_BRACKET lower=partitionValueList COMMA upper=partitionValueList RIGHT_PAREN
    ;

stepPartitionDef
    : FROM from=partitionValueList TO to=partitionValueList INTERVAL unitsAmount=INTEGER_VALUE unit=unitIdentifier?
    ;

inPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier ((VALUES IN)? ((LEFT_PAREN partitionValueLists+=partitionValueList
        (COMMA partitionValueLists+=partitionValueList)* RIGHT_PAREN) | constants=partitionValueList))?
    ;

partitionValueList
    : LEFT_PAREN values+=partitionValueDef (COMMA values+=partitionValueDef)* RIGHT_PAREN
    ;

partitionValueDef
    : SUBTRACT? INTEGER_VALUE | STRING_LITERAL | MAXVALUE | NULL
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
    ;

funcExpression
    : expression
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
    : LOGICALNOT booleanExpression                                                  #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                                           #exist
    | (ISNULL | IS_NULL_PRED) LEFT_PAREN valueExpression RIGHT_PAREN                #isnull
    | IS_NOT_NULL_PRED LEFT_PAREN valueExpression RIGHT_PAREN                       #is_not_null_pred
    | valueExpression predicate?                                                    #predicated
    | NOT booleanExpression                                                         #logicalNot
    | left=booleanExpression operator=(AND | LOGICALAND) right=booleanExpression    #logicalBinary
    | left=booleanExpression operator=XOR right=booleanExpression                   #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=DOUBLEPIPES right=booleanExpression           #doublePipes
    ;

rowConstructor
    : LEFT_PAREN (rowConstructorItem (COMMA rowConstructorItem)*)? RIGHT_PAREN
    ;

rowConstructorItem
    : constant // duplicate constant rule for improve the parse of `insert into tbl values`
    | DEFAULT
    | namedExpression
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=(REGEXP | RLIKE) pattern=valueExpression
    | NOT? kind=LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?
    | NOT? kind=(MATCH | MATCH_ANY | MATCH_ALL | MATCH_PHRASE | MATCH_PHRASE_PREFIX | MATCH_REGEXP | MATCH_PHRASE_EDGE) pattern=valueExpression
    | NOT? kind=IN LEFT_PAREN query RIGHT_PAREN
    | NOT? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | IS NOT? kind=NULL
    | IS NOT? kind=(TRUE | FALSE)
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(SUBTRACT | PLUS | TILDE) valueExpression                                     #arithmeticUnary
    // split arithmeticBinary from 1 to 5 due to they have different operator precedence
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=(ASTERISK | SLASH | MOD | DIV) right=valueExpression     #arithmeticBinary
    | left=valueExpression operator=(PLUS | SUBTRACT) right=valueExpression                  #arithmeticBinary
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    ;

primaryExpression
    : name=CURRENT_DATE                                                                        #currentDate
    | name=CURRENT_TIME                                                                        #currentTime
    | name=CURRENT_TIMESTAMP                                                                   #currentTimestamp
    | name=LOCALTIME                                                                           #localTime
    | name=LOCALTIMESTAMP                                                                      #localTimestamp
    | name=CURRENT_USER                                                                        #currentUser
    | name=SESSION_USER                                                                        #sessionUser
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | name=CAST LEFT_PAREN expression AS castDataType RIGHT_PAREN                              #cast
    | constant                                                                                 #constantDefault
    | interval                                                                                 #intervalLiteral
    | ASTERISK (exceptOrReplace)*                                                              #star
    | qualifiedName DOT ASTERISK (exceptOrReplace)*                                            #star
    | CHAR LEFT_PAREN
                arguments+=expression (COMMA arguments+=expression)*
                (USING charSet=identifierOrText)?
          RIGHT_PAREN                                                                          #charFunction
    | CONVERT LEFT_PAREN argument=expression USING charSet=identifierOrText RIGHT_PAREN        #convertCharSet
    | CONVERT LEFT_PAREN argument=expression COMMA castDataType RIGHT_PAREN                    #convertType
    | GROUP_CONCAT LEFT_PAREN (DISTINCT|ALL)?
        (LEFT_BRACKET identifier RIGHT_BRACKET)?
        argument=expression
        (ORDER BY sortItem (COMMA sortItem)*)?
        (SEPARATOR sep=expression)? RIGHT_PAREN
        (OVER windowSpec)?                                                                     #groupConcat
    | TRIM LEFT_PAREN
        ((BOTH | LEADING | TRAILING) expression? | expression) FROM expression RIGHT_PAREN     #trim
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

exceptOrReplace
    : EXCEPT  LEFT_PAREN namedExpressionSeq RIGHT_PAREN                                  #except
    | REPLACE LEFT_PAREN namedExpressionSeq RIGHT_PAREN                                  #replace
    ;

castDataType
    : dataType
    |(SIGNED|UNSIGNED) (INT|INTEGER)?
    ;

functionCallExpression
    : functionIdentifier
              LEFT_PAREN (
                  (DISTINCT|ALL)?
                  (LEFT_BRACKET identifier RIGHT_BRACKET)?
                  arguments+=funcExpression (COMMA arguments+=funcExpression)*
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
    | SESSION_USER
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
    | PLACEHOLDER                                                                              #placeholder
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
	: YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    ;

dataTypeWithNullable
    : dataType ((NOT)? NULL)?
    ;

dataType
    : complex=ARRAY LT dataType GT                                  #complexDataType
    | complex=MAP LT dataType COMMA dataType GT                     #complexDataType
    | complex=STRUCT LT complexColTypeList GT                       #complexDataType
    | complex=variantTypeDefinitions                                #variantPredefinedFields
    | AGG_STATE LT functionNameIdentifier
        LEFT_PAREN dataTypes+=dataTypeWithNullable
        (COMMA dataTypes+=dataTypeWithNullable)* RIGHT_PAREN GT     #aggStateDataType
    | primitiveColType (LEFT_PAREN (INTEGER_VALUE | ASTERISK)
      (COMMA INTEGER_VALUE)* RIGHT_PAREN)?                          #primitiveDataType
    ;

primitiveColType
    : type=TINYINT
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

variantTypeDefinitions
    : VARIANT LT variantSubColTypeList COMMA properties=propertyClause GT  #variant
    | VARIANT LT variantSubColTypeList GT                                  #variant
    | VARIANT LT properties=propertyClause GT                              #variant
    | VARIANT                                                              #variant
    ;

variantSubColTypeList
    : variantSubColType (COMMA variantSubColType)*
    ;
variantSubColType
    : variantSubColMatchType? STRING_LITERAL COLON dataType commentSpec?
    ;
variantSubColMatchType
    : (MATCH_NAME | MATCH_NAME_GLOB)
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
    : FOR VERSION AS OF version=(INTEGER_VALUE | STRING_LITERAL)
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
    : ACTIONS
    | AFTER
    | AGG_STATE
    | AGGREGATE
    | ALIAS
    | ALWAYS
    | ANALYZED
    | ARRAY
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
    | BITMAP_EMPTY
    | BITMAP_UNION
    | BITOR
    | BITXOR
    | BLOB
    | BOOLEAN
    | BRANCH
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
    | COMMENT_START
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPLETE
    | COMPRESS_TYPE
    | COMPUTE
    | CONDITIONS
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
    | DATETIME
    | DATETIMEV1
    | DATETIMEV2
    | DATEV1
    | DATEV2
    | DAY
    | DAYS
    | DECIMAL
    | DECIMALV2
    | DECIMALV3
    | DEFERRED
    | DEMAND
    | DIAGNOSE
    | DIAGNOSIS
    | DICTIONARIES
    | DICTIONARY
    | DISTINCTPC
    | DISTINCTPCSA
    | DO
    | DORIS_INTERNAL_TABLE_ID
    | DUAL
    | DYNAMIC
    | E
    | ENABLE
    | ENCRYPTION
    | ENCRYPTKEY
    | ENCRYPTKEYS
    | END
    | ENDS
    | ENGINE
    | ENGINES
    | ERRORS
    | ESCAPE
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
    | GROUP_CONCAT
    | HASH
    | HASH_MAP
    | HDFS
    | HELP
    | HINT_END
    | HINT_START
    | HISTOGRAM
    | HLL_UNION
    | HOSTNAME
    | HOTSPOT
    | HOUR
    | HOURS
    | HUB
    | IDENTIFIED
    | IGNORE
    | IMMEDIATE
    | INCREMENTAL
    | INDEXES
    | INVERTED
    | IP_TRIE
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
    | MATCH_NAME
    | MATCH_NAME_GLOB
    | MATERIALIZED
    | MAX
    | MEMO
    | MERGE
    | MIGRATE
    | MIGRATIONS
    | MIN
    | MINUTE
    | MINUTES
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
	| QUARTER
    | QUERY
    | QUOTA
    | QUALIFY
    | QUEUED
    | RANDOM
    | RECENT
    | RECOVER
    | RECYCLE
    | REFRESH
    | REPEATABLE
    | REPLACE
    | REPLACE_IF_NOT_NULL
    | REPLAYER
    | REPOSITORIES
    | REPOSITORY
    | RESOURCE
    | RESOURCES
    | RESTORE
    | RESTRICTIVE
    | RESUME
    | RETAIN
    | RETENTION
    | RETURNS
    | REWRITTEN
    | RIGHT_BRACE
    | RLIKE
    | ROLLBACK
    | ROLLUP
    | ROOT
    | ROUTINE
    | S3
    | SAMPLE
    | SCHEDULE
    | SCHEDULER
    | SCHEMA
    | SECOND
    | SEPARATOR
    | SERIALIZABLE
    | SET_SESSION_VARIABLE
    | SESSION
    | SESSION_USER
    | SHAPE
    | SKEW
    | SNAPSHOT
    | SNAPSHOTS
    | SONAME
    | SPLIT
    | SQL
    | STAGE
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
    | SUM
    | TABLES
    | TAG
    | TASK
    | TASKS
    | TEMPORARY
    | TEXT
    | THAN
    | TIME
    | TIMESTAMP
    | TRANSACTION
    | TREE
    | TRIGGERS
    | TRUNCATE
    | TYPE
    | TYPES
    | UNCOMMITTED
    | UNLOCK
    | UNSET
    | UP
    | USER
    | VALUE
    | VARCHAR
    | VARIABLE
    | VARIABLES
    | VARIANT
    | VAULT
    | VAULTS
    | VERBOSE
    | VERSION
    | VIEW
    | VIEWS
    | WARM
    | WARNINGS
    | WEEK
    | WORK
    | YEAR
//--DEFAULT-NON-RESERVED-END
    ;
