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
    : SEMICOLON* statement? (SEMICOLON+ statement)* SEMICOLON* EOF
    ;

singleStatement
    : SEMICOLON* statement? SEMICOLON* EOF
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
    | constraintStatement               #constraintStatementAlias
    | supportedDropStatement            #supportedDropStatementAlias
    | unsupportedStatement              #unsupported
    ;

unsupportedStatement
    : unsupportedSetStatement
    | unsupoortedUnsetStatement
    | unsupportedUseStatement
    | unsupportedDmlStatement
    | unsupportedKillStatement
    | unsupportedDescribeStatement
    | unsupportedCreateStatement
    | unsupportedDropStatement
    | unsupportedStatsStatement
    | unsupportedAlterStatement
    | unsupportedGrantRevokeStatement
    | unsupportedAdminStatement
    | unsupportedTransactionStatement
    | unsupportedRecoverStatement
    | unsupportedCancelStatement
    | unsupportedJobStatement
    | unsupportedCleanStatement
    | unsupportedRefreshStatement
    | unsupportedLoadStatement
    | unsupportedShowStatement
    | unsupportedOtherStatement
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
        (ON tableName=multipartIdentifier)?                                                     #dropMTMV
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
    | ALTER STORAGE VAULT name=multipartIdentifier properties=propertyClause   #alterStorageVault
    ;

supportedDropStatement
    : DROP CATALOG RECYCLE BIN WHERE idType=STRING_LITERAL EQ id=INTEGER_VALUE #dropCatalogRecycleBin
    ;

unsupportedOtherStatement
    : HELP mark=identifierOrText                                                    #help
    | INSTALL PLUGIN FROM source=identifierOrText properties=propertyClause?        #installPlugin
    | UNINSTALL PLUGIN name=identifierOrText                                        #uninstallPlugin
    | LOCK TABLES (lockTable (COMMA lockTable)*)?                                   #lockTables
    | UNLOCK TABLES                                                                 #unlockTables
    | WARM UP CLUSTER destination=identifier WITH
        (CLUSTER source=identifier | (warmUpItem (COMMA warmUpItem)*)) FORCE?       #warmUpCluster
    | BACKUP SNAPSHOT label=multipartIdentifier TO repo=identifier
        ((ON | EXCLUDE) LEFT_PAREN baseTableRef (COMMA baseTableRef)* RIGHT_PAREN)?
        properties=propertyClause?                                                  #backup
    | RESTORE SNAPSHOT label=multipartIdentifier FROM repo=identifier
        ((ON | EXCLUDE) LEFT_PAREN baseTableRef (COMMA baseTableRef)* RIGHT_PAREN)?
        properties=propertyClause?                                                  #restore
    | START TRANSACTION (WITH CONSISTENT SNAPSHOT)?                                 #unsupportedStartTransaction
    ;

warmUpItem
    : TABLE tableName=multipartIdentifier (PARTITION partitionName=identifier)?
    ;

lockTable
    : name=multipartIdentifier (AS alias=identifierOrText)?
        (READ (LOCAL)? | (LOW_PRIORITY)? WRITE)
    ;

unsupportedShowStatement
    : SHOW SQL_BLOCK_RULE (FOR ruleName=identifier)?                                #showSqlBlockRule
    | SHOW ROW POLICY (FOR (userIdentify | (ROLE role=identifier)))?                #showRowPolicy
    | SHOW STORAGE POLICY (USING (FOR policy=identifierOrText)?)?                   #showStoragePolicy
    | SHOW STAGES                                                                   #showStages
    | SHOW STORAGE VAULT                                                            #showStorageVault
    | SHOW CREATE REPOSITORY FOR identifier                                         #showCreateRepository
    | SHOW WHITELIST                                                                #showWhitelist
    | SHOW (GLOBAL | SESSION | LOCAL)? VARIABLES wildWhere?                         #showVariables
    | SHOW OPEN TABLES ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showOpenTables
    | SHOW TABLE STATUS ((FROM | IN) database=multipartIdentifier)? wildWhere?      #showTableStatus
    | SHOW FULL? TABLES ((FROM | IN) database=multipartIdentifier)? wildWhere?      #showTables
    | SHOW FULL? VIEWS ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showViews
    | SHOW TABLE tableId=INTEGER_VALUE                                              #showTableId
    | SHOW FULL? PROCESSLIST                                                        #showProcessList
    | SHOW (GLOBAL | SESSION | LOCAL)? STATUS wildWhere?                            #showStatus
    | SHOW FULL? TRIGGERS ((FROM | IN) database=multipartIdentifier)? wildWhere?    #showTriggers
    | SHOW EVENTS ((FROM | IN) database=multipartIdentifier)? wildWhere?            #showEvents
    | SHOW PLUGINS                                                                  #showPlugins
    | SHOW STORAGE? ENGINES                                                         #showStorageEngines
    | SHOW AUTHORS                                                                  #showAuthors
    | SHOW BRIEF? CREATE TABLE name=multipartIdentifier                             #showCreateTable
    | SHOW CREATE VIEW name=multipartIdentifier                                     #showCreateView
    | SHOW CREATE MATERIALIZED VIEW name=multipartIdentifier                        #showMaterializedView
    | SHOW CREATE (DATABASE | SCHEMA) name=multipartIdentifier                      #showCreateDatabase
    | SHOW CREATE CATALOG name=identifier                                           #showCreateCatalog
    | SHOW CREATE (GLOBAL | SESSION | LOCAL)? FUNCTION functionIdentifier
        LEFT_PAREN functionArguments? RIGHT_PAREN
        ((FROM | IN) database=multipartIdentifier)?                                 #showCreateFunction
    | SHOW (DATABASES | SCHEMAS) (FROM catalog=identifier)? wildWhere?              #showDatabases
    | SHOW DATABASE databaseId=INTEGER_VALUE                                        #showDatabaseId
    | SHOW DATA TYPES                                                               #showDataTypes
    | SHOW CATALOGS wildWhere?                                                      #showCatalogs
    | SHOW CATALOG name=identifier                                                  #showCatalog
    | SHOW DYNAMIC PARTITION TABLES ((FROM | IN) database=multipartIdentifier)?     #showDynamicPartition
    | SHOW FULL? (COLUMNS | FIELDS) (FROM | IN) tableName=multipartIdentifier
        ((FROM | IN) database=multipartIdentifier)? wildWhere?                      #showColumns
    | SHOW COLLATION wildWhere?                                                     #showCollation
    | SHOW ((CHAR SET) | CHARSET) wildWhere?                                        #showCharset
    | SHOW PROC path=STRING_LITERAL                                                 #showProc
    | SHOW COUNT LEFT_PAREN ASTERISK RIGHT_PAREN (WARNINGS | ERRORS)                #showWaringErrorCount
    | SHOW (WARNINGS | ERRORS) limitClause?                                         #showWaringErrors
    | SHOW LOAD WARNINGS ((((FROM | IN) database=multipartIdentifier)?
        wildWhere? limitClause?) | (ON url=STRING_LITERAL))                         #showLoadWarings
    | SHOW STREAM? LOAD ((FROM | IN) database=multipartIdentifier)? wildWhere?
        sortClause? limitClause?                                                    #showLoad
    | SHOW EXPORT ((FROM | IN) database=multipartIdentifier)? wildWhere?
        sortClause? limitClause?                                                    #showExport
    | SHOW DELETE ((FROM | IN) database=multipartIdentifier)?                       #showDelete
    | SHOW ALTER TABLE (ROLLUP | (MATERIALIZED VIEW) | COLUMN)
        ((FROM | IN) database=multipartIdentifier)? wildWhere?
        sortClause? limitClause?                                                    #showAlterTable
    | SHOW DATA SKEW FROM baseTableRef                                              #showDataSkew
    | SHOW DATA (FROM tableName=multipartIdentifier)? sortClause? propertyClause?   #showData
    | SHOW TEMPORARY? PARTITIONS FROM tableName=multipartIdentifier
        wildWhere? sortClause? limitClause?                                         #showPartitions
    | SHOW PARTITION partitionId=INTEGER_VALUE                                      #showPartitionId
    | SHOW TABLET tabletId=INTEGER_VALUE                                            #showTabletId
    | SHOW TABLETS BELONG
        tabletIds+=INTEGER_VALUE (COMMA tabletIds+=INTEGER_VALUE)*                  #showTabletBelong
    | SHOW TABLETS FROM tableName=multipartIdentifier partitionSpec?
        wildWhere? sortClause? limitClause?                                         #showTabletsFromTable
    | SHOW PROPERTY (FOR user=identifierOrText)? wildWhere?                         #showUserProperties
    | SHOW ALL PROPERTIES wildWhere?                                                #showAllProperties
    | SHOW BACKUP ((FROM | IN) database=multipartIdentifier)? wildWhere?            #showBackup
    | SHOW BRIEF? RESTORE ((FROM | IN) database=multipartIdentifier)? wildWhere?    #showRestore
    | SHOW BROKER                                                                   #showBroker
    | SHOW RESOURCES wildWhere? sortClause? limitClause?                            #showResources
    | SHOW WORKLOAD GROUPS wildWhere?                                               #showWorkloadGroups
    | SHOW BACKENDS                                                                 #showBackends
    | SHOW TRASH (ON backend=STRING_LITERAL)?                                       #showTrash
    | SHOW FRONTENDS name=identifier?                                               #showFrontends
    | SHOW REPOSITORIES                                                             #showRepositories
    | SHOW SNAPSHOT ON repo=identifier wildWhere?                                   #showSnapshot
    | SHOW ALL? GRANTS                                                              #showGrants
    | SHOW GRANTS FOR userIdentify                                                  #showGrantsForUser
    | SHOW ROLES                                                                    #showRoles
    | SHOW PRIVILEGES                                                               #showPrivileges
    | SHOW FULL? BUILTIN? FUNCTIONS
        ((FROM | IN) database=multipartIdentifier)? wildWhere?                      #showFunctions
    | SHOW GLOBAL FULL? FUNCTIONS wildWhere?                                        #showGlobalFunctions
    | SHOW TYPECAST ((FROM | IN) database=multipartIdentifier)?                     #showTypeCast
    | SHOW FILE ((FROM | IN) database=multipartIdentifier)?                         #showSmallFiles
    | SHOW (KEY | KEYS | INDEX | INDEXES)
        (FROM |IN) tableName=multipartIdentifier
        ((FROM | IN) database=multipartIdentifier)?                                 #showIndex
    | SHOW VIEW
        (FROM |IN) tableName=multipartIdentifier
        ((FROM | IN) database=multipartIdentifier)?                                 #showView
    | SHOW TRANSACTION ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showTransaction
    | SHOW QUERY PROFILE queryIdPath=STRING_LITERAL                                 #showQueryProfile
    | SHOW LOAD PROFILE loadIdPath=STRING_LITERAL                                   #showLoadProfile
    | SHOW CACHE HOTSPOT tablePath=STRING_LITERAL                                   #showCacheHotSpot
    | SHOW ENCRYPTKEYS ((FROM | IN) database=multipartIdentifier)? wildWhere?       #showEncryptKeys
    | SHOW SYNC JOB ((FROM | IN) database=multipartIdentifier)?                     #showSyncJob
    | SHOW TABLE CREATION ((FROM | IN) database=multipartIdentifier)? wildWhere?    #showTableCreation
    | SHOW LAST INSERT                                                              #showLastInsert
    | SHOW CREATE MATERIALIZED VIEW mvName=identifier
        ON tableName=multipartIdentifier                                            #showCreateMaterializedView
    | SHOW CATALOG RECYCLE BIN wildWhere?                                           #showCatalogRecycleBin
    | SHOW QUERY STATS ((FOR database=identifier)
            | (FROM tableName=multipartIdentifier (ALL VERBOSE?)?))?                #showQueryStats
    | SHOW BUILD INDEX ((FROM | IN) database=multipartIdentifier)?
        wildWhere? sortClause? limitClause?                                         #showBuildIndex
    | SHOW CLUSTERS                                                                 #showClusters
    | SHOW CONVERT_LSC ((FROM | IN) database=multipartIdentifier)?                  #showConvertLsc
    | SHOW REPLICA STATUS FROM baseTableRef wildWhere?                              #showReplicaStatus
    | SHOW REPLICA DISTRIBUTION FROM baseTableRef                                   #showREplicaDistribution
    | SHOW TABLET STORAGE FORMAT VERBOSE?                                           #showTabletStorageFormat
    | SHOW TABLET DIAGNOSIS tabletId=INTEGER_VALUE                                  #showDiagnoseTablet
    | SHOW COPY ((FROM | IN) database=multipartIdentifier)?
        whereClause? sortClause? limitClause?                                       #showCopy
    | SHOW WARM UP JOB wildWhere?                                                   #showWarmUpJob
    ;

unsupportedLoadStatement
    : LOAD mysqlDataDesc
        (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)?
        (commentSpec)?                                                              #mysqlLoad
    | CREATE SYNC label=multipartIdentifier
          LEFT_PAREN channelDescriptions RIGHT_PAREN
          FROM BINLOG LEFT_PAREN propertyItemList RIGHT_PAREN
          properties=propertyClause?                                                #createDataSyncJob
    | STOP SYNC JOB name=multipartIdentifier                                        #stopDataSyncJob
    | RESUME SYNC JOB name=multipartIdentifier                                      #resumeDataSyncJob
    | PAUSE SYNC JOB name=multipartIdentifier                                       #pauseDataSyncJob
    | CREATE ROUTINE LOAD label=multipartIdentifier (ON table=identifier)?
        (WITH (APPEND | DELETE | MERGE))?
        (loadProperty (COMMA loadProperty)*)? propertyClause? FROM type=identifier
        LEFT_PAREN customProperties=propertyItemList RIGHT_PAREN
        commentSpec?                                                                #createRoutineLoadJob
    | PAUSE ROUTINE LOAD FOR label=multipartIdentifier                              #pauseRoutineLoad
    | PAUSE ALL ROUTINE LOAD                                                        #pauseAllRoutineLoad
    | RESUME ROUTINE LOAD FOR label=multipartIdentifier                             #resumeRoutineLoad
    | RESUME ALL ROUTINE LOAD                                                       #resumeAllRoutineLoad
    | STOP ROUTINE LOAD FOR label=multipartIdentifier                               #stopRoutineLoad
    | SHOW ALL? ROUTINE LOAD ((FOR label=multipartIdentifier) | wildWhere?)         #showRoutineLoad
    | SHOW ROUTINE LOAD TASK ((FROM | IN) database=identifier)? wildWhere?          #showRoutineLoadTask
    | SHOW ALL? CREATE ROUTINE LOAD FOR label=multipartIdentifier                   #showCreateRoutineLoad
    | SHOW CREATE LOAD FOR label=multipartIdentifier                                #showCreateLoad
    | SYNC                                                                          #sync
    | importSequenceStatement                                                       #importSequenceStatementAlias
    | importPrecedingFilterStatement                                                #importPrecedingFilterStatementAlias
    | importWhereStatement                                                          #importWhereStatementAlias
    | importDeleteOnStatement                                                       #importDeleteOnStatementAlias
    | importColumnsStatement                                                        #importColumnsStatementAlias
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

channelDescriptions
    : channelDescription (COMMA channelDescription)*
    ;

channelDescription
    : FROM source=multipartIdentifier INTO destination=multipartIdentifier
        partitionSpec? columnList=identifierList?
    ;

unsupportedRefreshStatement
    : REFRESH TABLE name=multipartIdentifier                                        #refreshTable
    | REFRESH DATABASE name=multipartIdentifier propertyClause?                     #refreshDatabase
    | REFRESH CATALOG name=identifier propertyClause?                               #refreshCatalog
    | REFRESH LDAP (ALL | (FOR user=identifierOrText))                              #refreshLdap
    ;

unsupportedCleanStatement
    : CLEAN LABEL label=identifier? (FROM | IN) database=identifier                 #cleanLabel
    | CLEAN ALL PROFILE                                                             #cleanAllProfile
    | CLEAN QUERY STATS ((FOR database=identifier)
        | ((FROM | IN) table=multipartIdentifier))                                  #cleanQueryStats
    | CLEAN ALL QUERY STATS                                                         #cleanAllQueryStats
    ;

unsupportedJobStatement
    : CREATE JOB label=multipartIdentifier ON SCHEDULE
        (
            (EVERY timeInterval=INTEGER_VALUE timeUnit=identifier
            (STARTS (startTime=STRING_LITERAL | CURRENT_TIMESTAMP))?
            (ENDS endsTime=STRING_LITERAL)?)
            |
            (AT (atTime=STRING_LITERAL | CURRENT_TIMESTAMP)))
        commentSpec?
        DO statement                                                                #createJob
    | PAUSE JOB wildWhere?                                                          #pauseJob
    | DROP JOB (IF EXISTS)? wildWhere?                                              #dropJob
    | RESUME JOB wildWhere?                                                         #resumeJob
    | CANCEL TASK wildWhere?                                                        #cancelJobTask
    ;

unsupportedCancelStatement
    : CANCEL LOAD ((FROM | IN) database=identifier)? wildWhere?                     #cancelLoad
    | CANCEL EXPORT ((FROM | IN) database=identifier)? wildWhere?                   #cancelExport
    | CANCEL ALTER TABLE (ROLLUP | (MATERIALIZED VIEW) | COLUMN)
        FROM tableName=multipartIdentifier (LEFT_PAREN jobIds+=INTEGER_VALUE
            (COMMA jobIds+=INTEGER_VALUE)* RIGHT_PAREN)?                            #cancelAlterTable
    | CANCEL BUILD INDEX ON tableName=multipartIdentifier
        (LEFT_PAREN jobIds+=INTEGER_VALUE
            (COMMA jobIds+=INTEGER_VALUE)* RIGHT_PAREN)?                            #cancelBuildIndex
    | CANCEL DECOMMISSION BACKEND hostPorts+=STRING_LITERAL
        (COMMA hostPorts+=STRING_LITERAL)*                                          #cancelDecommisionBackend
    | CANCEL BACKUP ((FROM | IN) database=identifier)?                              #cancelBackup
    | CANCEL RESTORE ((FROM | IN) database=identifier)?                             #cancelRestore
    | CANCEL WARM UP JOB wildWhere?                                                 #cancelWarmUp
    ;

unsupportedRecoverStatement
    : RECOVER DATABASE name=identifier id=INTEGER_VALUE? (AS alias=identifier)?     #recoverDatabase
    | RECOVER TABLE name=multipartIdentifier
        id=INTEGER_VALUE? (AS alias=identifier)?                                    #recoverTable
    | RECOVER PARTITION name=identifier id=INTEGER_VALUE? (AS alias=identifier)?
        FROM tableName=multipartIdentifier                                          #recoverPartition
    ;

unsupportedAdminStatement
    : ADMIN SHOW REPLICA STATUS FROM baseTableRef wildWhere?                        #adminShowReplicaStatus
    | ADMIN SHOW REPLICA DISTRIBUTION FROM baseTableRef                             #adminShowReplicaDistribution
    | ADMIN SET REPLICA STATUS PROPERTIES LEFT_PAREN propertyItemList RIGHT_PAREN   #adminSetReplicaStatus
    | ADMIN SET REPLICA VERSION PROPERTIES LEFT_PAREN propertyItemList RIGHT_PAREN  #adminSetReplicaVersion
    | ADMIN REPAIR TABLE baseTableRef                                               #adminRepairTable
    | ADMIN CANCEL REPAIR TABLE baseTableRef                                        #adminCancelRepairTable
    | ADMIN COMPACT TABLE baseTableRef wildWhere?                                   #adminCompactTable
    | ADMIN SET (FRONTEND | (ALL FRONTENDS)) CONFIG
        (LEFT_PAREN propertyItemList RIGHT_PAREN)? ALL?                             #adminSetFrontendConfig
    | ADMIN CHECK tabletList properties=propertyClause?                             #adminCheckTablets
    | ADMIN REBALANCE DISK (ON LEFT_PAREN backends+=STRING_LITERAL
        (COMMA backends+=STRING_LITERAL) RIGHT_PAREN)?                              #adminRebalanceDisk
    | ADMIN CANCEL REBALANCE DISK (ON LEFT_PAREN backends+=STRING_LITERAL
        (COMMA backends+=STRING_LITERAL) RIGHT_PAREN)?                              #adminCancelRebalanceDisk
    | ADMIN CLEAN TRASH (ON LEFT_PAREN backends+=STRING_LITERAL
        (COMMA backends+=STRING_LITERAL) RIGHT_PAREN)?                              #adminCleanTrash
    | ADMIN SET TABLE name=multipartIdentifier
        PARTITION VERSION properties=propertyClause?                                #adminSetPartitionVersion
    | ADMIN DIAGNOSE TABLET tabletId=INTEGER_VALUE                                  #adminDiagnoseTablet
    | ADMIN SHOW TABLET STORAGE FORMAT VERBOSE?                                     #adminShowTabletStorageFormat
    | ADMIN COPY TABLET tabletId=INTEGER_VALUE properties=propertyClause?           #adminCopyTablet
    | ADMIN SET TABLE name=multipartIdentifier STATUS properties=propertyClause?    #adminSetTableStatus
    ;

baseTableRef
    : multipartIdentifier optScanParams? tableSnapshot? specifiedPartition?
        tabletList? tableAlias sample? relationHint?
    ;

wildWhere
    : LIKE STRING_LITERAL
    | WHERE expression
    ;

unsupportedTransactionStatement
    : BEGIN (WITH LABEL identifier?)?                                               #transactionBegin
    | COMMIT WORK? (AND NO? CHAIN)? (NO? RELEASE)?                                  #transcationCommit
    | ROLLBACK WORK? (AND NO? CHAIN)? (NO? RELEASE)?                                #transactionRollback
    ;

unsupportedGrantRevokeStatement
    : GRANT privilegeList ON multipartIdentifierOrAsterisk
        TO (userIdentify | ROLE STRING_LITERAL)                                     #grantTablePrivilege
    | GRANT privilegeList ON
        (RESOURCE | CLUSTER | STAGE | STORAGE VAULT | WORKLOAD GROUP)
        identifierOrTextOrAsterisk TO (userIdentify | ROLE STRING_LITERAL)          #grantResourcePrivilege
    | GRANT roles+=STRING_LITERAL (COMMA roles+=STRING_LITERAL)* TO userIdentify    #grantRole
    | REVOKE privilegeList ON multipartIdentifierOrAsterisk
        FROM (userIdentify | ROLE STRING_LITERAL)                                   #grantTablePrivilege
    | REVOKE privilegeList ON
        (RESOURCE | CLUSTER | STAGE | STORAGE VAULT | WORKLOAD GROUP)
        identifierOrTextOrAsterisk FROM (userIdentify | ROLE STRING_LITERAL)        #grantResourcePrivilege
    | REVOKE roles+=STRING_LITERAL (COMMA roles+=STRING_LITERAL)* FROM userIdentify #grantRole
    ;

privilege
    : name=identifier columns=identifierList?
    | ALL
    ;

privilegeList
    : privilege (COMMA privilege)*
    ;

unsupportedAlterStatement
    : ALTER TABLE tableName=multipartIdentifier
        alterTableClause (COMMA alterTableClause)*                                  #alterTable
    | ALTER TABLE tableName=multipartIdentifier ADD ROLLUP
        addRollupClause (COMMA addRollupClause)*                                    #alterTableAddRollup
    | ALTER TABLE tableName=multipartIdentifier DROP ROLLUP
        dropRollupClause (COMMA dropRollupClause)*                                  #alterTableDropRollup
    | ALTER SYSTEM alterSystemClause                                                #alterSystem
    | ALTER DATABASE name=identifier SET (DATA |REPLICA | TRANSACTION)
        QUOTA INTEGER_VALUE identifier?                                             #alterDatabaseSetQuota
    | ALTER DATABASE name=identifier RENAME newName=identifier                      #alterDatabaseRename
    | ALTER DATABASE name=identifier SET PROPERTIES
        LEFT_PAREN propertyItemList RIGHT_PAREN                                     #alterDatabaseProperties
    | ALTER CATALOG name=identifier RENAME newName=identifier                       #alterCatalogRename
    | ALTER CATALOG name=identifier SET PROPERTIES
        LEFT_PAREN propertyItemList RIGHT_PAREN                                     #alterCatalogProperties
    | ALTER CATALOG name=identifier MODIFY COMMENT comment=STRING_LITERAL           #alterCatalogComment
    | ALTER RESOURCE name=identifierOrText properties=propertyClause?               #alterResource
    | ALTER COLOCATE GROUP name=multipartIdentifier
        SET LEFT_PAREN propertyItemList RIGHT_PAREN                                 #alterColocateGroup
    | ALTER WORKLOAD GROUP name=identifierOrText
        properties=propertyClause?                                                  #alterWorkloadGroup
    | ALTER WORKLOAD POLICY name=identifierOrText
        properties=propertyClause?                                                  #alterWorkloadPolicy
    | ALTER ROUTINE LOAD FOR name=multipartIdentifier properties=propertyClause?
            (FROM type=identifier LEFT_PAREN propertyItemList RIGHT_PAREN)?         #alterRoutineLoad
    | ALTER SQL_BLOCK_RULE name=identifier properties=propertyClause?               #alterSqlBlockRule
    | ALTER TABLE name=multipartIdentifier
        SET LEFT_PAREN propertyItemList RIGHT_PAREN                                 #alterTableProperties
    | ALTER STORAGE POLICY name=identifierOrText
        properties=propertyClause                                                   #alterStoragePlicy
    | ALTER USER (IF EXISTS)? grantUserIdentify
        passwordOption (COMMENT STRING_LITERAL)?                                    #alterUser
    | ALTER ROLE role=identifier commentSpec                                        #alterRole
    | ALTER REPOSITORY name=identifier properties=propertyClause?                   #alterRepository
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
    | ADD COLUMN LEFT_PAREN columnDef (COMMA columnDef)* RIGHT_PAREN
        toRollup? properties=propertyClause?                                        #addColumnsClause
    | DROP COLUMN name=identifier fromRollup? properties=propertyClause?            #dropColumnClause
    | MODIFY COLUMN columnDef columnPosition? fromRollup?
    properties=propertyClause?                                                      #modifyColumnClause
    | ORDER BY identifierList fromRollup? properties=propertyClause?                #reorderColumnsClause
    | ADD TEMPORARY? (lessThanPartitionDef | fixedPartitionDef | inPartitionDef)
        (LEFT_PAREN partitionProperties=propertyItemList RIGHT_PAREN)?
        (DISTRIBUTED BY (HASH hashKeys=identifierList | RANDOM)
            (BUCKETS (INTEGER_VALUE | autoBucket=AUTO))?)?
        properties=propertyClause?                                                  #addPartitionClause
    | DROP TEMPORARY? PARTITION (IF EXISTS)? partitionName=identifier FORCE?
        (FROM INDEX indexName=identifier)?                                          #dropPartitionClause
    | MODIFY TEMPORARY? PARTITION (IF EXISTS)?
        (partitionName=identifier | partitionNames=identifierList
            | LEFT_PAREN ASTERISK RIGHT_PAREN)
        SET LEFT_PAREN partitionProperties=propertyItemList RIGHT_PAREN             #modifyPartitionClause
    | REPLACE partitions=partitionSpec? WITH tempPartitions=partitionSpec?
        FORCE? properties=propertyClause?                                           #replacePartitionClause
    | REPLACE WITH TABLE name=identifier properties=propertyClause?                 #replaceTableClause
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

unsupportedDropStatement
    : DROP (DATABASE | SCHEMA) (IF EXISTS)? name=multipartIdentifier FORCE?     #dropDatabase
    | DROP CATALOG (IF EXISTS)? name=identifier                                 #dropCatalog
    | DROP (GLOBAL | SESSION | LOCAL)? FUNCTION (IF EXISTS)?
        functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN            #dropFunction
    | DROP TABLE (IF EXISTS)? name=multipartIdentifier FORCE?                   #dropTable
    | DROP USER (IF EXISTS)? userIdentify                                       #dropUser
    | DROP VIEW (IF EXISTS)? name=multipartIdentifier                           #dropView
    | DROP REPOSITORY name=identifier                                           #dropRepository
    | DROP ROLE (IF EXISTS)? name=identifier                                    #dropRole
    | DROP FILE name=STRING_LITERAL
        ((FROM | IN) database=identifier)? properties=propertyClause            #dropFile
    | DROP INDEX (IF EXISTS)? name=identifier ON tableName=multipartIdentifier  #dropIndex
    | DROP RESOURCE (IF EXISTS)? name=identifierOrText                          #dropResource
    | DROP WORKLOAD GROUP (IF EXISTS)? name=identifierOrText                    #dropWorkloadGroup
    | DROP WORKLOAD POLICY (IF EXISTS)? name=identifierOrText                   #dropWorkloadPolicy
    | DROP ENCRYPTKEY (IF EXISTS)? name=multipartIdentifier                     #dropEncryptkey
    | DROP SQL_BLOCK_RULE (IF EXISTS)? identifierSeq                            #dropSqlBlockRule
    | DROP ROW POLICY (IF EXISTS)? policyName=identifier
        ON tableName=multipartIdentifier
        (FOR (userIdentify | ROLE roleName=identifier))?                        #dropRowPolicy
    | DROP STORAGE POLICY (IF EXISTS)? name=identifier                          #dropStoragePolicy
    | DROP STAGE (IF EXISTS)? name=identifier                                   #dropStage
    ;

unsupportedStatsStatement
    : ANALYZE TABLE name=multipartIdentifier partitionSpec?
        columns=identifierList? (WITH analyzeProperties)* propertyClause?       #analyzeTable
    | ANALYZE DATABASE name=multipartIdentifier
        (WITH analyzeProperties)* propertyClause?                               #analyzeDatabase
    | ALTER TABLE name=multipartIdentifier SET STATS
        LEFT_PAREN propertyItemList RIGHT_PAREN partitionSpec?                  #alterTableStats
    | ALTER TABLE name=multipartIdentifier (INDEX indexName=identifier)?
        MODIFY COLUMN columnName=identifier
        SET STATS LEFT_PAREN propertyItemList RIGHT_PAREN partitionSpec?        #alterColumnStats
    | DROP STATS tableName=multipartIdentifier
        columns=identifierList? partitionSpec?                                  #dropStats
    | DROP CACHED STATS tableName=multipartIdentifier                           #dropCachedStats
    | DROP EXPIRED STATS                                                        #dropExpiredStats
    | DROP ANALYZE JOB INTEGER_VALUE                                            #dropAanalyzeJob
    | KILL ANALYZE jobId=INTEGER_VALUE                                          #killAnalyzeJob
    | SHOW TABLE STATS tableName=multipartIdentifier
        partitionSpec? columnList=identifierList?                               #showTableStats
    | SHOW TABLE STATS tableId=INTEGER_VALUE                                    #showTableStats
    | SHOW INDEX STATS tableName=multipartIdentifier indexId=identifier         #showIndexStats
    | SHOW COLUMN CACHED? STATS tableName=multipartIdentifier
        columnList=identifierList? partitionSpec?                               #showColumnStats
    | SHOW COLUMN HISTOGRAM tableName=multipartIdentifier
        columnList=identifierList                                               #showColumnHistogramStats
    | SHOW AUTO? ANALYZE tableName=multipartIdentifier? wildWhere?              #showAnalyze
    | SHOW ANALYZE jobId=INTEGER_VALUE wildWhere?                               #showAnalyzeFromJobId
    | SHOW AUTO JOBS tableName=multipartIdentifier? wildWhere?                  #showAutoAnalyzeJobs
    | SHOW ANALYZE TASK STATUS jobId=INTEGER_VALUE                              #showAnalyzeTask
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

unsupportedCreateStatement
    : CREATE (DATABASE | SCHEMA) (IF NOT EXISTS)? name=multipartIdentifier
        properties=propertyClause?                                              #createDatabase
    | CREATE CATALOG (IF NOT EXISTS)? catalogName=identifier
        (WITH RESOURCE resourceName=identifier)?
        (COMMENT STRING_LITERAL)? properties=propertyClause?                    #createCatalog
    | CREATE (GLOBAL | SESSION | LOCAL)?
        (TABLES | AGGREGATE)? FUNCTION (IF NOT EXISTS)?
        functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN
        RETURNS returnType=dataType (INTERMEDIATE intermediateType=dataType)?
        properties=propertyClause?                                              #createUserDefineFunction
    | CREATE (GLOBAL | SESSION | LOCAL)? ALIAS FUNCTION (IF NOT EXISTS)?
        functionIdentifier LEFT_PAREN functionArguments? RIGHT_PAREN
        WITH PARAMETER LEFT_PAREN parameters=identifierSeq? RIGHT_PAREN
        AS expression                                                           #createAliasFunction
    | CREATE USER (IF NOT EXISTS)? grantUserIdentify
        (SUPERUSER | DEFAULT ROLE role=STRING_LITERAL)?
        passwordOption (COMMENT STRING_LITERAL)?                                #createUser
    | CREATE (READ ONLY)? REPOSITORY name=identifier WITH storageBackend        #createRepository
    | CREATE ROLE (IF NOT EXISTS)? name=identifier (COMMENT STRING_LITERAL)?    #createRole
    | CREATE FILE name=STRING_LITERAL
        ((FROM | IN) database=identifier)? properties=propertyClause            #createFile
    | CREATE INDEX (IF NOT EXISTS)? name=identifier
        ON tableName=multipartIdentifier identifierList
        (USING (BITMAP | NGRAM_BF | INVERTED))?
        properties=propertyClause? (COMMENT STRING_LITERAL)?                    #createIndex
    | CREATE EXTERNAL? RESOURCE (IF NOT EXISTS)?
        name=identifierOrText properties=propertyClause?                        #createResource
    | CREATE STORAGE VAULT (IF NOT EXISTS)?
        name=identifierOrText properties=propertyClause?                        #createStorageVault
    | CREATE WORKLOAD GROUP (IF NOT EXISTS)?
        name=identifierOrText properties=propertyClause?                        #createWorkloadGroup
    | CREATE WORKLOAD POLICY (IF NOT EXISTS)? name=identifierOrText
        (CONDITIONS LEFT_PAREN workloadPolicyConditions RIGHT_PAREN)?
        (ACTIONS LEFT_PAREN workloadPolicyActions RIGHT_PAREN)?
        properties=propertyClause?                                              #createWorkloadPolicy
    | CREATE ENCRYPTKEY (IF NOT EXISTS)? multipartIdentifier AS STRING_LITERAL  #createEncryptkey
    | CREATE SQL_BLOCK_RULE (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                              #createSqlBlockRule
    | CREATE STORAGE POLICY (IF NOT EXISTS)?
        name=identifier properties=propertyClause?                              #createStoragePolicy
    | BUILD INDEX name=identifier ON tableName=multipartIdentifier
        partitionSpec?                                                          #buildIndex
    | CREATE STAGE (IF NOT EXISTS)? name=identifier properties=propertyClause?  #createStage
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
    : functionArgument (COMMA functionArgument)*
    ;

functionArgument
    : DOTDOTDOT
    | dataType
    ;

unsupportedSetStatement
    : SET (optionWithType | optionWithoutType)
        (COMMA (optionWithType | optionWithoutType))*                   #setOptions
    | SET identifier AS DEFAULT STORAGE VAULT                           #setDefaultStorageVault
    | SET PROPERTY (FOR user=identifierOrText)? propertyItemList        #setUserProperties
    | SET (GLOBAL | LOCAL | SESSION)? TRANSACTION
        ( transactionAccessMode
        | isolationLevel
        | transactionAccessMode COMMA isolationLevel
        | isolationLevel COMMA transactionAccessMode)                   #setTransaction
    ;

optionWithType
    : (GLOBAL | LOCAL | SESSION) identifier EQ (expression | DEFAULT)
    ;

optionWithoutType
    : NAMES EQ expression                                               #setNames
    | (CHAR SET | CHARSET) (charsetName=identifierOrText | DEFAULT)     #setCharset
    | NAMES (charsetName=identifierOrText | DEFAULT)
        (COLLATE collateName=identifierOrText | DEFAULT)?               #setCollate
    | PASSWORD (FOR userIdentify)? EQ (STRING_LITERAL
        | (PASSWORD LEFT_PAREN STRING_LITERAL RIGHT_PAREN))             #setPassword
    | LDAP_ADMIN_PASSWORD EQ (STRING_LITERAL
    | (PASSWORD LEFT_PAREN STRING_LITERAL RIGHT_PAREN))                 #setLdapAdminPassword
    | variable                                                          #setVariableWithoutType
    ;

variable
    : (DOUBLEATSIGN ((GLOBAL | LOCAL | SESSION) DOT)?)? identifier EQ (expression | DEFAULT) #setSystemVariable
    | ATSIGN identifier EQ expression #setUserVariable
    ;

transactionAccessMode
    : READ (ONLY | WRITE)
    ;

isolationLevel
    : ISOLATION LEVEL ((READ UNCOMMITTED) | (READ COMMITTED) | (REPEATABLE READ) | (SERIALIZABLE))
    ;

unsupoortedUnsetStatement
    : UNSET (GLOBAL | SESSION | LOCAL)? VARIABLE (ALL | identifier)
    | UNSET DEFAULT STORAGE VAULT
    ;

unsupportedUseStatement
    : USE (catalog=identifier DOT)? database=identifier                              #useDatabase
    | USE ((catalog=identifier DOT)? database=identifier)? ATSIGN cluster=identifier #useCloudCluster
    | SWITCH catalog=identifier                                                      #switchCatalog
    ;

unsupportedDmlStatement
    : TRUNCATE TABLE multipartIdentifier specifiedPartition?                        #truncateTable
    | COPY INTO selectHint? name=multipartIdentifier columns=identifierList FROM
        (stageAndPattern | (LEFT_PAREN SELECT selectColumnClause
            FROM stageAndPattern whereClause RIGHT_PAREN))
        properties=propertyClause?                                                  #copyInto
    ;

stageAndPattern
    : AT (stage=identifier | TILDE) (pattern=STRING_LITERAL)?
    ;

unsupportedKillStatement
    : KILL (CONNECTION)? INTEGER_VALUE              #killConnection
    | KILL QUERY (INTEGER_VALUE | STRING_LITERAL)   #killQuery
    ;

unsupportedDescribeStatement
    : explainCommand FUNCTION tvfName=identifier LEFT_PAREN
        (properties=propertyItemList)? RIGHT_PAREN tableAlias   #describeTableValuedFunction
    | explainCommand multipartIdentifier ALL                    #describeTableAll
    | explainCommand multipartIdentifier specifiedPartition?    #describeTable
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
          level=(VERBOSE | TREE | GRAPH | PLAN)?
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

selectHint: hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HINT_END;

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
    : multipartIdentifier optScanParams? materializedViewName? tableSnapshot? specifiedPartition?
       tabletList? tableAlias sample? relationHint? lateralView*           #tableName
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
        (DEFAULT (nullValue=NULL | INTEGER_VALUE | DECIMAL_VALUE | PI | E | stringValue=STRING_LITERAL
           | CURRENT_DATE | defaultTimestamp=CURRENT_TIMESTAMP (LEFT_PAREN defaultValuePrecision=number RIGHT_PAREN)?))?
        (ON UPDATE CURRENT_TIMESTAMP (LEFT_PAREN onUpdateValuePrecision=number RIGHT_PAREN)?)?
        (COMMENT comment=STRING_LITERAL)?
    ;

indexDefs
    : indexes+=indexDef (COMMA indexes+=indexDef)*
    ;
    
indexDef
    : INDEX (IF NOT EXISTS)? indexName=identifier cols=identifierList (USING indexType=(BITMAP | INVERTED | NGRAM_BF))? (PROPERTIES LEFT_PAREN properties=propertyItemList RIGHT_PAREN)? (COMMENT comment=STRING_LITERAL)?
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
    : FROM from=partitionValueList TO to=partitionValueList INTERVAL unitsAmount=INTEGER_VALUE unit=datetimeUnit?
    ;

inPartitionDef
    : PARTITION (IF NOT EXISTS)? partitionName=identifier (VALUES IN ((LEFT_PAREN partitionValueLists+=partitionValueList
        (COMMA partitionValueLists+=partitionValueList)* RIGHT_PAREN) | constants=partitionValueList))?
    ;
    
partitionValueList
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
    | IS NOT? kind=(TRUE | FALSE)
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
    : ACTIONS
    | ADDDATE
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
    | COMMENT_START
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPLETE
    | COMPRESS_TYPE
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
    | E
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
    | HINT_END
    | HINT_START
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
    | SET_SESSION_VARIABLE
    | SEQUENCE
    | SESSION
    | SESSION_USER
    | SHAPE
    | SKEW
    | SNAPSHOT
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
    | UNSET
    | UP
    | USER
    | VALUE
    | VARCHAR
    | VARIABLE
    | VARIABLES
    | VARIANT
    | VAULT
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
