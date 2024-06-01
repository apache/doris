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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/antlr4/org/apache/hive/hplsql/HPlsql.g4
// and modified by Doris

// PL/SQL Procedural SQL Extension Grammar
parser grammar PLParser;

options { tokenVocab = PLLexer; }

import DorisParser;

program : block EOF;

block : ((begin_end_block | stmt) GO?)+ ;               // Multiple consecutive blocks/statements

begin_end_block :
       declare_block? BEGIN block exception_block? block_end
     ;

single_block_stmt :                                      // Single BEGIN END block (but nested blocks are possible) or single statement
       BEGIN block exception_block? block_end
     | stmt SEMICOLON?
     ;

block_end :
       {!_input.LT(2).getText().equalsIgnoreCase("TRANSACTION")}? END
     ;

procedure_block :
       begin_end_block
     | stmt+ GO?
     ;

// diff with http://mail.hplsql.org/doc
// 1. doris statement include plsql stmt:
//      alter_table_stmt
//      create_database_stmt
//      create_index_stmt
//      create_table_stmt
//      delete_stmt
//      describe_stmt
//      insert_stmt
//      insert_directory_stmt
//      grant_stmt
//      select_stmt
//      update_stmt
//      use_stmt
//      truncate_stmt
//
// 2. TODO doris statement add stmt:
//      begin_transaction_stmt
//      end_transaction_stmt
//      commit_stmt
//      rollback_stmt
//
// 3. TODO add plsql stmt:
//      cmp_stmt
//      copy_from_local_stmt
//      copy_stmt
//      create_local_temp_table_stmt
//      merge_stmt
//
// 4. delete hplsql stmt
//      collect_stats_stmt
//      summary_stmt
//      create_table_type_stmt
//      hive
doris_statement :
       statementBase
     ;

stmt :
       doris_statement
     | assignment_stmt
     | allocate_cursor_stmt
     | associate_locator_stmt
     | break_stmt
     | call_stmt
     | close_stmt
     | create_function_stmt // like a simple procedure
     | create_package_stmt
     | create_package_body_stmt
     | create_procedure_stmt
     | declare_stmt
     | drop_procedure_stmt
     | show_procedure_stmt
     | show_create_procedure_stmt
     | exec_stmt
     | exit_stmt
     | fetch_stmt
     | for_cursor_stmt
     | for_range_stmt
     | if_stmt
     | include_stmt
     | get_diag_stmt
     | leave_stmt
     | map_object_stmt
     | open_stmt
     | print_stmt
     | quit_stmt
     | raise_stmt
     | resignal_stmt
     | return_stmt
     | signal_stmt
     | values_into_stmt
     | while_stmt
     | unconditional_loop_stmt
     | label_stmt
     | host_pl
     | null_stmt
     | expr_stmt
     | semicolon_stmt      // Placed here to allow null statements ;;...
     ;

semicolon_stmt :
       SEMICOLON
     | '@' | '/'       //  '#' |
     ;

exception_block :       // Exception block
       EXCEPTION exception_block_item+
     ;

exception_block_item :
       WHEN IDENTIFIER THEN block ~(WHEN | END)
     ;

null_stmt :             // NULL statement (no operation)
       NULL
     ;

expr_stmt :             // Standalone expression
       {!_input.LT(1).getText().equalsIgnoreCase("GO")}? expr
     ;

assignment_stmt :       // Assignment statement
       SET set_session_option
     | SET? assignment_stmt_item (COMMA assignment_stmt_item)*
     ;

assignment_stmt_item :
       assignment_stmt_single_item
     | assignment_stmt_multiple_item
     | assignment_stmt_select_item
     | assignment_stmt_collection_item
     ;

assignment_stmt_single_item :
       ident_pl COLON? EQ expr
     | LEFT_PAREN ident_pl RIGHT_PAREN COLON? EQ expr
     ;

assignment_stmt_collection_item :
    expr_func COLON EQ expr
    ;

assignment_stmt_multiple_item :
       LEFT_PAREN ident_pl (COMMA ident_pl)* RIGHT_PAREN COLON? EQ LEFT_PAREN expr (COMMA expr)* RIGHT_PAREN
     ;

assignment_stmt_select_item :
       (ident_pl | (LEFT_PAREN ident_pl (COMMA ident_pl)* RIGHT_PAREN)) COLON? EQ LEFT_PAREN query RIGHT_PAREN
     ;

allocate_cursor_stmt:
       ALLOCATE ident_pl CURSOR FOR ((RESULT SET) | PROCEDURE) ident_pl
     ;

associate_locator_stmt :
       ASSOCIATE (RESULT SET)? (LOCATOR | LOCATORS) LEFT_PAREN ident_pl (COMMA ident_pl)* RIGHT_PAREN WITH PROCEDURE ident_pl
     ;

break_stmt :
       BREAK
     ;

call_stmt :
       CALL (expr_dot | expr_func | multipartIdentifier)
     ;

declare_stmt :          // Declaration statement
       DECLARE declare_stmt_item (COMMA declare_stmt_item)*
     ;

declare_block :         // Declaration block
       DECLARE declare_stmt_item SEMICOLON (declare_stmt_item SEMICOLON)*
     ;

declare_block_inplace :
       declare_stmt_item SEMICOLON (declare_stmt_item SEMICOLON)*
     ;

declare_stmt_item :
       declare_cursor_item
     | declare_condition_item
     | declare_handler_item
     | declare_var_item
     ; // TODO declare_temporary_table_item

declare_var_item :
       ident_pl (COMMA ident_pl)* AS? dtype dtype_len? dtype_attr* dtype_default?
     | ident_pl CONSTANT AS? dtype dtype_len? dtype_default
     ;

declare_condition_item :    // Condition declaration
       ident_pl CONDITION
     ;

declare_cursor_item :      // Cursor declaration
       (CURSOR ident_pl | ident_pl CURSOR) (cursor_with_return | cursor_without_return)? (IS | AS | FOR) (query | expr )
     ;

cursor_with_return :
       WITH RETURN ONLY? (TO (CALLER | CLIENT))?
     ;

cursor_without_return :
       WITHOUT RETURN
     ;

declare_handler_item :     // Condition handler declaration
       (CONTINUE | EXIT) HANDLER FOR (SQLEXCEPTION | SQLWARNING | NOT FOUND | ident_pl) single_block_stmt
     ;

dtype :                  // Data types
       CHAR
     | BIGINT
     | BINARY_DOUBLE
     | BINARY_FLOAT
     | BINARY_INTEGER
     | BIT
     | DATE
     | DATETIME
     | DEC
     | DECIMAL
     | DOUBLE PRECISION?
     | FLOAT
     | INT
     | INT2
     | INT4
     | INT8
     | INTEGER
     | NCHAR
     | NVARCHAR
     | NUMBER
     | NUMERIC
     | PLS_INTEGER
     | REAL
     | RESULT_SET_LOCATOR VARYING
     | SIMPLE_FLOAT
     | SIMPLE_DOUBLE
     | SIMPLE_INTEGER
     | SMALLINT
     | SMALLDATETIME
     | STRING
     | SYS_REFCURSOR
     | TIMESTAMP
     | TINYINT
     | VARCHAR
     | VARCHAR2
     | XML
     | qident ('%' (TYPE | ROWTYPE))?             // User-defined or derived data type
     ;

dtype_len :             // Data type length or size specification
       LEFT_PAREN (INTEGER_VALUE | MAX) (CHAR | BYTE)? (COMMA INTEGER_VALUE)? RIGHT_PAREN
     ;

dtype_attr :
       NOT? NULL
     | CHAR SET ident_pl
     | NOT? (CASESPECIFIC | CS)
     ;

dtype_default :
       COLON? EQ expr
     | WITH? DEFAULT expr?
     ;

create_function_stmt :
      (ALTER | CREATE (OR REPLACE)? | REPLACE) FUNCTION multipartIdentifier create_routine_params? create_function_return (AS | IS)? declare_block_inplace? single_block_stmt
    ;

create_function_return :
       (RETURN | RETURNS) dtype dtype_len?
     ;

create_package_stmt :
      (ALTER | CREATE (OR REPLACE)? | REPLACE) PACKAGE multipartIdentifier (AS | IS) package_spec END (ident_pl SEMICOLON)?
    ;

package_spec :
      package_spec_item SEMICOLON (package_spec_item SEMICOLON)*
    ;

package_spec_item :
      declare_stmt_item
    | FUNCTION ident_pl create_routine_params? create_function_return
    | (PROCEDURE | PROC) ident_pl create_routine_params?
    ;

create_package_body_stmt :
      (ALTER | CREATE (OR REPLACE)? | REPLACE) PACKAGE BODY multipartIdentifier (AS | IS) package_body END (ident_pl SEMICOLON)?
    ;

package_body :
      package_body_item SEMICOLON (package_body_item SEMICOLON)*
    ;

package_body_item :
      declare_stmt_item
    | create_function_stmt
    | create_procedure_stmt
    ;

create_procedure_stmt :
      (ALTER | CREATE (OR REPLACE)? | REPLACE) (PROCEDURE | PROC) multipartIdentifier create_routine_params? create_routine_options? (AS | IS)? declare_block_inplace? label_stmt? procedure_block (ident_pl SEMICOLON)?
    ;
drop_procedure_stmt:
      DROP (PROCEDURE | PROC) (IF EXISTS)? name=multipartIdentifier
    ;
show_procedure_stmt:
      SHOW PROCEDURE STATUS (LIKE pattern=valueExpression | whereClause)?
    ;

show_create_procedure_stmt:
      SHOW CREATE PROCEDURE name=multipartIdentifier
    ;      

create_routine_params :
       LEFT_PAREN RIGHT_PAREN
     | LEFT_PAREN create_routine_param_item (COMMA create_routine_param_item)* RIGHT_PAREN
     | {!_input.LT(1).getText().equalsIgnoreCase("IS") &&
        !_input.LT(1).getText().equalsIgnoreCase("AS") &&
        !(_input.LT(1).getText().equalsIgnoreCase("DYNAMIC") && _input.LT(2).getText().equalsIgnoreCase("RESULT"))
        }?
       create_routine_param_item (COMMA create_routine_param_item)*
     ;

create_routine_param_item :
       (IN | OUT | INOUT | IN OUT)? ident_pl dtype dtype_len? dtype_attr* dtype_default?
     | ident_pl (IN | OUT | INOUT | IN OUT)? dtype dtype_len? dtype_attr* dtype_default?
     ;

create_routine_options :
       create_routine_option+
     ;
create_routine_option :
       LANGUAGE SQL
     | SQL SECURITY (CREATOR | DEFINER | INVOKER | OWNER)
     | DYNAMIC? RESULT SETS INTEGER_VALUE
     ;

exec_stmt :             // EXEC, EXECUTE IMMEDIATE statement
       (EXEC | EXECUTE) IMMEDIATE? expr (LEFT_PAREN expr_func_params RIGHT_PAREN | expr_func_params)? (INTO IDENTIFIER (COMMA IDENTIFIER)*)? using_clause?
     ;

if_stmt :               // IF statement
       if_plsql_stmt
     | if_tsql_stmt
     | if_bteq_stmt
     ;

if_plsql_stmt :
       IF bool_expr THEN block elseif_block* else_block? END IF
     ;

if_tsql_stmt :
       IF bool_expr single_block_stmt (ELSE single_block_stmt)?
     ;

if_bteq_stmt :
       '.' IF bool_expr THEN single_block_stmt
     ;

elseif_block :
       (ELSIF | ELSEIF) bool_expr THEN block
     ;

else_block :
       ELSE block
     ;

include_stmt :          // INCLUDE statement
       INCLUDE (file_name | expr)
     ;

exit_stmt :
       EXIT IDENTIFIER? (WHEN bool_expr)?
     ;

get_diag_stmt :         // GET DIAGNOSTICS statement
       GET DIAGNOSTICS get_diag_stmt_item
     ;

get_diag_stmt_item :
       get_diag_stmt_exception_item
     | get_diag_stmt_rowcount_item
     ;

get_diag_stmt_exception_item :
       EXCEPTION INTEGER_VALUE qident EQ MESSAGE_TEXT
     ;

get_diag_stmt_rowcount_item :
       qident EQ ROW_COUNT
     ;

leave_stmt :
       LEAVE IDENTIFIER?
     ;

map_object_stmt :
       MAP OBJECT ident_pl (TO ident_pl)? (AT ident_pl)?
     ;

open_stmt :             // OPEN cursor statement
         OPEN ident_pl (FOR (query | expr))?
     ;

fetch_stmt :            // FETCH cursor statement
       FETCH FROM? ident_pl bulkCollectClause? INTO ident_pl (COMMA ident_pl)* fetch_limit?
     ;

fetch_limit:
      LIMIT expr
     ;

close_stmt :            // CLOSE cursor statement
       CLOSE IDENTIFIER
     ;

print_stmt :            // PRINT statement
       PRINT expr
     | PRINT LEFT_PAREN expr RIGHT_PAREN
     ;

quit_stmt :
       '.'? QUIT expr?
     ;

raise_stmt :
       RAISE
     ;

resignal_stmt :         // RESIGNAL statement
       RESIGNAL (SQLSTATE VALUE? expr (SET MESSAGE_TEXT EQ expr)? )?
     ;

return_stmt :           // RETURN statement
       RETURN expr?
     ;

// Plsql allows setting some local variables, which conflicts with doris set session veriable.
// First position is matched first, so all sets will be treated as set session veriables first.
set_session_option :
       set_doris_session_option
     | set_current_schema_option
     | set_mssql_session_option
     | set_teradata_session_option
     ;

set_doris_session_option :
       (GLOBAL | LOCAL | SESSION)? ident_pl EQ ident_pl
      ;

set_current_schema_option :
       ((CURRENT? SCHEMA) | CURRENT_SCHEMA) EQ? expr
     ;

set_mssql_session_option :
     ( ANSI_NULLS
     | ANSI_PADDING
     | NOCOUNT
     | QUOTED_IDENTIFIER
     | XACT_ABORT )
     (ON | OFF)
     ;

set_teradata_session_option :
       QUERY_BAND EQ (expr | NONE) UPDATE? FOR (TRANSACTION | SESSION)
     ;

signal_stmt :          // SIGNAL statement
       SIGNAL ident_pl
     ;

values_into_stmt :     // VALUES INTO statement
       VALUES LEFT_PAREN? expr (COMMA expr)* RIGHT_PAREN? INTO LEFT_PAREN? ident_pl (COMMA ident_pl)* RIGHT_PAREN?
     ;

while_stmt :            // WHILE loop statement
       WHILE bool_expr (DO | LOOP | THEN | BEGIN) block END (WHILE | LOOP)?
     ;

unconditional_loop_stmt : // LOOP .. END LOOP
       LOOP block END LOOP
     ;

for_cursor_stmt :       // FOR (cursor) statement
       FOR IDENTIFIER IN LEFT_PAREN? query RIGHT_PAREN? LOOP block END LOOP
     ;

for_range_stmt :        // FOR (Integer range) statement
       FOR IDENTIFIER IN REVERSE? expr DOT2 expr ((BY | STEP) expr)? LOOP block END LOOP
     ;

label_stmt :
       IDENTIFIER COLON
     | LT LT IDENTIFIER GT GT
     ;

using_clause :          // USING var,... clause
       USING expr (COMMA expr)*
     ;

bool_expr :                               // Boolean condition
       NOT? LEFT_PAREN bool_expr RIGHT_PAREN
     | bool_expr bool_expr_logical_operator bool_expr
     | bool_expr_atom
     ;

bool_expr_atom :
      bool_expr_unary
    | bool_expr_binary
    | expr
    ;

bool_expr_unary :
      expr IS NOT? NULL
    | expr BETWEEN expr AND expr
    ; // TODO NOT? EXISTS LEFT_PAREN query RIGHT_PAREN, bool_expr_single_in, bool_expr_multi_in

bool_expr_binary :
       expr bool_expr_binary_operator expr
     ;

bool_expr_logical_operator :
       AND
     | OR
     ;

bool_expr_binary_operator :
       EQ
     | NEQ
     | LT
     | LTE
     | GT
     | GTE
     | NOT? (LIKE | RLIKE | REGEXP)
     ;

expr :
       expr interval_item
     | expr (ASTERISK | SLASH) expr
     | expr (PLUS | SUBTRACT) expr
     | LEFT_PAREN query RIGHT_PAREN
     | LEFT_PAREN expr RIGHT_PAREN
     | expr_interval
     | expr_concat
     | expr_dot
     | expr_case
     | expr_cursor_attribute
     | expr_agg_window_func
     | expr_spec_func
     | expr_func
     | expr_atom
     ;

expr_atom :
       date_literal
     | timestamp_literal
     | bool_literal
     | qident
     | string
     | dec_number
     | int_number
     | null_const
     ;

expr_interval :
       INTERVAL expr interval_item
     ;
interval_item :
       DAY
     | DAYS
     | MICROSECOND
     | MICROSECONDS
     | SECOND
     | SECONDS
     ;

expr_concat :                  // String concatenation operator
       expr_concat_item (DOUBLEPIPES | CONCAT) expr_concat_item ((DOUBLEPIPES | CONCAT) expr_concat_item)*
     ;

expr_concat_item :
       LEFT_PAREN expr RIGHT_PAREN
     | expr_case
     | expr_agg_window_func
     | expr_spec_func
     | expr_dot
     | expr_func
     | expr_atom
     ;

expr_case :                    // CASE expression
       expr_case_simple
     | expr_case_searched
     ;

expr_case_simple :
       CASE expr (WHEN expr THEN expr)+ (ELSE expr)? END
     ;

expr_case_searched :
       CASE (WHEN bool_expr THEN expr)+ (ELSE expr)? END
     ;

expr_cursor_attribute :
      ident_pl '%' (ISOPEN | FOUND | NOTFOUND)
    ;

expr_agg_window_func :
       AVG LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | COUNT LEFT_PAREN ((expr_func_all_distinct? expr) | '*') RIGHT_PAREN expr_func_over_clause?
     | COUNT_BIG LEFT_PAREN ((expr_func_all_distinct? expr) | '*') RIGHT_PAREN expr_func_over_clause?
     | CUME_DIST LEFT_PAREN RIGHT_PAREN expr_func_over_clause
     | DENSE_RANK LEFT_PAREN RIGHT_PAREN expr_func_over_clause
     | FIRST_VALUE LEFT_PAREN expr RIGHT_PAREN expr_func_over_clause
     | LAG LEFT_PAREN expr (COMMA expr (COMMA expr)?)? RIGHT_PAREN expr_func_over_clause
     | LAST_VALUE LEFT_PAREN expr RIGHT_PAREN expr_func_over_clause
     | LEAD LEFT_PAREN expr (COMMA expr (COMMA expr)?)? RIGHT_PAREN expr_func_over_clause
     | MAX LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | MIN LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | RANK LEFT_PAREN RIGHT_PAREN expr_func_over_clause
     | ROW_NUMBER LEFT_PAREN RIGHT_PAREN expr_func_over_clause
     | STDEV LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | SUM LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | VAR LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     | VARIANCE LEFT_PAREN expr_func_all_distinct? expr RIGHT_PAREN expr_func_over_clause?
     ;

expr_func_all_distinct :
       ALL
     | DISTINCT
     ;

order_by_clause :
       ORDER BY expr (ASC | DESC)? (COMMA expr (ASC | DESC)?)*
     ;

expr_func_over_clause :
       OVER LEFT_PAREN expr_func_partition_by_clause? order_by_clause? RIGHT_PAREN
     ;

expr_func_partition_by_clause :
       PARTITION BY expr (COMMA expr)*
     ;

expr_spec_func :
       ACTIVITY_COUNT
     | CAST LEFT_PAREN expr AS  dtype dtype_len? RIGHT_PAREN
     | COUNT LEFT_PAREN (expr | '*') RIGHT_PAREN
     | CURRENT_DATE | CURRENT DATE
     | (CURRENT_TIMESTAMP | CURRENT TIMESTAMP) (LEFT_PAREN expr RIGHT_PAREN)?
     | CURRENT_USER | CURRENT USER
     | MAX_PART_STRING LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | MIN_PART_STRING LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | MAX_PART_INT LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | MIN_PART_INT LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | MAX_PART_DATE LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | MIN_PART_DATE LEFT_PAREN expr (COMMA expr (COMMA expr EQ expr)*)? RIGHT_PAREN
     | PART_COUNT LEFT_PAREN expr (COMMA expr EQ expr)* RIGHT_PAREN
     | PART_LOC LEFT_PAREN expr (COMMA expr EQ expr)+ (COMMA expr)? RIGHT_PAREN
     | TRIM LEFT_PAREN expr RIGHT_PAREN
     | SUBSTRING LEFT_PAREN expr FROM expr (FOR expr)? RIGHT_PAREN
     | SYSDATE
     | USER
     ;

expr_func :
       multipartIdentifier LEFT_PAREN expr_func_params? RIGHT_PAREN
     ;

expr_dot :
       expr_dot_method_call | expr_dot_property_access
      ;

expr_dot_method_call :
       (ident_pl | expr_func) DOT expr_func
      ;

expr_dot_property_access :
       (ident_pl | expr_func) DOT ident_pl
      ;

expr_func_params :
       func_param (COMMA func_param)*
     ;

func_param :
       {!_input.LT(1).getText().equalsIgnoreCase("INTO")}? (ident_pl EQ GT?)? expr
     ;

host_pl :
       '!' host_cmd  ';'                   // OS command
     | host_stmt
     ;

host_cmd :
       .*?
     ;

host_stmt :
       HOST expr
     ;

file_name :
       STRING_LITERAL | ('/' | '.' '/')? qident ('/' qident)*
     ;

date_literal :                             // DATE 'YYYY-MM-DD' literal
       DATE string
     ;

timestamp_literal :                       // TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFF' literal
       TIMESTAMP string
     ;

ident_pl :
       '-'? (IDENTIFIER | non_reserved_words | nonReserved)
     ;

qident :                                  // qualified identifier e.g: table_name.col_name or db_name._table_name
       ident_pl ('.'ident_pl)*
     ;

string :                                   // String literal (single or double quoted)
       STRING_LITERAL
     ;

int_number :                               // Integer (positive or negative)
     ('-' | '+')? INTEGER_VALUE
     ;

dec_number :                               // Decimal number (positive or negative)
     ('-' | '+')? DECIMAL_VALUE
     ;

bool_literal :                            // Boolean literal
       TRUE
     | FALSE
     ;

null_const :                              // NULL constant
       NULL
     ;

non_reserved_words :                      // Tokens that are not reserved words and can be used as identifiers
       ACTION
     | ACTIVITY_COUNT
     | ALLOCATE
     | ANSI_NULLS
     | ANSI_PADDING
     | ASSOCIATE
     | AVG
     | BATCHSIZE
     | BINARY_DOUBLE
     | BINARY_FLOAT
     | BIT
     | BODY
     | BREAK
     | BYTE
     | CALLER
     | CASCADE
     | CASESPECIFIC
     | CLIENT
     | CLOSE
     | CLUSTERED
     | CMP
     | COLLECTION
     | COMPRESS
     | CONSTANT
     | CONCAT
     | CONDITION
     | COUNT_BIG
     | CREATOR
     | CS
     | CUME_DIST
     | CURRENT_DATE
     | CURRENT_TIMESTAMP
     | CURRENT_USER
     | CURSOR
     | DAYS
     | DEC
     | DECLARE
     | DEFINED
     | DEFINER
     | DEFINITION
     | DELIMITED
     | DELIMITER
     | DENSE_RANK
     | DIAGNOSTICS
     | DIR
     | DIRECTORY
     | DISTRIBUTE
     | ESCAPED
     | EXEC
     | EXCEPTION
     | EXCLUSIVE
     | EXIT
     | FALLBACK
     | FETCH
     | FILES
     | FIRST_VALUE
     | FOUND
     | GET
     | GO
     | HANDLER
     | HOST
     | IDENTITY
     | INCLUDE
     | INITRANS
     | INOUT
     | INT2
     | INT4
     | INT8
     | INVOKER
     | ITEMS
     | ISOPEN
     | KEEP
     | KEYS
     | LAG
     | LANGUAGE
     | LAST_VALUE
     | LEAD
     | LEAVE
     | LOCATOR
     | LOCATORS
     | LOCKS
     | LOG
     | LOGGED
     | LOGGING
     | LOOP
     | MATCHED
     | MAXTRANS
     | MESSAGE_TEXT
     | MICROSECOND
     | MICROSECONDS
     | MULTISET
     | NCHAR
     | NEW
     | NVARCHAR
     | NOCOMPRESS
     | NOCOUNT
     | NOLOGGING
     | NONE
     | NOTFOUND
     | NUMERIC
     | NUMBER
     | OBJECT
     | OFF
     | OUT
     | OWNER
     | PACKAGE
     | PART_COUNT
     | PART_LOC
     | PCTFREE
     | PCTUSED
     | PI
     | PRECISION
     | PRESERVE
     | PRINT
     | PWD
     | QUALIFY
     | QUERY_BAND
     | QUIT
     | QUOTED_IDENTIFIER
     | RAISE
     | RANK
     | RR
     | RESIGNAL
     | RESTRICT
     | RESULT
     | RESULT_SET_LOCATOR
     | RETURN
     | REVERSE
     | RS
     | ROW_COUNT
     | ROW_NUMBER
     | SECONDS
     | SECURITY
     | SEGMENT
     | SEL
     | SESSIONS
     | SHARE
     | SIGNAL
     | SIMPLE_DOUBLE
     | SIMPLE_FLOAT
     | SMALLDATETIME
     | SQL
     | SQLEXCEPTION
     | SQLINSERT
     | SQLSTATE
     | SQLWARNING
     | STATISTICS
     | STEP
     | STDEV
     | STORED
     | SUBDIR
     | SUBSTRING
     | SUMMARY
     | SYSDATE
     | SYS_REFCURSOR
     | TABLESPACE
     | TEXTIMAGE_ON
     | TITLE
     | TOP
     | UR
     | VAR
     | VARCHAR2
     | VARYING
     | VARIANCE
     | VOLATILE
     | WHILE
     | WITHOUT
     | XML
     | YES
     ;
