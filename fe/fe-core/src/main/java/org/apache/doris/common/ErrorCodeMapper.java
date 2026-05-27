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

package org.apache.doris.common;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class ErrorCodeMapper {

    private static final Map<Pattern, ErrorCode> errorMap = new HashMap<>();
    private static final Pattern BACKEND_UNAVAILABLE_KEYWORDS = Pattern.compile(
            "dead|offline|unavailable|not alive|is not alive|Alive\\s*=\\s*false|heartbeat|"
                    + "无可用|不可用|已下线|离线|无心跳",
            Pattern.CASE_INSENSITIVE);

    static {
        errorMap.put(Pattern.compile("Can't create database"), ErrorCode.ERR_DB_CREATE_EXISTS);
        errorMap.put(Pattern.compile("No database selected"), ErrorCode.ERR_NO_DB_ERROR);
        errorMap.put(Pattern.compile("Current database is not set"), ErrorCode.ERR_NO_DB_ERROR);
        errorMap.put(Pattern.compile("Unknown database"), ErrorCode.ERR_BAD_DB_ERROR);
        errorMap.put(Pattern.compile("Database \\[.*\\] does not exist"), ErrorCode.ERR_BAD_DB_ERROR);
        errorMap.put(Pattern.compile("Unknown column"), ErrorCode.ERR_BAD_FIELD_ERROR);
        errorMap.put(Pattern.compile("Not unique table/alias"), ErrorCode.ERR_NONUNIQ_TABLE);
        errorMap.put(Pattern.compile("Unknown thread id"), ErrorCode.ERR_NO_SUCH_THREAD);
        errorMap.put(Pattern.compile("Unknown error"), ErrorCode.ERR_UNKNOWN_ERROR);
        // Narrow by auth keywords: only permission-related MetaException maps to table auth denied.
        errorMap.put(Pattern.compile(
                "org\\.apache\\.hadoop\\.hive\\.metastore\\.api\\.MetaException:.*"
                        + "(?i)(Permission denied|Access denied|has no privilege|permission denied)"),
                ErrorCode.ERR_TABLEACCESS_DENIED_ERROR);
        // Non-auth MetaException should keep its own semantic bucket.
        errorMap.put(Pattern.compile("org\\.apache\\.hadoop\\.hive\\.metastore\\.api\\.MetaException"),
                ErrorCode.ERR_MetaException);
        errorMap.put(Pattern.compile("has no privilege to query in catalog"),
                ErrorCode.ERR_CATALOG_ACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("Access denied"), ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("can't be set to the value of"), ErrorCode.ERR_LOCAL_VARIABLE);
        errorMap.put(Pattern.compile("Unknown system variable"), ErrorCode.ERR_LOCAL_VARIABLE);
        errorMap.put(Pattern.compile("Every derived table must have its own alias"),
                ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
        errorMap.put(Pattern.compile("is ambiguous"), ErrorCode.ERR_AMBIGUOUS_FIELD_TERM);
        errorMap.put(Pattern.compile("The parameter 2 or parameter 3 of LAG/LEAD must be a constant value"),
                ErrorCode.ERR_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR);
        errorMap.put(Pattern.compile("Can not found function"), ErrorCode.ERR_FUNC_INEXISTENT_NAME_COLLISION);
        errorMap.put(Pattern.compile("show query/load profile syntax is a deprecated feature"),
                ErrorCode.ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT);
        errorMap.put(Pattern.compile("send fragments failed"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("io.grpc.StatusRuntimeException"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("no viable alternative at input"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("mismatched input"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("java.lang.NullPointerException"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("Cannot invoke"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("Failed to get query fragments context"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("There is no scanNode Backend available"), ErrorCode.ERR_BACKEND_OFFLINE);
        errorMap.put(Pattern.compile("No available backends"), ErrorCode.ERR_BACKEND_OFFLINE);
        errorMap.put(Pattern.compile("Timeout"), ErrorCode.ERR_EXECUTE_TIMEOUT);
        errorMap.put(Pattern.compile("Unknown catalog"), ErrorCode.ERR_UNKNOWN_CATALOG);
        errorMap.put(Pattern.compile("No catalog found with name"), ErrorCode.ERR_UNKNOWN_CATALOG);
        errorMap.put(Pattern.compile("sum requires a numeric or boolean parameter"),
                ErrorCode.ERR_SUM_REQUIRES_NUMERIC_OR_BOOLEAN);
        errorMap.put(Pattern.compile("we meet an error when parsing"), ErrorCode.ERR_SQL_PARSING_ERROR);
        errorMap.put(Pattern.compile("Unsupported hive input format"),
                ErrorCode.ERR_UNSUPPORTED_HIVE_INPUT_FORMAT);
        errorMap.put(Pattern.compile("not in aggregate's output"), ErrorCode.ERR_NOT_IN_AGGREGATE_OUTPUT);
        errorMap.put(Pattern.compile("Nereids cost too much time"), ErrorCode.ERR_NEREIDS_TIMEOUT);
        errorMap.put(Pattern.compile("Invalid call to toSlot on unbound object"),
                ErrorCode.ERR_INVALID_CALL_TO_SLOT);
        // AUTH semantic should take precedence over generic split failure.
        // Some hive/hudi permission failures are wrapped as:
        // "get file split failed for table, err: ... MetaException ... Permission denied".
        errorMap.put(Pattern.compile(
                "get file split failed for table.*MetaException.*"
                        + "(?i)(Permission denied|Access denied|has no privilege|permission denied)"),
                ErrorCode.ERR_TABLEACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("get file split failed for table"),
                ErrorCode.ERR_GET_FILE_SPLIT_FAILED_FOR_TABLE);
        errorMap.put(Pattern.compile("failed to open transaction"), ErrorCode.ERR_FAILED_TO_OPEN_TRANSACTION);
        errorMap.put(Pattern.compile("is not mutable"), ErrorCode.ERR_CONFIG_NOT_MUTABLE);
        errorMap.put(Pattern.compile("Catalog had already exist with name"),
                ErrorCode.ERR_CATALOG_ALREADY_EXISTS);
        errorMap.put(Pattern.compile("Can not find the compatibility function signature"),
                ErrorCode.ERR_COMPATIBILITY_FUNCTION_SIGNATURE_NOT_FOUND);
        errorMap.put(Pattern.compile("Backend process epoch changed"),
                ErrorCode.ERR_BACKEND_PROCESS_EPOCH_CHANGED);
        errorMap.put(Pattern.compile("should be grouped by"), ErrorCode.ERR_SHOULD_BE_GROUPED_BY);
        errorMap.put(Pattern.compile("Failed to connect to backend"), ErrorCode.ERR_FAILED_TO_CONNECT_BACKEND);
        errorMap.put(Pattern.compile("Failed to get batch of split source"),
                ErrorCode.ERR_FETCH_SPLIT_BATCH_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("failed to init reader"), ErrorCode.ERR_FAILED_TO_INIT_READER);
        errorMap.put(Pattern.compile("MinorGC kill overcommit query"),
                ErrorCode.ERR_MINORGC_KILL_OVERCOMMIT_QUERY);
        errorMap.put(Pattern.compile("cancel top memory used query"),
                ErrorCode.ERR_PROCESS_MEMORY_NOT_ENOUGH_CANCEL_QUERY);
        errorMap.put(Pattern.compile("MEM_ALLOC_FAILED"), ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED);
        errorMap.put(Pattern.compile("MEM_LIMIT_EXCEEDED"), ErrorCode.ERR_PRECATCH_MEM_LIMIT_EXCEEDED);
        errorMap.put(Pattern.compile("failed to send brpc when exchange"),
                ErrorCode.ERR_FAILED_TO_SEND_BRPC_HOST_DOWN);
        errorMap.put(Pattern.compile("hyperscan"), ErrorCode.ERR_HYPERSCAN_ERROR);
        errorMap.put(Pattern.compile("Cancelled"), ErrorCode.ERR_CANCELLED);
        errorMap.put(Pattern.compile("Orc row reader nextBatch failed"),
                ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED);
        errorMap.put(Pattern.compile("FullGC release wg overcommit mem"),
                ErrorCode.ERR_FULLGC_RELEASE_OVERCOMMIT_MEM);
        errorMap.put(Pattern.compile("Can not build QueryTableValuedFunction by query"),
                ErrorCode.ERR_CANNOT_BUILD_QUERY_TABLE_VALUED_FUNCTION);
        errorMap.put(Pattern.compile("Syntax error"), ErrorCode.ERR_SYNTAX_ERROR);
        errorMap.put(Pattern.compile("Unmatched string literal"), ErrorCode.ERR_UMATCHED_STRING_LITERAL);
        errorMap.put(Pattern.compile("Incomplete escape sequence"), ErrorCode.ERR_ILLEGAL_STATE_EXCEPTION);
        errorMap.put(Pattern.compile("user cancel"), ErrorCode.ERR_USER_CANCELED);
        errorMap.put(Pattern.compile("timeout when waiting for send fragments rpc"),
                ErrorCode.ERR_SEND_FRAGMENTS_FAILED);
        errorMap.put(Pattern.compile("Process memory not enough"),
                ErrorCode.ERR_PROCESS_MEMORY_NOT_ENOUGH_CANCEL_QUERY);
        errorMap.put(Pattern.compile("Query may be timeout or be cancelled"),
                ErrorCode.ERR_USER_CANCELED);
        errorMap.put(Pattern.compile("query timeout"), ErrorCode.ERR_EXECUTE_TIMEOUT);
        errorMap.put(Pattern.compile("Allocator sys memory check failed"),
                ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED);
        errorMap.put(Pattern.compile("Table .* does not exist in database"),
                ErrorCode.ERR_TABLE_DOES_NOT_EXIST_IN_DATABASE);
        errorMap.put(Pattern.compile("failed to get table .*NoSuchObjectException.*\\$partitions table not found"),
                ErrorCode.ERR_TABLE_PARTITIONS_TABLE_NOT_FOUND);
        errorMap.put(Pattern.compile("failed to get table.*NoSuchObjectException.*table not found"),
                ErrorCode.ERR_NO_SUCH_OBJECT);
        errorMap.put(Pattern.compile("Only support csv data in utf8 codec"),
                ErrorCode.ERR_ONLY_SUPPORT_CSV_DATA_IN_UTF8_CODEC);
        errorMap.put(Pattern.compile("please check your sql"),
                ErrorCode.ERR_SQL_PARSING_ERROR);
        errorMap.put(Pattern.compile("External catalog .* is not allowed in 'DescribeStmt ALL'"),
                ErrorCode.ERR_IS_NOT_ALLOWED_IN_DESCRIBE_STMT_ALL);
        errorMap.put(Pattern.compile("Failed to create orc row reader"),
                ErrorCode.ERR_FAILED_TO_CREATE_ORC_ROW_READER);
        errorMap.put(Pattern.compile("ParseException"),
                ErrorCode.ERR_PARSE_EXCEPTION);
        errorMap.put(Pattern.compile(
                        "failed to get table.*MetaException.*"
                                + "(?i)(Permission denied|Access denied|has no privilege|permission denied)"),
                ErrorCode.ERR_TABLEACCESS_DENIED_ERROR);

        errorMap.put(Pattern.compile("Query timeout|query timeout"), ErrorCode.ERR_QUERY_TIMEOUT);
        errorMap.put(Pattern.compile("cancel query by user from"), ErrorCode.ERR_USER_CANCEL);
        errorMap.put(Pattern.compile("query is cancelled"), ErrorCode.ERR_QUERY_CANCELLED);
        errorMap.put(Pattern.compile("Read hdfs file failed"), ErrorCode.ERR_READ_HDFS_FILE_FAILED_CN);
        errorMap.put(Pattern.compile("fail to offer request to the work pool"),
                ErrorCode.ERR_WORK_POOL_OFFER_FAILED);
        errorMap.put(Pattern.compile("column must use with specific function"),
                ErrorCode.ERR_COLUMN_MUST_WITH_SPEC_FUNC);
        errorMap.put(Pattern.compile("Failed to submit scanner to scanner pool reason"),
                ErrorCode.ERR_SUBMIT_SCANNER_FAILED);
        errorMap.put(Pattern.compile("Could not compile regexp pattern"),
                ErrorCode.ERR_REGEX_COMPILE_FAILED);
        errorMap.put(Pattern.compile("UDF failed to evaluate"),
                ErrorCode.ERR_UDF_EVALUATE_FAILED_CN);
        errorMap.put(Pattern.compile("prune hive partitions failed"),
                ErrorCode.ERR_PRUNE_HIVE_PARTITIONS_FAILED);
        errorMap.put(Pattern.compile("Infinity result is invalid"),
                ErrorCode.ERR_INFINITY_RESULT_INVALID);
        errorMap.put(Pattern.compile("date/datetime literal is invalid"),
                ErrorCode.ERR_INVALID_DATE_LITERAL);
        errorMap.put(Pattern.compile("Invalid regex expression"),
                ErrorCode.ERR_INVALID_REGEX_EXPRESSION);
        errorMap.put(Pattern.compile("Can not build PartitionsTableValuedFunction by partition_values"),
                ErrorCode.ERR_BUILD_TVF_BY_PARTITION_VALUES_FAILED);
        errorMap.put(Pattern.compile("date_trunc function second param only support"),
                ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_UNSUPPORTED);
        errorMap.put(Pattern.compile("the second parameter of date_trunc function must be a string constant"),
                ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_MUST_STRING_CONST);
        errorMap.put(Pattern.compile("can not cast from origin type TEXT to target type=UNSUPPORTED"),
                ErrorCode.ERR_CAST_TEXT_TO_UNSUPPORTED);
        errorMap.put(Pattern.compile("Doris process failed: Connection reset by peer"),
                ErrorCode.ERR_CONN_RESET_BY_PEER);
        errorMap.put(Pattern.compile("Catalog .* does not exist"),
                ErrorCode.ERR_CATALOG_NOT_EXIST_CN);
        errorMap.put(Pattern.compile("Decode url failed"),
                ErrorCode.ERR_DECODE_URL_FAILED);
        errorMap.put(Pattern.compile("Couldn't deserialize thrift msg"),
                ErrorCode.ERR_THRIFT_DESERIALIZE_FAILED);
        errorMap.put(Pattern.compile("column_type not match"),
                ErrorCode.ERR_COLUMN_TYPE_NOT_MATCH);
        errorMap.put(Pattern.compile("GROUP BY expression must not contain aggregate functions"),
                ErrorCode.ERR_GROUP_BY_NOT_CONTAIN_AGG);
        errorMap.put(Pattern.compile("Required field 'queryId' was not present"),
                ErrorCode.ERR_REQUIRED_QUERY_ID_NOT_PRESENT);
        errorMap.put(Pattern.compile("maximum_expression_tree"),
                ErrorCode.ERR_MAX_EXPRESSION_TREE);
        errorMap.put(Pattern.compile("function: substring_index|substring_index"),
                ErrorCode.ERR_SUBSTRING_INDEX);
        errorMap.put(Pattern.compile("query queue timeout"),
                ErrorCode.ERR_QUERY_QUEUE_TIMEOUT);
        errorMap.put(Pattern.compile("no table auth"),
                ErrorCode.ERR_NO_TABLE_AUTH);
        errorMap.put(Pattern.compile("can not cast from origin type ARRAY"),
                ErrorCode.ERR_CAST_ARRAY_FAILED);
        errorMap.put(Pattern.compile("Json path error: Invalid Json Path for value"),
                ErrorCode.ERR_INVALID_JSON_PATH_VALUE);
        errorMap.put(Pattern.compile("Doris process failed: Broken pipe"), ErrorCode.ERR_BROKEN_PIPE);
        errorMap.put(Pattern.compile("Unknown error 255"),
                ErrorCode.ERR_UNKNOWN_ERROR_255);
        errorMap.put(Pattern.compile("analysis err"),
                ErrorCode.ERR_ANALYSIS_ERR_CN);
        errorMap.put(Pattern.compile("Lzo decompression failed: MalformedInputException"),
                ErrorCode.ERR_LZO_DECOMPRESS_FAILED);
        errorMap.put(Pattern.compile("lowestCostPlans with physicalProperties"),
                ErrorCode.ERR_LOWEST_COST_PLANS);
        errorMap.put(Pattern.compile("Invalid call to"),
                ErrorCode.ERR_INVALID_CALL);
        errorMap.put(Pattern.compile("GC wg for hard limit"),
                ErrorCode.ERR_GC_WG_HARD_LIMIT);
        errorMap.put(Pattern.compile("Expected EQ 1 to be returned by expression"),
                ErrorCode.ERR_EXPECT_EQ1_BY_EXPR);
        errorMap.put(Pattern.compile("table not found"),
                ErrorCode.ERR_TABLE_DOES_NOT_EXIST_IN_DATABASE);
        errorMap.put(Pattern.compile("aggregate function cannot contain aggregate parameters"),
                ErrorCode.ERR_AGG_FUNC_CONTAIN_AGG_PARAMS);
        errorMap.put(Pattern.compile("Operands have unequal number of columns"),
                ErrorCode.ERR_OPERANDS_UNEQUAL_COLUMNS);
        errorMap.put(Pattern.compile("unsupport chubaofs"),
                ErrorCode.ERR_UNSUPPORT_CHUBAOFs);
        errorMap.put(Pattern.compile("exceed max bytes for single hive/hudi table"),
                ErrorCode.ERR_EXCEED_MAX_BYTES_SINGLE_HUDI);
        errorMap.put(Pattern.compile(" return type is Nullable"),
                ErrorCode.ERR_RETURN_TYPE_NULLABLE);
        errorMap.put(Pattern.compile("COUNT DISTINCT could not process type"),
                ErrorCode.ERR_COUNT_DISTINCT_TYPE_UNSUPPORTED);
        errorMap.put(Pattern.compile("cannot be cast to class org.apache.doris.nereids.types.StructType"),
                ErrorCode.ERR_CANNOT_CAST_TO_STRUCTTYPE);
        errorMap.put(Pattern.compile("LOGICAL_PROJECT can not contains TableGeneratingFunction expression"),
                ErrorCode.ERR_LOGICAL_PROJECT_TGF);
        errorMap.put(Pattern.compile("LEFT OUTER JOIN requires an ON or USING clause"),
                ErrorCode.ERR_LEFT_OUTER_JOIN_NEED_ON);
        errorMap.put(Pattern.compile("No such file or directory"),
                ErrorCode.ERR_NO_SUCH_FILE_OR_DIR);
        errorMap.put(Pattern.compile("WindowFrame clause requires OrderBy clause"),
                ErrorCode.ERR_WINDOWFRAME_NEED_ORDERBY);
        errorMap.put(Pattern.compile("analytic function is not allowed in LOGICAL_PROJECT"),
                ErrorCode.ERR_LOGICAL_PROJECT_NO_ANALYTIC);
        errorMap.put(Pattern.compile("Exceeded the maximum number of child expressions"),
                ErrorCode.ERR_EXCEEDED_MAX_CHILD_EXPR);
        errorMap.put(Pattern.compile("Unknown lambda slot"),
                ErrorCode.ERR_UNKNOWN_LAMBDA_SLOT);
        errorMap.put(Pattern.compile("doesn't support order by expression"),
                ErrorCode.ERR_UNSUPPORTED_ORDER_BY_EXPR);
        errorMap.put(Pattern.compile("No matching function with signature"),
                ErrorCode.ERR_NO_MATCHING_FUNCTION_SIGNATURE);
        errorMap.put(Pattern.compile("string cannot be cast to double"),
                ErrorCode.ERR_STRING_CANNOT_CAST_TO_DOUBLE);
        errorMap.put(Pattern.compile("IllegalArgumentException: Expecting scale"),
                ErrorCode.ERR_EXPECTING_SCALE);
        errorMap.put(Pattern.compile("has no permission on column"),
                ErrorCode.ERR_NO_COLUMN_PERMISSION);
        errorMap.put(Pattern.compile("type UNSUPPORTED is unsupported for Nereids"),
                ErrorCode.ERR_TYPE_UNSUPPORTED_FOR_NEREIDS);
        errorMap.put(Pattern.compile("The requested URL returned error: 404"),
                ErrorCode.ERR_REQUESTED_URL_404);
        errorMap.put(Pattern.compile(" Failed to open input stream for file ,such as avro..."),
                ErrorCode.ERR_FAILED_OPEN_INPUT_STREAM);
        errorMap.put(Pattern.compile("LOGICAL_SORT can not contains"),
                ErrorCode.ERR_LOGICAL_SORT_CANNOT_CONTAINS);
        errorMap.put(Pattern.compile("engine:Coordinator dead"),
                ErrorCode.ERR_COORDINATOR_DEAD);
        errorMap.put(Pattern.compile("engine:The select item in correlated subquery"),
                ErrorCode.ERR_SELECT_ITEM_IN_CORRELATED_SUBQUERY);
        errorMap.put(Pattern.compile("Arithmetic overflow when converting value"),
                ErrorCode.ERR_ARITHMETIC_OVERFLOW);
        errorMap.put(Pattern.compile("engine:Output slots of table function node is empty"),
                ErrorCode.ERR_TABLE_FUNCTION_OUTPUT_EMPTY);
        errorMap.put(Pattern.compile("No value present"),
                ErrorCode.ERR_NO_VALUE_PRESENT);
        errorMap.put(Pattern.compile("string column length is too large"),
                ErrorCode.ERR_STRING_COLUMN_LENGTH_TOO_LARGE);
        errorMap.put(Pattern.compile("LOGICAL_FILTER can not contains AggregateFunction expression"),
                ErrorCode.ERR_LOGICAL_FILTER_CONTAIN_AGG_FUNC);
        errorMap.put(Pattern.compile("can't support multi distinct"),
                ErrorCode.ERR_CANT_SUPPORT_MULTI_DISTINCT);
        errorMap.put(Pattern.compile("DISTINCT not allowed in analytic function"),
                ErrorCode.ERR_DISTINCT_NOT_ALLOWED_IN_ANALYTIC);
        errorMap.put(Pattern.compile("Function eq get failed"),
                ErrorCode.ERR_FUNCTION_EQ_GET_FAILED);
        errorMap.put(Pattern.compile("Failed to fetch internal SQL result"),
                ErrorCode.ERR_FAILED_FETCH_INTERNAL_SQL_RESULT);
        errorMap.put(Pattern.compile("Region data is not ready"),
                ErrorCode.ERR_REGION_DATA_NOT_READY);
        errorMap.put(Pattern.compile("grouping and/or aggregation LogicalProject"),
                ErrorCode.ERR_GROUPING_OR_AGG_LOGICAL_PROJECT);
        errorMap.put(Pattern.compile("unknown qualifier"),
                ErrorCode.ERR_UNKNOWN_QUALIFIER);
        errorMap.put(Pattern.compile("count or sum distinct have multi columns"),
                ErrorCode.ERR_COUNT_OR_SUM_DISTINCT_MULTI_COLUMNS);
        errorMap.put(Pattern.compile("scalar subquery's correlatedPredicates's operator must be EQ"),
                ErrorCode.ERR_SCALAR_SUBQUERY_PREDICATE_MUST_EQ);
        errorMap.put(Pattern.compile("Failed to get next row of data. "
                + "CAUSED BY: ArrayIndexOutOfBoundsException"),
                ErrorCode.ERR_GET_NEXT_ROW_ARRAY_INDEX_OOBE);
        errorMap.put(Pattern.compile("Multiple columns returned by subquery are not yet supported"),
                ErrorCode.ERR_SUBQUERY_MULTI_COLUMNS_NOT_SUPPORTED);
        errorMap.put(Pattern.compile("remote table.* storage input format is null"),
                ErrorCode.ERR_REMOTE_TABLE_STORAGE_INPUT_FORMAT_NULL);
        errorMap.put(Pattern.compile("query waiting queue is full"),
                ErrorCode.ERR_QUERY_WAIT_QUEUE_FULL);
        errorMap.put(Pattern.compile("The depth of the expression tree is too big, make it less than"),
                ErrorCode.ERR_EXPR_TREE_TOO_DEEP);
        errorMap.put(Pattern.compile("out of bounds for length"),
                ErrorCode.ERR_OUT_OF_BOUNDS_FOR_LENGTH);
        errorMap.put(Pattern.compile("LOGICAL_HAVING can not contains WindowExpression expression"),
                ErrorCode.ERR_LOGICAL_HAVING_CONTAIN_WINDOW_EXPR);
        errorMap.put(Pattern.compile("comparison predicate could not contains complex type"),
                ErrorCode.ERR_COMPARISON_PREDICATE_NOT_CONTAIN_COMPLEX_TYPE);
        errorMap.put(Pattern.compile("Cannot convert infinity or NaN to decimal"),
                ErrorCode.ERR_CANNOT_CONVERT_INF_OR_NAN_TO_DECIMAL);
        errorMap.put(Pattern.compile("Agg Function .* is not implemented"),
                ErrorCode.ERR_AGG_FUNCTION_NOT_IMPLEMENTED);
        errorMap.put(Pattern.compile("the second parameter of date_trunc function must be a string constant: "
                + "date_trunc"),
                ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_MUST_STRING_CONST);
        errorMap.put(Pattern.compile("failed to get next in hadoop hudi jni scanner"),
                ErrorCode.ERR_HUDI_JNI_SCANNER_GET_NEXT_FAILED);
        errorMap.put(Pattern.compile("CastExpr cannot be cast to class org.apache.doris.analysis.LiteralExp"),
                ErrorCode.ERR_CASTEXPR_CANNOT_CAST_TO_LITERAL);
        errorMap.put(Pattern.compile("empty error"),
                ErrorCode.ERR_EMPTY_ERROR);
        errorMap.put(Pattern.compile("Primary key is not set in hudi table"),
                ErrorCode.ERR_HUDI_PRIMARY_KEY_NOT_SET);
        errorMap.put(Pattern.compile("failed to convert hive partition"),
                ErrorCode.ERR_CONVERT_HIVE_PARTITION_FAILED);
        errorMap.put(Pattern.compile("requires a numeric parameter"),
                ErrorCode.ERR_REQUIRES_NUMERIC_PARAMETER);
        errorMap.put(Pattern.compile("Input slot\\(s\\) not in child output"),
                ErrorCode.ERR_INPUT_SLOTS_NOT_IN_CHILD_OUTPUT);
        errorMap.put(Pattern.compile("Wrong parquet level format"),
                ErrorCode.ERR_WRONG_PARQUET_LEVEL_FORMAT);
        errorMap.put(Pattern.compile("OutOfMemoryError: Java heap space"),
                ErrorCode.ERR_OOM_JAVA_HEAP_SPACE);
        errorMap.put(Pattern.compile("can only be used in conjunction with COUNT"),
                ErrorCode.ERR_CAN_ONLY_USED_WITH_COUNT);
        errorMap.put(Pattern.compile("Cross Region Read NonCR Blocks Failed"),
                ErrorCode.ERR_CROSS_REGION_READ_NONCR_BLOCKS_FAILED);
        errorMap.put(Pattern.compile("RemoteException: File does not exist"),
                ErrorCode.ERR_REMOTE_FILE_NOT_EXIST);
        errorMap.put(Pattern.compile("BlockMissingException: Could not obtain block"),
                ErrorCode.ERR_BLOCK_MISSING_COULD_NOT_OBTAIN);
        errorMap.put(Pattern.compile("IOException: .* missing blocks"),
                ErrorCode.ERR_IO_MISSING_BLOCKS);
        errorMap.put(Pattern.compile("IOException: Blocklist for .* has changed"),
                ErrorCode.ERR_BLOCKLIST_CHANGED);
    }

    public static ErrorCode getErrorCode(String errorMessage) {
        if (errorMessage == null) {
            return ErrorCode.ERR_UNKNOWN_ERROR;
        }
        // Prefer 9119 for "exceed max bytes for single hive/hudi table" so that both hive and hudi
        // cases (including when wrapped as "get file split failed for table, err: ...") return 9119.
        if (errorMessage.contains("exceed max bytes for single hive/hudi table")) {
            return ErrorCode.ERR_EXCEED_MAX_BYTES_SINGLE_HUDI;
        }
        if (errorMessage.contains("date_trunc function second param only support")) {
            return ErrorCode.ERR_DATE_TRUNC_SECOND_PARAM_UNSUPPORTED;
        }
        if (errorMessage.contains("MEM_LIMIT_EXCEEDED")) {
            return ErrorCode.ERR_PRECATCH_MEM_LIMIT_EXCEEDED;
        }
        // Converge query timeout wording to a single timeout bucket.
        if (errorMessage.contains("Query timeout")
                || errorMessage.contains("query timeout")) {
            return ErrorCode.ERR_QUERY_TIMEOUT;
        }
        if (errorMessage.contains("get file split failed for table")
                && errorMessage.contains("MetaException")) {
            String lowerMessage = errorMessage.toLowerCase(Locale.ROOT);
            if (lowerMessage.contains("permission denied")
                    || lowerMessage.contains("access denied")
                    || lowerMessage.contains("has no privilege")) {
                return ErrorCode.ERR_TABLEACCESS_DENIED_ERROR;
            }
        }
        if (errorMessage.contains("MetaException")) {
            String lowerMessage = errorMessage.toLowerCase(Locale.ROOT);
            if (lowerMessage.contains("permission denied")
                    || lowerMessage.contains("access denied")
                    || lowerMessage.contains("has no privilege")) {
                return ErrorCode.ERR_TABLEACCESS_DENIED_ERROR;
            }
        }
        if (errorMessage.contains("File does not exist")) {
            return ErrorCode.ERR_REMOTE_FILE_NOT_EXIST;
        }
        // Memory allocator failures should be grouped as memory errors even when wrapped by
        // reader/orc level messages.
        if (errorMessage.contains("Allocator sys memory check failed")) {
            return ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED;
        }
        // Keep ORC nextBatch failures in a single bucket even when message also contains
        // "Read hdfs file failed" / "IOException ... missing blocks".
        if (errorMessage.contains("Orc row reader nextBatch failed")) {
            return ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED;
        }
        // Converge HDFS read failures into one bucket.
        if (errorMessage.contains("Read hdfs file failed")
                || errorMessage.contains("BlockMissingException: Could not obtain block")
                || errorMessage.contains("IOException: Blocklist for ")
                || errorMessage.matches(".*IOException: .* missing blocks.*")) {
            return ErrorCode.ERR_READ_HDFS_FILE_FAILED_CN;
        }
        if (errorMessage.contains("No namenodes available under nameservice")) {
            return ErrorCode.ERR_FAILED_TO_INIT_READER;
        }
        if (errorMessage.contains("engine: Backend")
                || errorMessage.contains("engine:Backend")
                || errorMessage.contains("Backend Backend")) {
            if (BACKEND_UNAVAILABLE_KEYWORDS.matcher(errorMessage).find()) {
                return ErrorCode.ERR_BACKEND_OFFLINE;
            }
            return ErrorCode.ERR_BACKEND_ERROR;
        }
        for (Map.Entry<Pattern, ErrorCode> entry : errorMap.entrySet()) {
            if (entry.getKey().matcher(errorMessage).find()) {
                return entry.getValue();
            }
        }
        return ErrorCode.ERR_UNKNOWN_ERROR;
    }
}
