---
{
    "title": "Description of the return value of the OLAP function on the BE side",
    "language": "en"
}

---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Description of the return value of the OLAP function on the BE side

| Return value name | Return value | Return value description |
| ------------------------------------------------ | ------ | ------------------------------------------------------------ |
| OLAP_SUCCESS | 0 | Success |
| OLAP_ERR_OTHER_ERROR | -1 | Other errors |
| OLAP_REQUEST_FAILED | -2 | Request failed |
| System error codes, such as file system memory and other system call failures | | |
| OLAP_ERR_OS_ERROR | -100 | Operating system error |
| OLAP_ERR_DIR_NOT_EXIST | -101 | Directory does not exist error |
| OLAP_ERR_FILE_NOT_EXIST | -102 | File does not exist error |
| OLAP_ERR_CREATE_FILE_ERROR | -103 | Error creating file |
| OLAP_ERR_MALLOC_ERROR | -104 | Memory allocation error |
| OLAP_ERR_STL_ERROR | -105 | Standard template library error |
| OLAP_ERR_IO_ERROR | -106 | IO error |
| OLAP_ERR_MUTEX_ERROR | -107 | Mutex error |
| OLAP_ERR_PTHREAD_ERROR | -108 | POSIX thread error |
| OLAP_ERR_NETWORK_ERROR | -109 | Network abnormal error |
| OLAP_ERR_UB_FUNC_ERROR | -110 | |
| OLAP_ERR_COMPRESS_ERROR | -111 | Data compression error |
| OLAP_ERR_DECOMPRESS_ERROR | -112 | Data decompression error |
| OLAP_ERR_UNKNOWN_COMPRESSION_TYPE | -113 | Unknown data compression type |
| OLAP_ERR_MMAP_ERROR | -114 | Memory mapped file error |
| OLAP_ERR_RWLOCK_ERROR | -115 | Read-write lock error |
| OLAP_ERR_READ_UNENOUGH | -116 | Read memory is not enough exception |
| OLAP_ERR_CANNOT_CREATE_DIR | -117 | Cannot create directory exception |
| OLAP_ERR_UB_NETWORK_ERROR | -118 | Network exception |
| OLAP_ERR_FILE_FORMAT_ERROR | -119 | File format abnormal |
| OLAP_ERR_EVAL_CONJUNCTS_ERROR | -120 | |
| OLAP_ERR_COPY_FILE_ERROR | -121 | Copy file error |
| OLAP_ERR_FILE_ALREADY_EXIST | -122 | File already exists error |
| General error codes | | |
| OLAP_ERR_NOT_INITED | -200 | Cannot initialize exception |
| OLAP_ERR_FUNC_NOT_IMPLEMENTED | -201 | Function cannot be executed exception |
| OLAP_ERR_CALL_SEQUENCE_ERROR | -202 | Call SEQUENCE exception |
| OLAP_ERR_INPUT_PARAMETER_ERROR | -203 | Input parameter error |
| OLAP_ERR_BUFFER_OVERFLOW | -204 | Memory buffer overflow error |
| OLAP_ERR_CONFIG_ERROR | -205 | Configuration error |
| OLAP_ERR_INIT_FAILED | -206 | Initialization failed |
| OLAP_ERR_INVALID_SCHEMA | -207 | Invalid Schema |
| OLAP_ERR_CHECKSUM_ERROR | -208 | Check value error |
| OLAP_ERR_SIGNATURE_ERROR | -209 | Signature error |
| OLAP_ERR_CATCH_EXCEPTION | -210 | Exception caught |
| OLAP_ERR_PARSE_PROTOBUF_ERROR | -211 | Error parsing Protobuf|
| OLAP_ERR_INVALID_ROOT_PATH | -222 | Invalid root directory |
| OLAP_ERR_NO_AVAILABLE_ROOT_PATH | -223 | No valid root directory |
| OLAP_ERR_CHECK_LINES_ERROR | -224 | Check the number of lines error |
| OLAP_ERR_INVALID_CLUSTER_INFO | -225 | Invalid Cluster Information |
| OLAP_ERR_TRANSACTION_NOT_EXIST | -226 | Transaction does not exist |
| OLAP_ERR_DISK_FAILURE | -227 | Disk error |
| OLAP_ERR_TRANSACTION_ALREADY_COMMITTED | -228 | Transaction submitted |
| OLAP_ERR_TRANSACTION_ALREADY_VISIBLE | -229 | Transaction visible |
| OLAP_ERR_VERSION_ALREADY_MERGED | -230 | Version has been merged |
| OLAP_ERR_LZO_DISABLED | -231 | LZO is disabled |
| OLAP_ERR_DISK_REACH_CAPACITY_LIMIT | -232 | Disk reached capacity limit |
| OLAP_ERR_TOO_MANY_TRANSACTIONS | -233 | Too many transaction backlogs are not completed |
| OLAP_ERR_INVALID_SNAPSHOT_VERSION | -234 | Invalid snapshot version |
| OLAP_ERR_TOO_MANY_VERSION | -235 | The tablet data version exceeds the maximum limit (default 500) |
| OLAP_ERR_NOT_INITIALIZED | -236 | Cannot initialize |
| OLAP_ERR_ALREADY_CANCELLED | -237 | Has been cancelled |
| OLAP_ERR_TOO_MANY_SEGMENTS | -238 | usually occurs when the amount of imported data in the same batch is too large, resulting in too many segment files for a tablet |
| Command execution exception code | | |
| OLAP_ERR_CE_CMD_PARAMS_ERROR | -300 | Command parameter error |
| OLAP_ERR_CE_BUFFER_TOO_SMALL | -301 | Too many small files in the buffer |
| OLAP_ERR_CE_CMD_NOT_VALID | -302 | Invalid command |
| OLAP_ERR_CE_LOAD_TABLE_ERROR | -303 | Error loading data table |
| OLAP_ERR_CE_NOT_FINISHED | -304 | The command was not executed successfully |
| OLAP_ERR_CE_TABLET_ID_EXIST | -305 | Tablet Id does not exist error |
| OLAP_ERR_CE_TRY_CE_LOCK_ERROR | -306 | Attempt to obtain execution command lock error |
| Tablet error exception code | | |
| OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR | -400 | Tablet copy version error |
| OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR | -401 | tablet version index mismatch exception |
| OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR | -402 | The initial version of the tablet is not checked here, because if the BE is restarted during a schema-change of a tablet, we may encounter an empty tablet exception |
| OLAP_ERR_TABLE_INDEX_FIND_ERROR | -403 | Unable to get the position of the first block or failure to find the last block of the block will cause this exception |
| OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR | -404 | This exception is triggered when the tablet cannot be loaded |
| OLAP_ERR_TABLE_CREATE_META_ERROR | -405 | Unable to create tablet (change schema), base tablet does not exist, this exception will be triggered |
| OLAP_ERR_TABLE_ALREADY_DELETED_ERROR | -406 | The tablet has been deleted |
| Storage Engine Error Code | | |
| OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE | -500 | Add the same tablet twice, add the tablet to the same data directory twice, the new tablet is empty, and the old tablet exists. Will trigger this exception |
| OLAP_ERR_ENGINE_DROP_NOEXISTS_TABLE | -501 | Delete non-existent table |
| OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR | -502 | Failed to load tablet_meta, segment group meta with invalid cumulative rowset, will cause this exception |
| OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR | -503 | Duplicate table insert |
| OLAP_ERR_DELETE_VERSION_ERROR | -504 | Delete version error |
| OLAP_ERR_GC_SCAN_PATH_ERROR | -505 | GC scan path error |
| OLAP_ERR_ENGINE_INSERT_OLD_TABLET | -506 | When BE is restarting and older tablets have been added to the garbage collection queue but have not yet been deleted. In this case, since data_dirs are loaded in parallel, tablets loaded later may be loaded later than before The tablet is old, this should not be confirmed as a failure, so return to change the code at this time |
| Fetch Handler error code | | |
| OLAP_ERR_FETCH_OTHER_ERROR | -600 | FetchHandler other errors |
| OLAP_ERR_FETCH_TABLE_NOT_EXIST | -601 | FetchHandler table does not exist |
| OLAP_ERR_FETCH_VERSION_ERROR | -602 | FetchHandler version error |
| OLAP_ERR_FETCH_SCHEMA_ERROR | -603 | FetchHandler Schema error |
| OLAP_ERR_FETCH_COMPRESSION_ERROR | -604 | FetchHandler compression error |
| OLAP_ERR_FETCH_CONTEXT_NOT_EXIST | -605 | FetchHandler context does not exist |
| OLAP_ERR_FETCH_GET_READER_PARAMS_ERR | -606 | FetchHandler GET read parameter error |
| OLAP_ERR_FETCH_SAVE_SESSION_ERR | -607 | FetchHandler save session error |
| OLAP_ERR_FETCH_MEMORY_EXCEEDED | -608 | FetchHandler memory exceeded exception |
| Read exception error code | | |
| OLAP_ERR_READER_IS_UNINITIALIZED | -700 | Read cannot be initialized |
| OLAP_ERR_READER_GET_ITERATOR_ERROR | -701 | Get read iterator error |
| OLAP_ERR_CAPTURE_ROWSET_READER_ERROR | -702 | Current Rowset read error |
| OLAP_ERR_READER_READING_ERROR | -703 | Failed to initialize column data, the column data of cumulative rowset is invalid, this exception code will be returned |
| OLAP_ERR_READER_INITIALIZE_ERROR | -704 | Read initialization failed |
| BaseCompaction exception code information | | |
| OLAP_ERR_BE_VERSION_NOT_MATCH | -800 | BE Compaction version mismatch error |
| OLAP_ERR_BE_REPLACE_VERSIONS_ERROR | -801 | BE Compaction replacement version error |
| OLAP_ERR_BE_MERGE_ERROR | -802 | BE Compaction merge error |
| OLAP_ERR_CAPTURE_ROWSET_ERROR | -804 | Cannot find the version corresponding to Rowset |
| OLAP_ERR_BE_SAVE_HEADER_ERROR | -805 | BE Compaction save header error |
| OLAP_ERR_BE_INIT_OLAP_DATA | -806 | BE Compaction initialized OLAP data error |
| OLAP_ERR_BE_TRY_OBTAIN_VERSION_LOCKS | -807 | BE Compaction trying to obtain version lock error |
| OLAP_ERR_BE_NO_SUITABLE_VERSION | -808 | BE Compaction does not have a suitable version |
| OLAP_ERR_BE_TRY_BE_LOCK_ERROR | -809 | The other base compaction is running, and the attempt to acquire the lock failed |
| OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS | -810 | Invalid Merge version |
| OLAP_ERR_BE_ERROR_DELETE_ACTION | -811 | BE performing delete operation error |
| OLAP_ERR_BE_SEGMENTS_OVERLAPPING | -812 | Rowset exception with overlapping cumulative points |
| OLAP_ERR_BE_CLONE_OCCURRED | -813 | Cloning tasks may occur after the compression task is submitted to the thread pool, and the set of rows selected for compression may change. In this case, the current compression task should not be performed. Return this code |
| PUSH exception code | | |
| OLAP_ERR_PUSH_INIT_ERROR | -900 | Unable to initialize reader, unable to create table descriptor, unable to initialize memory tracker, unsupported file format type, unable to open scanner, unable to obtain tuple descriptor, failed to allocate memory for tuple, Will return this code |
| OLAP_ERR_PUSH_DELTA_FILE_EOF | -901 | |
| OLAP_ERR_PUSH_VERSION_INCORRECT | -902 | PUSH version is incorrect |
| OLAP_ERR_PUSH_SCHEMA_MISMATCH | -903 | PUSH Schema does not match |
| OLAP_ERR_PUSH_CHECKSUM_ERROR | -904 | PUSH check value error |
| OLAP_ERR_PUSH_ACQUIRE_DATASOURCE_ERROR | -905 | PUSH get data source error |
| OLAP_ERR_PUSH_CREAT_CUMULATIVE_ERROR | -906 | PUSH Create CUMULATIVE error code |
| OLAP_ERR_PUSH_BUILD_DELTA_ERROR | -907 | The pushed incremental file has an incorrect check code |
| OLAP_ERR_PUSH_VERSION_ALREADY_EXIST | -908 | PUSH version already exists |
| OLAP_ERR_PUSH_TABLE_NOT_EXIST | -909 | PUSH table does not exist |
| OLAP_ERR_PUSH_INPUT_DATA_ERROR | -910 | PUSH data is invalid, it may be length, data type and other issues |
| OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST | -911 | When submitting the transaction to the engine, it is found that Rowset exists, but the Rowset ID is different |
| OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED | -912 | Deleted the push batch process |
| OLAP_ERR_PUSH_COMMIT_ROWSET | -913 | PUSH Commit Rowset |
| OLAP_ERR_PUSH_ROWSET_NOT_FOUND | -914 | PUSH Rowset not found |
| SegmentGroup exception code | | |
| OLAP_ERR_INDEX_LOAD_ERROR | -1000 | Load index error |
| OLAP_ERR_INDEX_EOF | -1001 | |
| OLAP_ERR_INDEX_CHECKSUM_ERROR | -1002 | Checksum verification error, segment error loading index. |
| OLAP_ERR_INDEX_DELTA_PRUNING | -1003 | Index incremental pruning |
| OLAPData exception code information | | |
| OLAP_ERR_DATA_ROW_BLOCK_ERROR | -1100 | Data row Block block error |
| OLAP_ERR_DATA_FILE_TYPE_ERROR | -1101 | Data file type error |
| OLAP_ERR_DATA_EOF | -1102 | |
| OLAP data write error code | | |
| OLAP_ERR_WRITER_INDEX_WRITE_ERROR | -1200 | Index write error |
| OLAP_ERR_WRITER_DATA_WRITE_ERROR | -1201 | Data writing error |
| OLAP_ERR_WRITER_ROW_BLOCK_ERROR | -1202 | Row Block block write error |
| OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED | -1203 | Before adding a new segment, the previous segment was not completed |
| RowBlock error code | | |
| OLAP_ERR_ROWBLOCK_DECOMPRESS_ERROR | -1300 | Rowblock decompression error |
| OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION | -1301 | Failed to obtain Block Entry |
| Tablet metadata error | | |
| OLAP_ERR_HEADER_ADD_VERSION | -1400 | Tablet metadata increase version |
| OLAP_ERR_HEADER_DELETE_VERSION | -1401 | Tablet metadata deletion version |
| OLAP_ERR_HEADER_ADD_PENDING_DELTA | -1402 | Tablet metadata add pending increment |
| OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION | -1403 | Tablet metadata addition self-increment version |
| OLAP_ERR_HEADER_INVALID_FLAG | -1404 | Invalid tablet metadata flag |
| OLAP_ERR_HEADER_PUT | -1405 | tablet metadata PUT operation |
| OLAP_ERR_HEADER_DELETE | -1406 | tablet metadata DELETE operation |
| OLAP_ERR_HEADER_GET | -1407 | tablet metadata GET operation |
| OLAP_ERR_HEADER_LOAD_INVALID_KEY | -1408 | Tablet metadata loading invalid Key |
| OLAP_ERR_HEADER_FLAG_PUT | -1409 | |
| OLAP_ERR_HEADER_LOAD_JSON_HEADER | -1410 | tablet metadata loading JSON Header |
| OLAP_ERR_HEADER_INIT_FAILED | -1411 | Tablet metadata header initialization failed |
| OLAP_ERR_HEADER_PB_PARSE_FAILED | -1412 | Tablet metadata Protobuf parsing failed |
| OLAP_ERR_HEADER_HAS_PENDING_DATA | -1413 | Tablet metadata pending data |
| TabletSchema exception code information | | |
| OLAP_ERR_SCHEMA_SCHEMA_INVALID | -1500 | Invalid Tablet Schema |
| OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID | -1501 | Tablet Schema field is invalid |
| SchemaHandler exception code information | | |
| OLAP_ERR_ALTER_MULTI_TABLE_ERR | -1600 | ALTER multi-table error |
| OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS | -1601 | Failed to get all data sources, Tablet has no version |
| OLAP_ERR_ALTER_STATUS_ERR | -1602 | Failed to check the row number, internal sorting failed, row block sorting failed, these will return this code |
| OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED | -1603 | The previous schema change is not completed |
| OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID | -1604 | Schema change information is invalid |
| OLAP_ERR_QUERY_SPLIT_KEY_ERR | -1605 | Query Split key error |
| OLAP_ERR_DATA_QUALITY_ERROR | -1606 | Errors caused by data quality issues or reach memory limit during schema changes/materialized views |
| Column File error code | | |
| OLAP_ERR_COLUMN_DATA_LOAD_BLOCK | -1700 | Error loading column data block |
| OLAP_ERR_COLUMN_DATA_RECORD_INDEX | -1701 | Load data record index error |
| OLAP_ERR_COLUMN_DATA_MAKE_FILE_HEADER | -1702 | |
| OLAP_ERR_COLUMN_DATA_READ_VAR_INT | -1703 | Cannot read column data from Stream |
| OLAP_ERR_COLUMN_DATA_PATCH_LIST_NUM | -1704 | |
| OLAP_ERR_COLUMN_STREAM_EOF | -1705 | If the data stream ends, return this code |
| OLAP_ERR_COLUMN_READ_STREAM | -1706 | The block size is greater than the buffer size, the remaining compressed size is less than the Stream header size, and the read stream fails. This exception will be thrown in these cases |
| OLAP_ERR_COLUMN_STREAM_NOT_EXIST | -1707 | Stream is empty, does not exist, the data stream is not found, etc. The exception code is returned |
| OLAP_ERR_COLUMN_VALUE_NULL | -1708 | Column value is empty exception |
| OLAP_ERR_COLUMN_SEEK_ERROR | -1709 | If you add a column through a schema change, the column index may exist due to the schema change, and this exception code is returned |
| DeleteHandler error code | | |
| OLAP_ERR_DELETE_INVALID_CONDITION | -1900 | Invalid delete condition |
| OLAP_ERR_DELETE_UPDATE_HEADER_FAILED | -1901 | Delete update Header error |
| OLAP_ERR_DELETE_SAVE_HEADER_FAILED | -1902 | Delete save header error |
| OLAP_ERR_DELETE_INVALID_PARAMETERS | -1903 | Invalid delete parameter |
| OLAP_ERR_DELETE_INVALID_VERSION | -1904 | Invalid delete version |
| Cumulative Handler error code | | |
| OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS | -2000 | Cumulative does not have a suitable version |
| OLAP_ERR_CUMULATIVE_REPEAT_INIT | -2001 | Cumulative Repeat initialization error |
| OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS | -2002 | Invalid Cumulative parameter |
| OLAP_ERR_CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE | -2003 | Cumulative failed to obtain data source |
| OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS | -2004 | Cumulative does not have a valid version that needs to be merged |
| OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION | -2005 | Cumulative delete operation error |
| OLAP_ERR_CUMULATIVE_MISS_VERSION | -2006 | rowsets missing version |
| OLAP_ERR_CUMULATIVE_CLONE_OCCURRED | -2007 | Cloning tasks may occur after the compression task is submitted to the thread pool, and the set of rows selected for compression may change. In this case, the current compression task should not be performed. Otherwise it will trigger a change exception |
| OLAPMeta exception code | | |
| OLAP_ERR_META_INVALID_ARGUMENT | -3000 | Invalid metadata parameter |
| OLAP_ERR_META_OPEN_DB | -3001 | Open DB metadata error |
| OLAP_ERR_META_KEY_NOT_FOUND | -3002 | Metadata key not found |
| OLAP_ERR_META_GET | -3003 | GET metadata error |
| OLAP_ERR_META_PUT | -3004 | PUT metadata error |
| OLAP_ERR_META_ITERATOR | -3005 | Metadata iterator error |
| OLAP_ERR_META_DELETE | -3006 | Delete metadata error |
| OLAP_ERR_META_ALREADY_EXIST | -3007 | Metadata already has an error |
| Rowset error code | | |
| OLAP_ERR_ROWSET_WRITER_INIT | -3100 | Rowset write initialization error |
| OLAP_ERR_ROWSET_SAVE_FAILED | -3101 | Rowset save failed |
| OLAP_ERR_ROWSET_GENERATE_ID_FAILED | -3102 | Rowset failed to generate ID |
| OLAP_ERR_ROWSET_DELETE_FILE_FAILED | -3103 | Rowset failed to delete file |
| OLAP_ERR_ROWSET_BUILDER_INIT | -3104 | Rowset initialization failed to build |
| OLAP_ERR_ROWSET_TYPE_NOT_FOUND | -3105 | Rowset type not found |
| OLAP_ERR_ROWSET_ALREADY_EXIST | -3106 | Rowset already exists |
| OLAP_ERR_ROWSET_CREATE_READER | -3107 | Rowset failed to create read object |
| OLAP_ERR_ROWSET_INVALID | -3108 | Rowset is invalid |
| OLAP_ERR_ROWSET_READER_INIT | -3110 | Rowset read object initialization failed |
| OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION | -3112 | Rowset invalid transaction state |
| OLAP_ERR_ROWSET_RENAME_FILE_FAILED | -3116 | Rowset failed to rename file |
| OLAP_ERR_SEGCOMPACTION_INIT_READER | -3117 | Segment Compaction failed to init reader |
| OLAP_ERR_SEGCOMPACTION_INIT_WRITER | -3118 | Segment Compaction failed to init writer |
| OLAP_ERR_SEGCOMPACTION_FAILED | -3119 | Segment Compaction failed |



