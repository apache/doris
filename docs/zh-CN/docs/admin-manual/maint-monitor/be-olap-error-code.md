---
{
    "title": "BE端OLAP函数的返回值说明",
    "language": "zh-CN"
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

# BE端OLAP函数的返回值说明

| 返回值名称                                       | 返回值 | 返回值说明                                                   |
| ------------------------------------------------ | ------ | ------------------------------------------------------------ |
| OLAP_SUCCESS                                     | 0      | 成功                                                         |
| OLAP_ERR_OTHER_ERROR                             | -1     | 其他错误                                                     |
| OLAP_REQUEST_FAILED                              | -2     | 请求失败                                                     |
| 系统错误代码，例如文件系统内存和其他系统调用失败 |        |                                                              |
| OLAP_ERR_OS_ERROR                                | -100   | 操作系统错误                                                 |
| OLAP_ERR_DIR_NOT_EXIST                           | -101   | 目录不存在错误                                               |
| OLAP_ERR_FILE_NOT_EXIST                          | -102   | 文件不存在错误                                               |
| OLAP_ERR_CREATE_FILE_ERROR                       | -103   | 创建文件错误                                                 |
| OLAP_ERR_MALLOC_ERROR                            | -104   | 内存分配错误                                                 |
| OLAP_ERR_STL_ERROR                               | -105   | 标准模板库错误                                               |
| OLAP_ERR_IO_ERROR                                | -106   | IO错误                                                       |
| OLAP_ERR_MUTEX_ERROR                             | -107   | 互斥锁错误                                                   |
| OLAP_ERR_PTHREAD_ERROR                           | -108   | POSIX thread错误                                             |
| OLAP_ERR_NETWORK_ERROR                           | -109   | 网络异常错误                                                 |
| OLAP_ERR_UB_FUNC_ERROR                           | -110   |                                                              |
| OLAP_ERR_COMPRESS_ERROR                          | -111   | 数据压缩错误                                                 |
| OLAP_ERR_DECOMPRESS_ERROR                        | -112   | 数据解压缩错误                                               |
| OLAP_ERR_UNKNOWN_COMPRESSION_TYPE                | -113   | 未知的数据压缩类型                                           |
| OLAP_ERR_MMAP_ERROR                              | -114   | 内存映射文件错误                                             |
| OLAP_ERR_RWLOCK_ERROR                            | -115   | 读写锁错误                                                   |
| OLAP_ERR_READ_UNENOUGH                           | -116   | 读取内存不够异常                                             |
| OLAP_ERR_CANNOT_CREATE_DIR                       | -117   | 不能创建目录异常                                             |
| OLAP_ERR_UB_NETWORK_ERROR                        | -118   | 网络异常                                                     |
| OLAP_ERR_FILE_FORMAT_ERROR                       | -119   | 文件格式异常                                                 |
| OLAP_ERR_EVAL_CONJUNCTS_ERROR                    | -120   |                                                              |
| OLAP_ERR_COPY_FILE_ERROR                         | -121   | 拷贝文件错误                                                 |
| OLAP_ERR_FILE_ALREADY_EXIST                      | -122   | 文件已经存在错误                                             |
| 通用错误代码                                     |        |                                                              |
| OLAP_ERR_NOT_INITED                              | -200   | 不能初始化异常                                               |
| OLAP_ERR_FUNC_NOT_IMPLEMENTED                    | -201   | 函数不能执行异常                                             |
| OLAP_ERR_CALL_SEQUENCE_ERROR                     | -202   | 调用SEQUENCE异常                                             |
| OLAP_ERR_INPUT_PARAMETER_ERROR                   | -203   | 输入参数错误                                                 |
| OLAP_ERR_BUFFER_OVERFLOW                         | -204   | 内存缓冲区溢出错误                                           |
| OLAP_ERR_CONFIG_ERROR                            | -205   | 配置错误                                                     |
| OLAP_ERR_INIT_FAILED                             | -206   | 初始化失败                                                   |
| OLAP_ERR_INVALID_SCHEMA                          | -207   | 无效的Schema                                                 |
| OLAP_ERR_CHECKSUM_ERROR                          | -208   | 检验值错误                                                   |
| OLAP_ERR_SIGNATURE_ERROR                         | -209   | 签名错误                                                     |
| OLAP_ERR_CATCH_EXCEPTION                         | -210   | 捕捉到异常                                                   |
| OLAP_ERR_PARSE_PROTOBUF_ERROR                    | -211   | 解析Protobuf出错                                             |
| OLAP_ERR_SERIALIZE_PROTOBUF_ERROR                | -212   | Protobuf序列化错误                                           |
| OLAP_ERR_WRITE_PROTOBUF_ERROR                    | -213   | Protobuf写错误                                               |
| OLAP_ERR_VERSION_NOT_EXIST                       | -214   | tablet版本不存在错误                                         |
| OLAP_ERR_TABLE_NOT_FOUND                         | -215   | 未找到tablet错误                                             |
| OLAP_ERR_TRY_LOCK_FAILED                         | -216   | 尝试锁失败                                                   |
| OLAP_ERR_OUT_OF_BOUND                            | -218   | 内存越界                                                     |
| OLAP_ERR_UNDERFLOW                               | -219   | underflow错误                                                |
| OLAP_ERR_FILE_DATA_ERROR                         | -220   | 文件数据错误                                                 |
| OLAP_ERR_TEST_FILE_ERROR                         | -221   | 测试文件错误                                                 |
| OLAP_ERR_INVALID_ROOT_PATH                       | -222   | 无效的根目录                                                 |
| OLAP_ERR_NO_AVAILABLE_ROOT_PATH                  | -223   | 没有有效的根目录                                             |
| OLAP_ERR_CHECK_LINES_ERROR                       | -224   | 检查行数错误                                                 |
| OLAP_ERR_INVALID_CLUSTER_INFO                    | -225   | 无效的Cluster信息                                            |
| OLAP_ERR_TRANSACTION_NOT_EXIST                   | -226   | 事务不存在                                                   |
| OLAP_ERR_DISK_FAILURE                            | -227   | 磁盘错误                                                     |
| OLAP_ERR_TRANSACTION_ALREADY_COMMITTED           | -228   | 交易已提交                                                   |
| OLAP_ERR_TRANSACTION_ALREADY_VISIBLE             | -229   | 事务可见                                                     |
| OLAP_ERR_VERSION_ALREADY_MERGED                  | -230   | 版本已合并                                                   |
| OLAP_ERR_LZO_DISABLED                            | -231   | LZO已禁用                                                    |
| OLAP_ERR_DISK_REACH_CAPACITY_LIMIT               | -232   | 磁盘到达容量限制                                             |
| OLAP_ERR_TOO_MANY_TRANSACTIONS                   | -233   | 太多事务积压未完成                                           |
| OLAP_ERR_INVALID_SNAPSHOT_VERSION                | -234   | 无效的快照版本                                               |
| OLAP_ERR_TOO_MANY_VERSION                        | -235   | tablet的数据版本超过了最大限制（默认500）                    |
| OLAP_ERR_NOT_INITIALIZED                         | -236   | 不能初始化                                                   |
| OLAP_ERR_ALREADY_CANCELLED                       | -237   | 已经被取消                                                   |
| OLAP_ERR_TOO_MANY_SEGMENTS                       | -238   | 通常出现在同一批导入数据量过大的情况，从而导致某一个 tablet 的 Segment 文件过多 |
| 命令执行异常代码                                 |        |                                                              |
| OLAP_ERR_CE_CMD_PARAMS_ERROR                     | -300   | 命令参数错误                                                 |
| OLAP_ERR_CE_BUFFER_TOO_SMALL                     | -301   | 缓冲区太多小文件                                             |
| OLAP_ERR_CE_CMD_NOT_VALID                        | -302   | 无效的命令                                                   |
| OLAP_ERR_CE_LOAD_TABLE_ERROR                     | -303   | 加载数据表错误                                               |
| OLAP_ERR_CE_NOT_FINISHED                         | -304   | 命令没有执行成功                                             |
| OLAP_ERR_CE_TABLET_ID_EXIST                      | -305   | tablet Id不存在错误                                          |
| OLAP_ERR_CE_TRY_CE_LOCK_ERROR                    | -306   | 尝试获取执行命令锁错误                                       |
| Tablet错误异常代码                               |        |                                                              |
| OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR           | -400   | tablet 副本版本错误                                          |
| OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR      | -401   | tablet 版本索引不匹配异常                                     |
| OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR              | -402   | 这里不检查tablet的初始版本，因为如果在一个tablet进行schema-change时重新启动 BE，我们可能会遇到空tablet异常 |
| OLAP_ERR_TABLE_INDEX_FIND_ERROR                  | -403   | 无法获得第一个Block块位置 或者找到最后一行Block块失败会引发此异常 |
| OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR          | -404   | 无法加载Tablet的时候会触发此异常                             |
| OLAP_ERR_TABLE_CREATE_META_ERROR                 | -405   | 无法创建Tablet（更改schema），Base tablet不存在 ，会触发此异常 |
| OLAP_ERR_TABLE_ALREADY_DELETED_ERROR             | -406   | tablet已经被删除                                             |
| 存储引擎错误代码                                 |        |                                                              |
| OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE              | -500   | 添加相同的tablet两次，添加tablet到相同数据目录两次，新tablet为空，旧tablet存在。会触发此异常 |
| OLAP_ERR_ENGINE_DROP_NOEXISTS_TABLE              | -501   | 删除不存在的表                                               |
| OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR           | -502   | 加载tablet_meta失败，cumulative rowset无效的segment group meta，会引发此异常 |
| OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR          | -503   | 表插入重复                                                   |
| OLAP_ERR_DELETE_VERSION_ERROR                    | -504   | 删除版本错误                                                 |
| OLAP_ERR_GC_SCAN_PATH_ERROR                      | -505   | GC扫描路径错误                                               |
| OLAP_ERR_ENGINE_INSERT_OLD_TABLET                | -506   | 当 BE 正在重新启动并且较旧的tablet已添加到垃圾收集队列但尚未删除时,在这种情况下,由于 data_dirs 是并行加载的，稍后加载的tablet可能比以前加载的tablet旧，这不应被确认为失败,所以此时返回改代码 |
| Fetch Handler错误代码                            |        |                                                              |
| OLAP_ERR_FETCH_OTHER_ERROR                       | -600   | FetchHandler其他错误                                         |
| OLAP_ERR_FETCH_TABLE_NOT_EXIST                   | -601   | FetchHandler表不存在                                         |
| OLAP_ERR_FETCH_VERSION_ERROR                     | -602   | FetchHandler版本错误                                         |
| OLAP_ERR_FETCH_SCHEMA_ERROR                      | -603   | FetchHandler Schema错误                                      |
| OLAP_ERR_FETCH_COMPRESSION_ERROR                 | -604   | FetchHandler压缩错误                                         |
| OLAP_ERR_FETCH_CONTEXT_NOT_EXIST                 | -605   | FetchHandler上下文不存在                                     |
| OLAP_ERR_FETCH_GET_READER_PARAMS_ERR             | -606   | FetchHandler GET读参数错误                                   |
| OLAP_ERR_FETCH_SAVE_SESSION_ERR                  | -607   | FetchHandler保存会话错误                                     |
| OLAP_ERR_FETCH_MEMORY_EXCEEDED                   | -608   | FetchHandler内存超出异常                                     |
| 读异常错误代码                                   |        |                                                              |
| OLAP_ERR_READER_IS_UNINITIALIZED                 | -700   | 读不能初始化                                                 |
| OLAP_ERR_READER_GET_ITERATOR_ERROR               | -701   | 获取读迭代器错误                                             |
| OLAP_ERR_CAPTURE_ROWSET_READER_ERROR             | -702   | 当前Rowset读错误                                             |
| OLAP_ERR_READER_READING_ERROR                    | -703   | 初始化列数据失败，cumulative rowset 的列数据无效 ，会返回该异常代码 |
| OLAP_ERR_READER_INITIALIZE_ERROR                 | -704   | 读初始化失败                                                 |
| BaseCompaction异常代码信息                       |        |                                                              |
| OLAP_ERR_BE_VERSION_NOT_MATCH                    | -800   | BE Compaction 版本不匹配错误                                 |
| OLAP_ERR_BE_REPLACE_VERSIONS_ERROR               | -801   | BE Compaction 替换版本错误                                   |
| OLAP_ERR_BE_MERGE_ERROR                          | -802   | BE Compaction合并错误                                        |
| OLAP_ERR_CAPTURE_ROWSET_ERROR                    | -804   | 找不到Rowset对应的版本                                       |
| OLAP_ERR_BE_SAVE_HEADER_ERROR                    | -805   | BE Compaction保存Header错误                                  |
| OLAP_ERR_BE_INIT_OLAP_DATA                       | -806   | BE Compaction 初始化OLAP数据错误                             |
| OLAP_ERR_BE_TRY_OBTAIN_VERSION_LOCKS             | -807   | BE Compaction 尝试获得版本锁错误                             |
| OLAP_ERR_BE_NO_SUITABLE_VERSION                  | -808   | BE Compaction 没有合适的版本                                 |
| OLAP_ERR_BE_TRY_BE_LOCK_ERROR                    | -809   | 其他base compaction正在运行，尝试获取锁失败                  |
| OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS         | -810   | 无效的Merge版本                                              |
| OLAP_ERR_BE_ERROR_DELETE_ACTION                  | -811   | BE执行删除操作错误                                           |
| OLAP_ERR_BE_SEGMENTS_OVERLAPPING                 | -812   | cumulative point有重叠的Rowset异常                           |
| OLAP_ERR_BE_CLONE_OCCURRED                       | -813   | 将压缩任务提交到线程池后可能会发生克隆任务，并且选择用于压缩的行集可能会发生变化。 在这种情况下，不应执行当前的压缩任务。 返回该代码 |
| PUSH异常代码                                     |        |                                                              |
| OLAP_ERR_PUSH_INIT_ERROR                         | -900   | 无法初始化读取器，无法创建表描述符，无法初始化内存跟踪器，不支持的文件格式类型，无法打开扫描仪，无法获取元组描述符，为元组分配内存失败，都会返回该代码 |
| OLAP_ERR_PUSH_DELTA_FILE_EOF                     | -901   |                                                              |
| OLAP_ERR_PUSH_VERSION_INCORRECT                  | -902   | PUSH版本不正确                                               |
| OLAP_ERR_PUSH_SCHEMA_MISMATCH                    | -903   | PUSH Schema不匹配                                            |
| OLAP_ERR_PUSH_CHECKSUM_ERROR                     | -904   | PUSH校验值错误                                               |
| OLAP_ERR_PUSH_ACQUIRE_DATASOURCE_ERROR           | -905   | PUSH 获取数据源错误                                          |
| OLAP_ERR_PUSH_CREAT_CUMULATIVE_ERROR             | -906   | PUSH 创建CUMULATIVE错误代码                                  |
| OLAP_ERR_PUSH_BUILD_DELTA_ERROR                  | -907   | 推送的增量文件有错误的校验码                                 |
| OLAP_ERR_PUSH_VERSION_ALREADY_EXIST              | -908   | PUSH的版本已经存在                                           |
| OLAP_ERR_PUSH_TABLE_NOT_EXIST                    | -909   | PUSH的表不存在                                               |
| OLAP_ERR_PUSH_INPUT_DATA_ERROR                   | -910   | PUSH的数据无效，可能是长度，数据类型等问题                   |
| OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST          | -911   | 将事务提交给引擎时，发现Rowset存在，但Rowset ID 不一样       |
| OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED              | -912   | 删除了推送批处理过程                                         |
| OLAP_ERR_PUSH_COMMIT_ROWSET                      | -913   | PUSH Commit Rowset                                           |
| OLAP_ERR_PUSH_ROWSET_NOT_FOUND                   | -914   | PUSH Rowset没有发现                                          |
| SegmentGroup异常代码                             |        |                                                              |
| OLAP_ERR_INDEX_LOAD_ERROR                        | -1000  | 加载索引错误                                                 |
| OLAP_ERR_INDEX_EOF                               | -1001  |                                                              |
| OLAP_ERR_INDEX_CHECKSUM_ERROR                    | -1002  | 校验码验证错误，加载索引对应的Segment 错误。                 |
| OLAP_ERR_INDEX_DELTA_PRUNING                     | -1003  | 索引增量修剪                                                 |
| OLAPData异常代码信息                             |        |                                                              |
| OLAP_ERR_DATA_ROW_BLOCK_ERROR                    | -1100  | 数据行Block块错误                                            |
| OLAP_ERR_DATA_FILE_TYPE_ERROR                    | -1101  | 数据文件类型错误                                             |
| OLAP_ERR_DATA_EOF                                | -1102  |                                                              |
| OLAP数据写错误代码                               |        |                                                              |
| OLAP_ERR_WRITER_INDEX_WRITE_ERROR                | -1200  | 索引写错误                                                   |
| OLAP_ERR_WRITER_DATA_WRITE_ERROR                 | -1201  | 数据写错误                                                   |
| OLAP_ERR_WRITER_ROW_BLOCK_ERROR                  | -1202  | Row Block块写错误                                            |
| OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED            | -1203  | 在添加新Segment之前，上一Segment未完成                       |
| RowBlock错误代码                                 |        |                                                              |
| OLAP_ERR_ROWBLOCK_DECOMPRESS_ERROR               | -1300  | Rowblock解压缩错误                                           |
| OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION             | -1301  | 获取Block Entry失败                                          |
| Tablet元数据错误                                 |        |                                                              |
| OLAP_ERR_HEADER_ADD_VERSION                      | -1400  | tablet元数据增加版本                                         |
| OLAP_ERR_HEADER_DELETE_VERSION                   | -1401  | tablet元数据删除版本                                         |
| OLAP_ERR_HEADER_ADD_PENDING_DELTA                | -1402  | tablet元数据添加待处理增量                                   |
| OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION          | -1403  | tablet元数据添加自增版本                                     |
| OLAP_ERR_HEADER_INVALID_FLAG                     | -1404  | tablet元数据无效的标记                                       |
| OLAP_ERR_HEADER_PUT                              | -1405  | tablet元数据PUT操作                                          |
| OLAP_ERR_HEADER_DELETE                           | -1406  | tablet元数据DELETE操作                                       |
| OLAP_ERR_HEADER_GET                              | -1407  | tablet元数据GET操作                                          |
| OLAP_ERR_HEADER_LOAD_INVALID_KEY                 | -1408  | tablet元数据加载无效Key                                      |
| OLAP_ERR_HEADER_FLAG_PUT                         | -1409  |                                                              |
| OLAP_ERR_HEADER_LOAD_JSON_HEADER                 | -1410  | tablet元数据加载JSON Header                                  |
| OLAP_ERR_HEADER_INIT_FAILED                      | -1411  | tablet元数据Header初始化失败                                 |
| OLAP_ERR_HEADER_PB_PARSE_FAILED                  | -1412  | tablet元数据 Protobuf解析失败                                |
| OLAP_ERR_HEADER_HAS_PENDING_DATA                 | -1413  | tablet元数据有待处理的数据                                   |
| TabletSchema异常代码信息                         |        |                                                              |
| OLAP_ERR_SCHEMA_SCHEMA_INVALID                   | -1500  | Tablet Schema无效                                            |
| OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID             | -1501  | Tablet Schema 字段无效                                       |
| SchemaHandler异常代码信息                        |        |                                                              |
| OLAP_ERR_ALTER_MULTI_TABLE_ERR                   | -1600  | ALTER 多表错误                                               |
| OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS             | -1601  | 获取所有数据源失败，Tablet无版本                             |
| OLAP_ERR_ALTER_STATUS_ERR                        | -1602  | 检查行号失败，内部排序失败，行块排序失败，这些都会返回该代码 |
| OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED     | -1603  | 先前的Schema更改未完成                                       |
| OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID              | -1604  | Schema变更信息无效                                           |
| OLAP_ERR_QUERY_SPLIT_KEY_ERR                     | -1605  | 查询 Split key 错误                                          |
| OLAP_ERR_DATA_QUALITY_ERROR                      | -1606  | 模式更改/物化视图期间因数据质量问题或内存使用超出限制导致的错误                  |
| Column File错误代码                              |        |                                                              |
| OLAP_ERR_COLUMN_DATA_LOAD_BLOCK                  | -1700  | 加载列数据块错误                                             |
| OLAP_ERR_COLUMN_DATA_RECORD_INDEX                | -1701  | 加载数据记录索引错误                                         |
| OLAP_ERR_COLUMN_DATA_MAKE_FILE_HEADER            | -1702  |                                                              |
| OLAP_ERR_COLUMN_DATA_READ_VAR_INT                | -1703  | 无法从Stream中读取列数据                                     |
| OLAP_ERR_COLUMN_DATA_PATCH_LIST_NUM              | -1704  |                                                              |
| OLAP_ERR_COLUMN_STREAM_EOF                       | -1705  | 如果数据流结束，返回该代码                                   |
| OLAP_ERR_COLUMN_READ_STREAM                      | -1706  | 块大小大于缓冲区大小，压缩剩余大小小于Stream头大小，读取流失败 这些情况下会抛出该异常 |
| OLAP_ERR_COLUMN_STREAM_NOT_EXIST                 | -1707  | Stream为空，不存在，未找到数据流 等情况下返回该异常代码      |
| OLAP_ERR_COLUMN_VALUE_NULL                       | -1708  | 列值为空异常                                                 |
| OLAP_ERR_COLUMN_SEEK_ERROR                       | -1709  | 如果通过schema变更添加列，由于schema变更可能导致列索引存在，返回这个异常代码 |
| DeleteHandler错误代码                            |        |                                                              |
| OLAP_ERR_DELETE_INVALID_CONDITION                | -1900  | 删除条件无效                                                 |
| OLAP_ERR_DELETE_UPDATE_HEADER_FAILED             | -1901  | 删除更新Header错误                                           |
| OLAP_ERR_DELETE_SAVE_HEADER_FAILED               | -1902  | 删除保存header错误                                           |
| OLAP_ERR_DELETE_INVALID_PARAMETERS               | -1903  | 删除参数无效                                                 |
| OLAP_ERR_DELETE_INVALID_VERSION                  | -1904  | 删除版本无效                                                 |
| Cumulative Handler错误代码                       |        |                                                              |
| OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS         | -2000  | Cumulative没有合适的版本                                     |
| OLAP_ERR_CUMULATIVE_REPEAT_INIT                  | -2001  | Cumulative Repeat 初始化错误                                 |
| OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS           | -2002  | Cumulative参数无效                                           |
| OLAP_ERR_CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE   | -2003  | Cumulative获取数据源失败                                     |
| OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS | -2004  | Cumulative无有效需要合并版本                                 |
| OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION          | -2005  | Cumulative删除操作错误                                       |
| OLAP_ERR_CUMULATIVE_MISS_VERSION                 | -2006  | rowsets缺少版本                                              |
| OLAP_ERR_CUMULATIVE_CLONE_OCCURRED               | -2007  | 将压缩任务提交到线程池后可能会发生克隆任务，并且选择用于压缩的行集可能会发生变化。 在这种情况下，不应执行当前的压缩任务。否则会触发改异常 |
| OLAPMeta异常代码                                 |        |                                                              |
| OLAP_ERR_META_INVALID_ARGUMENT                   | -3000  | 元数据参数无效                                               |
| OLAP_ERR_META_OPEN_DB                            | -3001  | 打开DB元数据错误                                             |
| OLAP_ERR_META_KEY_NOT_FOUND                      | -3002  | 元数据key没发现                                              |
| OLAP_ERR_META_GET                                | -3003  | GET元数据错误                                                |
| OLAP_ERR_META_PUT                                | -3004  | PUT元数据错误                                                |
| OLAP_ERR_META_ITERATOR                           | -3005  | 元数据迭代器错误                                             |
| OLAP_ERR_META_DELETE                             | -3006  | 删除元数据错误                                               |
| OLAP_ERR_META_ALREADY_EXIST                      | -3007  | 元数据已经存在错误                                           |
| Rowset错误代码                                   |        |                                                              |
| OLAP_ERR_ROWSET_WRITER_INIT                      | -3100  | Rowset写初始化错误                                           |
| OLAP_ERR_ROWSET_SAVE_FAILED                      | -3101  | Rowset保存失败                                               |
| OLAP_ERR_ROWSET_GENERATE_ID_FAILED               | -3102  | Rowset生成ID失败                                             |
| OLAP_ERR_ROWSET_DELETE_FILE_FAILED               | -3103  | Rowset删除文件失败                                           |
| OLAP_ERR_ROWSET_BUILDER_INIT                     | -3104  | Rowset初始化构建失败                                         |
| OLAP_ERR_ROWSET_TYPE_NOT_FOUND                   | -3105  | Rowset类型没有发现                                           |
| OLAP_ERR_ROWSET_ALREADY_EXIST                    | -3106  | Rowset已经存在                                               |
| OLAP_ERR_ROWSET_CREATE_READER                    | -3107  | Rowset创建读对象失败                                         |
| OLAP_ERR_ROWSET_INVALID                          | -3108  | Rowset无效                                                   |
| OLAP_ERR_ROWSET_READER_INIT                      | -3110  | Rowset读对象初始化失败                                       |
| OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION         | -3112  | Rowset无效的事务状态                                         |
| OLAP_ERR_ROWSET_RENAME_FILE_FAILED               | -3116  | Rowset重命名文件失败                                         |
| OLAP_ERR_SEGCOMPACTION_INIT_READER               | -3117  | SegmentCompaction初始化Reader失败                            |
| OLAP_ERR_SEGCOMPACTION_INIT_WRITER               | -3118  | SegmentCompaction初始化Writer失败                            |
| OLAP_ERR_SEGCOMPACTION_FAILED                    | -3119  | SegmentCompaction失败                                        |
