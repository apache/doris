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

#pragma once

namespace doris::profile {

// ============================================================
// Profile node names
// ============================================================

// Sub-profile names used by all operators
inline constexpr char COMMON_COUNTERS[] = "CommonCounters";
inline constexpr char CUSTOM_COUNTERS[] = "CustomCounters";
inline constexpr char SCANNER[] = "Scanner";
inline constexpr char FAKER_PROFILE[] = "faker profile";

// ============================================================
// Source operator common counters (PipelineXLocalState::init)
// ============================================================
inline constexpr char ROWS_PRODUCED[] = "RowsProduced";
inline constexpr char BLOCKS_PRODUCED[] = "BlocksProduced";
inline constexpr char OUTPUT_BLOCK_BYTES[] = "OutputBlockBytes";
inline constexpr char MAX_OUTPUT_BLOCK_BYTES[] = "MaxOutputBlockBytes";
inline constexpr char MIN_OUTPUT_BLOCK_BYTES[] = "MinOutputBlockBytes";

// ============================================================
// Sink operator common counters (PipelineXSinkLocalState::init)
// ============================================================
inline constexpr char INPUT_ROWS[] = "InputRows";
inline constexpr char PENDING_FINISH_DEPENDENCY[] = "PendingFinishDependency";

// ============================================================
// Shared operator common counters
// ============================================================
inline constexpr char EXEC_TIME[] = "ExecTime";
inline constexpr char INIT_TIME[] = "InitTime";
inline constexpr char OPEN_TIME[] = "OpenTime";
inline constexpr char CLOSE_TIME[] = "CloseTime";
inline constexpr char PROJECTION_TIME[] = "ProjectionTime";
inline constexpr char MEMORY_USAGE[] = "MemoryUsage";

// ============================================================
// PipelineTask counters (pipeline_task.cpp)
// ============================================================
inline constexpr char TASK_CPU_TIME[] = "TaskCpuTime";
inline constexpr char EXECUTE_TIME[] = "ExecuteTime";
inline constexpr char PREPARE_TIME[] = "PrepareTime";
inline constexpr char GET_BLOCK_TIME[] = "GetBlockTime";
inline constexpr char GET_BLOCK_COUNTER[] = "GetBlockCounter";
inline constexpr char SINK_TIME[] = "SinkTime";
inline constexpr char WAIT_WORKER_TIME[] = "WaitWorkerTime";
inline constexpr char NUM_SCHEDULE_TIMES[] = "NumScheduleTimes";
inline constexpr char NUM_YIELD_TIMES[] = "NumYieldTimes";
inline constexpr char CORE_CHANGE_TIMES[] = "CoreChangeTimes";
inline constexpr char MEMORY_RESERVE_TIMES[] = "MemoryReserveTimes";
inline constexpr char MEMORY_RESERVE_FAILED_TIMES[] = "MemoryReserveFailedTimes";

// ============================================================
// Scan operator counters (scan_operator.cpp)
// ============================================================
inline constexpr char ROWS_READ[] = "RowsRead";
inline constexpr char NUM_SCANNERS[] = "NumScanners";
inline constexpr char SCAN_ROWS[] = "ScanRows";
inline constexpr char SCAN_BYTES[] = "ScanBytes";
inline constexpr char SCANNER_GET_BLOCK_TIME[] = "ScannerGetBlockTime";
inline constexpr char SCANNER_CPU_TIME[] = "ScannerCpuTime";
inline constexpr char SCANNER_FILTER_TIME[] = "ScannerFilterTime";
inline constexpr char SCANNER_WORKER_WAIT_TIME[] = "ScannerWorkerWaitTime";
inline constexpr char NEWLY_CREATE_FREE_BLOCKS_NUM[] = "NewlyCreateFreeBlocksNum";
inline constexpr char MAX_SCAN_CONCURRENCY[] = "MaxScanConcurrency";
inline constexpr char MIN_SCAN_CONCURRENCY[] = "MinScanConcurrency";
inline constexpr char RUNNING_SCANNER[] = "RunningScanner";

// ============================================================
// Spill write counters (shared between Source and Sink)
// ============================================================
inline constexpr char SPILL_TOTAL_TIME[] = "SpillTotalTime";
inline constexpr char SPILL_WRITE_TIME[] = "SpillWriteTime";
inline constexpr char SPILL_WRITE_TASK_WAIT_IN_QUEUE_COUNT[] = "SpillWriteTaskWaitInQueueCount";
inline constexpr char SPILL_WRITE_TASK_COUNT[] = "SpillWriteTaskCount";
inline constexpr char SPILL_WRITE_TASK_WAIT_IN_QUEUE_TIME[] = "SpillWriteTaskWaitInQueueTime";
inline constexpr char SPILL_WRITE_FILE_TIME[] = "SpillWriteFileTime";
inline constexpr char SPILL_WRITE_SERIALIZE_BLOCK_TIME[] = "SpillWriteSerializeBlockTime";
inline constexpr char SPILL_WRITE_BLOCK_COUNT[] = "SpillWriteBlockCount";
inline constexpr char SPILL_WRITE_BLOCK_BYTES[] = "SpillWriteBlockBytes";
inline constexpr char SPILL_WRITE_ROWS[] = "SpillWriteRows";

// Spill write file counters (Source-only)
inline constexpr char SPILL_WRITE_FILE_BYTES[] = "SpillWriteFileBytes";
inline constexpr char SPILL_WRITE_FILE_TOTAL_COUNT[] = "SpillWriteFileTotalCount";
inline constexpr char SPILL_WRITE_FILE_CURRENT_BYTES[] = "SpillWriteFileCurrentBytes";
inline constexpr char SPILL_WRITE_FILE_CURRENT_COUNT[] = "SpillWriteFileCurrentCount";

// ============================================================
// Spill read counters (Source-only)
// ============================================================
inline constexpr char SPILL_RECOVER_TIME[] = "SpillRecoverTime";
inline constexpr char SPILL_READ_TASK_WAIT_IN_QUEUE_COUNT[] = "SpillReadTaskWaitInQueueCount";
inline constexpr char SPILL_READ_TASK_COUNT[] = "SpillReadTaskCount";
inline constexpr char SPILL_READ_TASK_WAIT_IN_QUEUE_TIME[] = "SpillReadTaskWaitInQueueTime";
inline constexpr char SPILL_READ_FILE_TIME[] = "SpillReadFileTime";
inline constexpr char SPILL_READ_DESERIALIZE_BLOCK_TIME[] = "SpillReadDeserializeBlockTime";
inline constexpr char SPILL_READ_BLOCK_COUNT[] = "SpillReadBlockCount";
inline constexpr char SPILL_READ_BLOCK_BYTES[] = "SpillReadBlockBytes";
inline constexpr char SPILL_READ_FILE_BYTES[] = "SpillReadFileBytes";
inline constexpr char SPILL_READ_ROWS[] = "SpillReadRows";
inline constexpr char SPILL_READ_FILE_COUNT[] = "SpillReadFileCount";

// Spill partition counters (Sink-only)
inline constexpr char SPILL_MAX_ROWS_OF_PARTITION[] = "SpillMaxRowsOfPartition";
inline constexpr char SPILL_MIN_ROWS_OF_PARTITION[] = "SpillMinRowsOfPartition";

} // namespace doris::profile
