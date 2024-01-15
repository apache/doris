---
{
    "title": "Profile Action",
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

# Profile Action

## Request

`GET /api/profile`
`GET /api/profile/text`

## Description

用于获取指定 query id 的 query profile
如果query_id不存在, 直接返回404 NOT FOUND错误
如果query_id存在，返回下列文本的profile:

```
Query:
  Summary:
     - Query ID: a0a9259df9844029-845331577440a3bd
     - Start Time: 2020-06-15 14:10:05
     - End Time: 2020-06-15 14:10:05
     - Total: 8ms
     - Query Type: Query
     - Query State: EOF
     - Doris Version: trunk
     - User: root
     - Default Db: default_cluster:test
     - Sql Statement: select * from table1
  Execution Profile a0a9259df9844029-845331577440a3bd:(Active: 7.315ms, % non-child: 100.00%)
    Fragment 0:
      Instance a0a9259df9844029-845331577440a3be (host=TNetworkAddress(hostname:172.26.108.176, port:9560)):(Active: 1.523ms, % non-child: 0.24%)
         - MemoryLimit: 2.00 GB
         - PeakUsedReservation: 0.00
         - PeakMemoryUsage: 72.00 KB
         - RowsProduced: 5
         - AverageThreadTokens: 0.00
         - PeakReservation: 0.00
        BlockMgr:
           - BlocksCreated: 0
           - BlockWritesOutstanding: 0
           - BytesWritten: 0.00
           - TotalEncryptionTime: 0ns
           - BufferedPins: 0
           - TotalReadBlockTime: 0ns
           - TotalBufferWaitTime: 0ns
           - BlocksRecycled: 0
           - TotalIntegrityCheckTime: 0ns
           - MaxBlockSize: 8.00 MB
        DataBufferSender (dst_fragment_instance_id=a0a9259df9844029-845331577440a3be):
           - AppendBatchTime: 9.23us
             - ResultSendTime: 956ns
             - TupleConvertTime: 5.735us
           - NumSentRows: 5
        OLAP_SCAN_NODE (id=0):(Active: 1.506ms, % non-child: 20.59%)
           - TotalRawReadTime: 0ns
           - CompressedBytesRead: 6.47 KB
           - PeakMemoryUsage: 0.00
           - RowsPushedCondFiltered: 0
           - ScanRangesComplete: 0
           - ScanTime: 25.195us
           - BitmapIndexFilterTimer: 0ns
           - BitmapIndexFilterCount: 0
           - NumScanners: 65
           - RowsStatsFiltered: 0
           - VectorPredEvalTime: 0ns
           - BlockSeekTime: 1.299ms
           - RawRowsRead: 1.91K (1910)
           - ScannerThreadsVoluntaryContextSwitches: 0
           - RowsDelFiltered: 0
           - IndexLoadTime: 911.104us
           - NumDiskAccess: 1
           - ScannerThreadsTotalWallClockTime: 0ns
             - MaterializeTupleTime: 0ns
             - ScannerThreadsUserTime: 0ns
             - ScannerThreadsSysTime: 0ns
           - TotalPagesNum: 0
           - RowsReturnedRate: 3.319K /sec
           - BlockLoadTime: 539.289us
           - CachedPagesNum: 0
           - BlocksLoad: 384
           - UncompressedBytesRead: 0.00
           - RowsBloomFilterFiltered: 0
           - TabletCount : 1
           - RowsReturned: 5
           - ScannerThreadsInvoluntaryContextSwitches: 0
           - DecompressorTimer: 0ns
           - RowsVectorPredFiltered: 0
           - ReaderInitTime: 6.498ms
           - RowsRead: 5
           - PerReadThreadRawHdfsThroughput: 0.0 /sec
           - BlockFetchTime: 4.318ms
           - ShowHintsTime: 0ns
           - TotalReadThroughput: 0.0 /sec
           - IOTimer: 1.154ms
           - BytesRead: 48.49 KB
           - BlockConvertTime: 97.539us
           - BlockSeekCount: 0
```
如果为text接口，直接返回profile的纯文本内容  
    
## Path parameters

无

## Query parameters

* `query_id`

    指定的 query id

## Request body

无

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"profile": "query profile ..."
	},
	"count": 0
}
```
    
## Examples

1. 获取指定 query_id 的 query profile

    ```
    GET /api/profile?query_id=f732084bc8e74f39-8313581c9c3c0b58
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"profile": "query profile ..."
    	},
    	"count": 0
    }
    ```
2. 获取指定 query_id 的 query profile 的纯文本
    ```
    GET /api/profile/text?query_id=f732084bc8e74f39-8313581c9c3c0b58
    
    Response:
        Summary:
        - Profile ID: 48bdf6d75dbb46c9-998b9c0368f4561f
        - Task Type: QUERY
        - Start Time: 2023-12-20 11:09:41
        - End Time: 2023-12-20 11:09:45
        - Total: 3s680ms
        - Task State: EOF
        - User: root
        - Default Db: tpcds
        - Sql Statement: with customer_total_return as
      select sr_customer_sk as ctr_customer_sk
      ,sr_store_sk as ctr_store_sk
      ,sum(SR_FEE) as ctr_total_return
      ...
    ```

