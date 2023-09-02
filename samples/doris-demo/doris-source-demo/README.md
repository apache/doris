# Doris reads data through the Thrift interface
## Scenes
It is necessary to read a large amount of data, and spark/flink connectors use this method to read data.
## process
### 1. Query the corresponding queryPlan according to SQL
Call api：/api/<db>/<table>/_query_plan
Specifically, can see: https://doris.apache.org/zh-CN/docs/dev/admin-manual/http-actions/fe/table-query-plan-action
This interface will return the tabletID hit by the corresponding SQL and the distributed backend information

### 2. Access backend and read tablet data directly
Interact with BE through Thrift interface, Thrift interface definition can be seen TDorisExternalService
#### 2.1 doris will build  a scan context for this session, context_id returned if success
TScanOpenResult open(1: TScanOpenParams params);

#### 2.2 return the batch_size of data
TScanBatchResult getNext(1: TScanNextBatchParams params);

#### 2.3 release the context resource associated with the context_id
TScanCloseResult close(1: TScanCloseParams params);

### 3. Convert Arrow data
Read data through Arrow and convert it to row data

For more design documents, please refer to: https://github.com/apache/doris/issues/1525
