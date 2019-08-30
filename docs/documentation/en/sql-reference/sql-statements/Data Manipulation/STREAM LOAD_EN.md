# STREAM LOAD
## Description
NAME:
stream-load: load data to table in streaming

SYNOPSIS
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

DESCRIPTION
This statement is used to import data to a specified table, which differs from ordinary Load in that it is imported synchronously.
This import method can still guarantee the atomicity of a batch of import tasks, either all data imports succeed or all failures.
This operation updates the rollup table data associated with the base table at the same time.
This is a synchronous operation, the entire data import work is completed and returned to the user import results.
Currently, HTTP chunked and non-chunked uploads are supported. For non-chunked uploads, Content-Length must be used to indicate the length of uploaded content, so as to ensure the integrity of data.
In addition, it is better for users to set Expect Header field content 100-continue, which can avoid unnecessary data transmission in some error scenarios.

OPTIONS
Users can pass in import parameters through the Header section of HTTP

Label: A label that is imported at one time. Data from the same label cannot be imported many times. Users can avoid the problem of duplicate data import by specifying Label.
Currently Palo retains the recently successful label within 30 minutes.

Column_separator: Specifies the column separator in the import file, defaulting to t. If the character is invisible, it needs to be prefixed with x, using hexadecimal to represent the separator.
For example, the separator X01 of the hit file needs to be specified as - H "column_separator: x01"

Columns: Used to specify the correspondence between columns in the import file and columns in the table. If the column in the source file corresponds exactly to the content in the table, then you do not need to specify the content of this field.
If the source file does not correspond to the table schema, some data conversion is required for this field. There are two forms of column. One is to directly correspond to the field in the imported file, which is represented by the field name.
One is a derived column with the grammar `column_name'= expression. Give me a few examples to help understand.
Example 1: There are three columns "c1, c2, c3" in the table, and the three columns in the source file correspond to "c3, c2, c1" at one time; then - H "columns: c3, c2, c1" needs to be specified.
Example 2: There are three columns "c1, c2, c3" in the table, and the first three columns in the source file correspond in turn, but there is one more column; then - H"columns: c1, c2, c3, XXX"need to be specified;
The last column is free to specify a name placeholder.
Example 3: There are three columns "year, month, day" in the table. There is only one time column in the source file, in the format of "2018-06-01:02:03";
那么可以指定-H "columns: col, year = year(col), month=month(col), day=day(col)"完成导入

Where: Used to extract some data. Users can set this option if they need to filter out unnecessary data.
Example 1: If you import only data whose column is larger than K1 equals 20180601, you can specify - H "where: K1 = 20180601" at import time.

Max_filter_ratio: The ratio of data that is most tolerant of being filterable (for reasons such as data irregularities). Default zero tolerance. Data irregularities do not include rows filtered through where conditions.

Partitions: Used to specify the partitions designed for this import. If the user can determine the partition corresponding to the data, it is recommended to specify the item. Data that does not satisfy these partitions will be filtered out.
For example, specify imports to p1, P2 partitions, - H "partitions: p1, p2"

RETURN VALUES
When the import is complete, the relevant content of the import will be returned in Json format. Currently includes the following fields
Status: Import the final state.
Success: This means that the import is successful and the data is visible.
Publish Timeout: Represents that the import job has been successfully Commit, but for some reason it is not immediately visible. Users can view imports as successful without retrying
Label Already Exists: Indicates that the Label has been occupied by other jobs, either successfully imported or being imported.
Users need to use the get label state command to determine subsequent operations
Others: The import failed, and the user can specify Label to retry the job.
Message: Detailed description of import status. Failure returns the specific cause of failure.
NumberTotal Rows: The total number of rows read from the data stream
Number Loaded Rows: Number of rows imported for this time is valid only for Success
Number Filtered Rows: The number of rows filtered out by this import, that is, the number of rows whose data quality is not up to par
Number Unselected Rows: Number of rows filtered out by where condition in this import
LoadBytes: The amount of data in the source file imported
LoadTime Ms: The time taken for this import
ErrorURL: Specific content of filtered data, retaining only the first 1000 items

ERRORS
The import error details can be viewed by the following statement:

SHOW LOAD WARNINGS ON 'url'

The URL is the URL given by Error URL.

## example

1. Import the data from the local file'testData'into the table'testTbl' in the database'testDb', and use Label for de-duplication.
curl --location-trusted -u root -H "label:123" -T testData http://host:port/api/testDb/testTbl/_stream_load

2. Import the data from the local file'testData'into the table'testTbl' in the database'testDb', use Label for de-duplication, and import only the data whose K1 equals 20180601.
curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData http://host:port/api/testDb/testTbl/_stream_load

3. Import data from the local file'testData'into the'testTbl' table in the database'testDb', allowing a 20% error rate (the user is in defalut_cluster)
curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData http://host:port/api/testDb/testTbl/_stream_load

4. Import the data from the local file'testData'into the table'testTbl' in the database'testDb', allowing a 20% error rate, and specify the column name of the file (the user is in defalut_cluster)
curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "columns: k2, k1, v1" -T testData http://host:port/api/testDb/testTbl/_stream_load

5. Import the data from the local file'testData'into the tables of'testTbl' in'testDb'of the database, allowing 20% error rate.
curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "partitions: p1, p2" -T testData http://host:port/api/testDb/testTbl/_stream_load

6. Import in streaming mode (user is in defalut_cluster)
seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_stream_load

7. Import tables containing HLL columns, which can be columns in tables or columns in data to generate HLL columns
curl --location-trusted -u root -H "columns: k1, k2, v1=hll_hash(k1)" -T testData http://host:port/api/testDb/testTbl/_stream_load

## keyword
STREAM,LOAD
