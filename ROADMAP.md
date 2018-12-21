# Doris Road Map (2019)

1. Support loading data from Kafka (On the way)

    User can create a routine load job to load data from Kafka continuously, by using streaming load interface.

2. Support query on ElasticSearch (On the way)

    By creating an ElasticSearch table in Doris(mapping to the real ElasticSearch index), user can query ElasticSearch by SQL, utilizing both the full-text search capability of ES and complex SQL query engine of Doris.

3. Support UDF/UDAF (On the way)

    By writing C(C++) implemented UDF/UDAF library, user can implement its own business logic.

4. Support loading data file in Parquet format

    Embrace the hadoop eco-system.

5. Support all queries in TPC-DS

    Support all 99 SQL queries in standard TPC-DS benchmark.

7. Query circuit-breaker mechanism and working queue for heavy query load

    Avoid unreasonable queries which can running out of system resourece. And working queue for low priority query jobs.

8. Newly implemented query optimizer (On the way)

    Implement an extensible query optimization framework, for both Rule-Based Optimization and Cost-Based Optimization.

9. Efficient decimal data type implementation (On the way)

    Decimal is every common in some e-commerce scenarios.

9. Improve the compatibility of meta data between different versions

    Serialize meta data in ProtoBuf, make it both forward and backward compatibility.
    
10. Separation between storage and computation

    Make Doris a more elastic and cloud native data warehouse.

11. Support UPDATE operation

12. Support Time Series Data
