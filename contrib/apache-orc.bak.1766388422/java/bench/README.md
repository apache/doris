# File Format Benchmarks

These big data file format benchmarks, compare:

* Avro
* Json
* ORC
* Parquet

There are three sub-modules to try to mitigate dependency hell:

* core - the shared part of the benchmarks
* hive - the Hive benchmarks
* spark - the Spark benchmarks

To build this library, run the following in the parent directory:

```
% ./mvnw clean package -Pbenchmark -DskipTests
% cd bench
```

To fetch the source data:

```% ./fetch-data.sh```

> :warning: Script will fetch 4GB of data

To generate the derived data:

```% java -jar core/target/orc-benchmarks-core-*-uber.jar generate data```

To run a scan of all of the data:

```% java -jar core/target/orc-benchmarks-core-*-uber.jar scan data```

To run full read benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar read-all data```

To run a write benchmark: 
```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar write data```

To run column projection benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar read-some data```

To run decimal/decimal64 benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar decimal data```

To run row-filter benchmark:

```% java -jar hive/target/orc-benchmarks-hive-*-uber.jar row-filter data```

To run spark benchmark:

```% java -jar spark/target/orc-benchmarks-spark-*.jar spark data```

