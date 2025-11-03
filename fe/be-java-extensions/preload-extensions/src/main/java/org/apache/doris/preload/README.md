- [Preload Dependencies For BE Extensions](#Preload-Dependencies-For-BE-Extensions)
    - [Avro Scanner](#Avro-Scanner)
    - [Hudi Scanner](#Hudi-Scanner)
    - [MaxCompute Scanner](#MaxCompute-Scanner)
    - [Paimon Scanner](#Paimon-Scanner)
    - [JDBC Scanner](#JDBC-Scanner)

# Preload Dependencies For BE Extensions

## Avro Scanner

Avro Scanner Compile Dependencies:

```
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.avro</groupId>
                    <artifactId>avro-tools</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

## Hudi Scanner

Hudi Scanner Compile Dependencies:

```
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-avro</artifactId>
            <version>1.10.1</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-spark-client</artifactId>
            <version>${hudi.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-spark-common_${scala.binary.version}</artifactId>
            <version>${hudi.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-spark3-common</artifactId>
            <version>${hudi.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hudi</groupId>
            <artifactId>hudi-spark3.2.x_${scala.binary.version}</artifactId>
            <version>${hudi.version}</version>
            <exclusions>
                <exclusion>
                    <artifactId>json4s-ast_2.11</artifactId>
                    <groupId>org.json4s</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>json4s-core_2.11</artifactId>
                    <groupId>org.json4s</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>json4s-jackson_2.11</artifactId>
                    <groupId>org.json4s</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>json4s-scalap_2.11</artifactId>
                    <groupId>org.json4s</groupId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.binary.version}</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>javax.servlet</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <artifactId>jackson-module-scala_2.12</artifactId>
                    <groupId>com.fasterxml.jackson.module</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>hadoop-client-api</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>hadoop-client-runtime</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
            </exclusions>
            <version>${spark.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-launcher_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-catalyst_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <scope>compile</scope>
            <exclusions>
                <exclusion>
                    <groupId>org.codehaus.janino</groupId>
                    <artifactId>janino</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.codehaus.janino</groupId>
                    <artifactId>commons-compiler</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <!-- version of spark's janino is error -->
            <groupId>org.codehaus.janino</groupId>
            <artifactId>janino</artifactId>
            <version>${janino.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>org.codehaus.janino</groupId>
                    <artifactId>commons-compiler</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.codehaus.janino</groupId>
            <artifactId>commons-compiler</artifactId>
            <version>${janino.version}</version>
        </dependency>
        <dependency>
            <!-- version of spark's jackson module is error -->
            <groupId>com.fasterxml.jackson.module</groupId>
            <artifactId>jackson-module-scala_${scala.binary.version}</artifactId>
            <version>${jackson.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>com.google.guava</groupId>
                    <artifactId>guava</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```


## MaxCompute Scanner

MaxCompute Scanner Compile Dependencies:

```
        <dependency>
            <groupId>org.apache.arrow</groupId>
            <artifactId>arrow-memory-unsafe</artifactId>
            <version>${arrow.version}</version>
            <scope>compile</scope>
        </dependency>
```

## Paimon Scanner

```

```

## JDBC Scanner

JDBC Scanner Compile Dependencies:

```
        <dependency>
            <groupId>com.oracle.database.jdbc</groupId>
            <artifactId>ojdbc8</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid</artifactId>
        </dependency>
        <dependency>
            <groupId>com.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <classifier>all</classifier>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>com.oracle.ojdbc</groupId>
            <artifactId>orai18n</artifactId>
            <version>19.3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.doris</groupId>
            <artifactId>hive-catalog-shade</artifactId>
        </dependency>
```