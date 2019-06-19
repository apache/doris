# Spark sink For Doris

## Introduction
Doris Sink is a type of Structured Streaming sink with exactly-once fault-tolerance guarantees. It now supports two mode: Broker LOAD & Mini LOAD.  

- **Broker LOAD**: the data will write files on hdfs and Doris use hdfs broker  to load the files in. Suitable for the scenario that the data of each batch is larger than 1GB. 
- **Mini LOAD**: the data will send to Doris by HTTP PUT request. Suitable for the scenario that the data of each batch is less than 1GB (**Advice mode**) .

We will support the third mode **Streaming LOAD** recently which has higher performance than the two mode reminded above.

Note: Doris is also named ***Palo*** in Baidu
## How to build Doris jar

In `incubator-doris/extension/spark-sink`, run the command `mvn clean install -DskipTests`, which will produce a jar archieve like`spark-sql-palo_2.11-2.4.2.jar` and also install it into local maven repository, then copy the jar to your spark-client/jars

## How to write first App with Doris Sink
### dependency
Add below dependency into the pom.xml in your project.

```html
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-sql-palo_2.11</artifactId>
  <version>2.4.2</version>
</dependency>
```


### Demo:
Mini Load （Bulk Load）
	
	import org.apache.spark.sql.SparkSession
	import org.apache.spark.sql.palo.PaloConfig

    object SSSinkToPaloNewWay {
      val className = this.getClass.getName.stripSuffix("$")
    
      def main(args: Array[String]): Unit = {
        if (args.length < 7) {
          System.err.println(s"Usage: $className <computeUrl> <mysqlurl>  +
           <user> <password> <database> <table> <checkpoint>")
          sys.exit(1)
        }
        val Array(computeUrl, mysqlUrl, user, password, database, table, checkpoint) = args
        val spark = SparkSession
          .builder
          .getOrCreate()
        val lines = spark.readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", 9999)
          .load()
        import spark.implicits._
        var id = 0
        val messages = lines.map {word =>
          id += 1
          word + "," + id + "," + s"www.${word}.com\n"
        }
        val query = messages.writeStream
          .outputMode("append")
          .format("palo")
          .option(PaloConfig.COMPUTENODEURL, computeUrl)
          .option(PaloConfig.MYSQLURL, mysqlUrl)
          .option(PaloConfig.USERNAME, user)
          .option(PaloConfig.PASSWORD, password)
          .option(PaloConfig.DATABASE, database)
          .option(PaloConfig.TABLE, table)
          .option(PaloConfig.LOADWAY, "BULK_LOAD")
          .option(PaloConfig.DELETE_FILE, "false")
          .option("checkpointLocation", checkpoint)
          .start()
        query.awaitTermination()
      }
    }

Broker Load

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.palo.PaloConfig
    
    object SSSinkToPaloWithHdfs {
      val className = this.getClass.getName.stripSuffix("$")
    
      def main(args: Array[String]): Unit = {
        if (args.length < 8) {
          System.err.println(s"Usage: $className <computeUrl> <mysqlurl> " +
            "<user> <password> <database> <table> <dataDir> <checkpoint>")
          sys.exit(1)
        }
        val Array(computeUrl, mysqlUrl, user, password, database, table,
          dataDir, checkpoint) = args
     
        val spark = SparkSession
          .builder
          .getOrCreate()
        
        val lines = spark.readStream
	      .format("socket")
          .option("host", "localhost")
          .option("port", 9999)
          .load()
          import spark.implicits._
        var id = 0
        val messages = lines.map {word =>
          id += 1
          word + "," + id + "," + s"www.${word}.com\n"
        }
      
        val loadCmd = "COLUMNS TERMINATED BY \",\""
        val query = messages.writeStream
          .outputMode("append")
          .format("palo")
          .option(PaloConfig.COMPUTENODEURL, computeUrl)
          .option(PaloConfig.MYSQLURL, mysqlUrl)
          .option(PaloConfig.USERNAME, user)
          .option(PaloConfig.PASSWORD, password)
          .option(PaloConfig.DATABASE, database)
          .option(PaloConfig.TABLE, table)
          .option(PaloConfig.PALO_DATA_DIR, dataDir)
          .option(PaloConfig.BROKER_NAME, "baidu_hdfs")
          .option(PaloConfig.LOADCMD, loadCmd)
          .option(PaloConfig.LOADWAY, "LOAD")
          .option(PaloConfig.DELETE_FILE, "false")
          .option("checkpointLocation", checkpoint)
          .start()
        query.awaitTermination()
      }
    }
	