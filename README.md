# SparkToHBASE

This tutorial demonstrates how to move log data from Hadoop to HBase using Spark.

### Prepare the enviroment
1. Install JDK 8
2. Install Hadoop
3. Install HBase
4. Install Spark-2.3.3-bin-hadoop2.7 (because Spark-HBase connector uses Scala 2.11)

### Load data to Hadoop

Open Command Prompt as Administrator and type __start-all__. This opens four windows, don't close them. Then type __hdfs dfs -ls /__ to see root catalogue.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/starthdfs.png)

It's empty from the beginning. Type __hdfs dfs -put /C:/GitHub/SparkToHBASE/input hdfs://__ to load log files, then check catalogue again.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/puthdfs.png)

Type __hdfs getconf -confkey fs.defaultFS__ to get hdfs IP and port, Spark needs them.

### Read data to Spark and transform them

Open new Command Prompt as Administrator and type __spark-shell__. You can either type __:load /C:/GitHub/SparkToHBASE/Spark_to_hbase.scala__(don't forget to start Hbase first) or paste command lines one by one in order to see intermediate results.


```scala
// Read log files from hadoop
val log=spark.read.format("csv")
	.option("inferschema",true)
	.option("header",true)
	.option("delimiter",";")
	.load("hdfs://0.0.0.0:19000/input/*.log")
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/readlogs.png)

// HBase requires unique rowkey for every row, so we have to generate them. First let's make timestamp column from String date and time.
val log1=log.withColumn("dt",concat(col("date"),lit(" "),col("time")))
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/concat.png)

// Spark 2.3.3 doesn't support milliseconds, so here we need use UDF
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import scala.util.{Try, Success, Failure}
val getTimestamp: (String => Option[Timestamp])=s => s match {
	case "" =>None case _ =>{
		val format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS");
		Try(new Timestamp(format.parse(s).getTime)) match {
			case Success(t) => Some(t) 
			case Failure(_) => None
			}
		}
	}
val getTimestampUDF=udf(getTimestamp)
val tts=getTimestampUDF($"dt")
val log2=log1.withColumn("ts", tts)
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/timestamp.png)

// Now we can use window for generating technical key
import org.apache.spark.sql.expressions.Window
val key=Window.partitionBy('dummy).orderBy('ts)
val log3=log2.withColumn("key", rank over key)
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/key.png)

// Take columns to write to HBase
val log4=log3.select('key,'user,'date,'time,'Device,'Model,'gt,'x,'y,'z)

// To write data to HBase we need to load special connector
:require C:/GitHub/SparkToHBASE/jar/shc-core-1.1.0.3.1.5.6121-6.jar
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


// Create HBASE catalog
def catalog=s"""{
	"table":{"namespace":"default", "name":"logs"}, 
	"rowkey":"key", "columns":{
		"key":{"cf":"rowkey", "col":"key", "type":"integer"}, 
		"user":{"cf":"line", "col":"user", "type":"string"}, 
		"date":{"cf":"line", "col":"date", "type":"string"}, 
		"time":{"cf":"line", "col":"time", "type":"string"}, 
		"Device":{"cf":"line", "col":"Device", "type":"string"}, 
		"Model":{"cf":"line", "col":"Model", "type":"string"}, 
		"gt":{"cf":"line", "col":"gt", "type":"string"}, 
		"x":{"cf":"line", "col":"x", "type":"double"}, 
		"y":{"cf":"line", "col":"y", "type":"double"}, 
		"z":{"cf":"line", "col":"z", "type":"double"}
		}
	}""".stripMargin
```
	
## Start HBase

Open Command Prompt as Administrator and go to HBase /bin folder. Type __start-hbase__, it's open master start window, then type __hbase shell__. Ignore error and type __list__. From the beginning it's empty.

## Write data to HBase

Go to Spark window and execute write command
```scala
log4.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
	.format("org.apache.spark.sql.execution.datasources.hbase")
	.save()
```
In case of strange error just repeat the command.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/writehbase.png)

Go to HBase window and type __list__ again. Now there is a table 'logs'.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/list.png)

Type __scan 'logs', {LIMIT=>2}__ to see table contents.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/scan.png)

Type __count 'logs'__ to check all rows was loaded.
![img](https://github.com/shumasey/SparkToHBASE/blob/main/screenshots/count.png)
