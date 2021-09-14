# SparkToHBASE
## Get log file by Spark and write it to HBASE

```scala
// Read log file from hadoop
val access=spark.read.textFile("hdfs://127.0.0.1:19000/access.log")

// Split log lines by spaces
val df=access.map(x=>x.split(' ')).toDF()

// Make data with columns, drop unnecessary
val df1=df.select((0 until 12).map(i=>col("value")(i).alias(s"col$i")): _*)

// Delete left symbol from date column to prepare for convertion
val df2=df1.withColumn("col3", expr("substring(col3, 2, length(col3))"))

// Convert date string to timestamp
val df3=df2.withColumn("DateTime",to_timestamp($"col3","dd/MMM/yyyy:HH:mm:ss"))

// Format data for HBASE table
val df4=df3.select('col0 as "ip", 'col11 as "url", 'col9 as "traffic", 'DateTime)

// Load HBASE connector
:require /Users/User/hbase-spark-2.0.0-alpha4.jar
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

// Create HBASE catalog
def catalog = s"""{
	"table":{"namespace":"default", "name":"Logs"}, 
	"rowkey":"key", 
	"columns":{"ip":{"cf":"rowkey", "col":"ip", "type":"string"}, 
		"url":{"cf":"line", "col":"url", "type":"string"}, 
		"traffic":{"cf":"line", "col":"traffic", "type":"string"}, 
		"date":{"cf":"line", "col":"DateTime", "type":"string"}
		}
}""".stripMargin

// Write data to HBASE
df4.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> “5”))
	.format(“org.apache.spark.sql.execution.datasources.hbase”)
	.save()
```
