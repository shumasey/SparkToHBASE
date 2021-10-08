// Read log files from hadoop
val log=spark.read.format("csv").option("inferschema",true).option("header",true).option("delimiter",";").load("hdfs://0.0.0.0:19000/input/*.log")
// HBase requires unique rowkey for every row, so we have to generate them. First let's make timestamp column from String date and time.
val log1=log.withColumn("dt",concat(col("date"),lit(" "),col("time")))
// Spark 2.3.3 doesn't support milliseconds, so here we need use UDF
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import scala.util.{Try, Success, Failure}
val getTimestamp: (String => Option[Timestamp])=s => s match {case "" =>None case _ =>{val format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS"); Try(new Timestamp(format.parse(s).getTime)) match {case Success(t) => Some(t) case Failure(_) => None}}}
val getTimestampUDF=udf(getTimestamp)
val tts=getTimestampUDF($"dt")
val log2=log1.withColumn("ts", tts)
// Now we can use window for generating technical key
import org.apache.spark.sql.expressions.Window
val key=Window.partitionBy('dummy).orderBy('ts)
val log3=log2.withColumn("key", rank over key)
// Take columns to write to HBase
val log4=log3.select('key,'user,'date,'time,'Device,'Model,'gt,'x,'y,'z)
// We need to load special connector to write data to HBase 
:require C:/GitHub/SparkToHBASE/jar/shc-core-1.1.0.3.1.5.6121-6.jar
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
// Create HBASE catalog
def catalog=s"""{"table":{"namespace":"default", "name":"logs"}, "rowkey":"key", "columns":{"key":{"cf":"rowkey", "col":"key", "type":"integer"}, "user":{"cf":"line", "col":"user", "type":"string"}, "date":{"cf":"line", "col":"date", "type":"string"}, "time":{"cf":"line", "col":"time", "type":"string"}, "Device":{"cf":"line", "col":"Device", "type":"string"}, "Model":{"cf":"line", "col":"Model", "type":"string"}, "gt":{"cf":"line", "col":"gt", "type":"string"}, "x":{"cf":"line", "col":"x", "type":"double"}, "y":{"cf":"line", "col":"y", "type":"double"}, "z":{"cf":"line", "col":"z", "type":"double"}}}""".stripMargin
// Write data to HBase
log4.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()