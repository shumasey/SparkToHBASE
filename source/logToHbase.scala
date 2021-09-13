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
