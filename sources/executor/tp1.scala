// Databricks notebook source
// DBTITLE 1,Download the dataset of tweets
import sys.process._ 
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!

// COMMAND ----------

// DBTITLE 1,Read the file in Spark
val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))

// COMMAND ----------

// DBTITLE 1,Get a DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

// DBTITLE 1,Get an RDD with the text of the tweets
val wordRDD = df.select("text").rdd.map(row => row.getString(0))

// COMMAND ----------

// DBTITLE 1,Count words
val wordCounts = wordRDD.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

// COMMAND ----------

// DBTITLE 1,Show word counts
wordCounts.take(10).foreach(println)

// COMMAND ----------

// DBTITLE 1,Find hashtags on texts
val hashtagCounts = wordCounts.filter(_._1.matches("#\\w+$"))

// COMMAND ----------

// DBTITLE 1,Most frequent hashtags on tweets
//hashtagCounts.sortBy(_._2, false).take(10).foreach(println)

val hashtagDF = df.withColumn("hashtag", explode($"entities.hashtags.text")).groupBy("hashtag").count().orderBy($"count".desc).limit(10)

hashtagDF.show()

// COMMAND ----------

// DBTITLE 1,Get an RDD with the user of the tweets
//val userRDD = df.select("user.id_str", "user.name", "user.screen_name").rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2))))

//val userRDD = df.select("user.id_str", "user.name", "user.screen_name").orderBy(unix_timestamp(df("created_at"), "EEE MMM dd HH:mm:ss ZZZZZ yyyy").desc).rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2))))

// COMMAND ----------

// DBTITLE 1,Count users with more tweets
//val userCounts = userRDD.map(user => (user._1, (Array(user._2._1), Array(user._2._2), 1))).reduceByKey((a, b) => (a._1.union(b._1), a._2.union(b._2), a._3 + b._3))

//val userCounts = userRDD.map(user => (user._1, (user._2._1, user._2._2, 1))).reduceByKey((a, b) => (a._1, a._2, a._3 + b._3))

// COMMAND ----------

// DBTITLE 1,Users with more tweets
//userCounts.sortBy(_._2._3, false).take(10).map(user => (user._1, user._2._1, user._2._2, user._2._3)).foreach(println)

val userDF = df.groupBy("user.id_str").count().orderBy($"count".desc).limit(10)

userDF.show()

// COMMAND ----------

// DBTITLE 1,Detect trending topics
// http://blog.gainlo.co/index.php/2016/05/03/how-to-design-a-trending-algorithm-for-twitter/

// Round to 1 day interval
val interval = (round($"created_at" / 86400) * 86400).cast("timestamp").alias("interval")

val hashtagTTDF = df.
withColumn("created_at", unix_timestamp(df("created_at"), "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).
withColumn("hashtag", explode($"entities.hashtags.text")).
           groupBy(interval, $"hashtag").
           agg(count("hashtag").alias("frequency")).
           orderBy($"interval", $"frequency".desc)

val tweetTTDF = df.withColumn("created_at", unix_timestamp(df("created_at"), "EEE MMM dd HH:mm:ss ZZZZZ yyyy"))                          

val hashtagTTRDD = hashtagTTDF.withColumn("interval", col("interval").cast("string")).rdd.map(row => (row.getString(0), (row.getString(1), row.getLong(2))))

//val hashtagTT = hashtagTTRDD.reduceByKey((a, b) => if a._1.ignoreCase(b._1))

hashtagTTRDD.
map(hashtag => (hashtag._1, (List(hashtag._2)))).
reduceByKey((a, b) => (a.union(b))).
map(hashtag => 
    (hashtag._1, hashtag._2.sortWith(_._2 > _._2).take(5).map(topic => topic._1))
   ).
sortBy(_._1).collect().
map(tt => 
    (tt._1.substring(0, 10), (tt._2(0), tt._2(1), tt._2(2), tt._2(3), tt._2(4)))
   ).
foreach(println)

// COMMAND ----------


