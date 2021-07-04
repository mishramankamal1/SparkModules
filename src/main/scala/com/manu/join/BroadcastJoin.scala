package com.manu.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadcastJoin extends App {

    val spark=SparkSession.builder()
      .master("local")
      .appName("BraodcastJoin")
      .getOrCreate()

    spark.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

    val largeDataframe=spark.range(1,100000000)

    val smallRdd=List((1,"Gold"), (2,"Silver"), (3,"Bronze"))

    val smallDataframe=spark.createDataFrame(smallRdd).toDF("id","medals")

    val joinedDf=largeDataframe.join((smallDataframe),"id")
   // joinedDf.explain()

    joinedDf.show()
    Thread.sleep(10000000)

}
