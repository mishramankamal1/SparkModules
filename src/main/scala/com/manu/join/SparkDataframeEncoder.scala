package com.manu.join

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkDataframeEncoder extends App {

  case class Person(name:String,age:Long)

  val spark=SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkDataFrameEncoder")
    .getOrCreate()

  val encoder = org.apache.spark.sql.Encoders.product[Person]

  val personList: Array[Person] = (1 to 9999999).map(value => Person("p"+value, value)).toArray
  val rddPerson: RDD[Person] = spark.sparkContext.parallelize(personList,5)
  val personDF: DataFrame =spark.sqlContext.createDataFrame(rddPerson)
 // val personDS: Dataset[Person] =personDF.as(encoder)
  personDF.persist(StorageLevel.MEMORY_ONLY_SER)
//  personDS.persist(StorageLevel.MEMORY_ONLY_SER)
  personDF.show
 // personDS.show
  Thread.sleep(200000)


}
