package com.manu.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat,col,lit,floor,rand,explode,array}
case class Person(id:Int,name:String)

object SaltingSkewedJoin extends App{

  val spark=SparkSession.builder().appName("Salting").master("local[*]").getOrCreate()
  val data1 = List.tabulate(1000000)(n=>(Person(1001,s"Person_$n")))
  val data2 = List.tabulate(200)(n=>(Person(2001,s"Person_$n")))
  val data3 = List.tabulate(2)(n=>Person(3001,s"Person_$n"))
  val data4 = List.tabulate(10)(n=>Person(5001,s"Person_$n"))

  val total_emp_data = List.concat(data1,data2,data3,data4)

  val data1_1 = List.tabulate(10000)(n=>(Person(1001,s"Dept_$n")))
  val data2_1 = List.tabulate(200)(n=>(Person(2001,s"Dept_$n")))
  val data3_1 = List.tabulate(20)(n=>Person(3001,s"Dept_$n"))
  val data4_1 = List.tabulate(100)(n=>Person(5001,s"Dept_$n"))

  val total_dept_data = List.concat(data1_1,data2_1,data3_1,data4_1)

  val rdd1 = spark.sparkContext.parallelize(total_emp_data,3)
  val rdd2 = spark.sparkContext.parallelize(total_dept_data,3)

  val df1 = spark.createDataFrame(rdd1)
  val df1_1=df1.repartition(3)
  val df2 = spark.createDataFrame(rdd2)
  val df2_1 = df2.repartition(3)

  val left_df = df1_1
    .withColumn("Skewed_id",concat(col("id"),lit("_"),lit(floor(rand(123456) * 10))))

  val right_df =df2_1.withColumn("Skewed_id",explode(
    array((0 to 10).map(lit(_)): _ *)
  ))

  //left_df.show(100)
  //right_df.show(100)

  val joinedDfWithSkewed = left_df
    .join(right_df,left_df.col("Skewed_id") ===
      concat(right_df.col("id"),lit("_"),right_df.col("Skewed_id")))

  val joinedDfWithoutSkewed = df1_1.join(df2_1,Seq("id"))
  joinedDfWithSkewed.show
  joinedDfWithoutSkewed.show


  Thread.sleep(20000000)



}
