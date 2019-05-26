package ts.sstreaming.test

import org.apache.spark.sql.{SaveMode, SparkSession}

object ApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate();


    var input = spark.read.option("header","true").csv("hdfs://192.168.10.12:9000/flok/layer1_35all_J247.csv")
    input = input.union(input)
    input = input.union(input)
    input = input.union(input)
    input = input.union(input)
    input.createOrReplaceTempView("t")
    input = spark.sql("select *,ROW_NUMBER() OVER (PARTITION BY host ORDER BY align_time DESC) id from t")
    input.write.option("header","true").mode(SaveMode.Overwrite).csv("hdfs://192.168.10.12:9000/sim_big_36.csv")

  }
}
