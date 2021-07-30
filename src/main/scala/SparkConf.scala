import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object SparkConf {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Bucketing_Partitioning_Example")
      .getOrCreate()

    //Set Log Level to ERROR Only
    spark.sparkContext.setLogLevel("ERROR")

    val customersDF = spark.read.
      option("inferSchema",true).
      option("header",true).
      csv("D:/Projects/Spark Bucketing Example Scala/customers.csv")

    //customersDF.show(false)
    println(customersDF.rdd.getNumPartitions)

    val transformedDF = customersDF.
      withColumn("CombinedColumn",concat(col("Region"),lit(" "),col("Milk"))).
      withColumn("Id_by_int",col("Id").cast(IntegerType)).
      drop("Id").withColumnRenamed("Id_by_int","Id")

    //transformedDF.show(false)
    println(transformedDF.rdd.getNumPartitions)
    transformedDF.printSchema()

    /* While Bucketing no. of files will be df partitions * buckets
    *  In my case i have 1 df partition i set it to 2 using repartition
    *  No. of files will be no. of buckets * df partitions
    *  e.g. 2 * 2 = 4 per partition which is Region
    * */

    transformedDF.
      repartition(2).
      write.
      format("csv").
      mode("overwrite").
      bucketBy(2,"Id").
      sortBy("Id").partitionBy("Region").
      option("path","C:\\Users\\Muhammad-Fayyaz\\spark-warehouse\\customers").
      saveAsTable("testTable")
  }
}
