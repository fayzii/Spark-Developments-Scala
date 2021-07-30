import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StringType, StructType}
object SparkComplexDTypes {

  def main(args: Array[String]): Unit = {
      val spark = SparkSession.
        builder().master("local").
        appName("Spark Complex Data Types").
        getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /*val arrayStructureData = Seq(
      Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")),
        Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
      Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")),
        Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
      Row("Robert",List(Row("LasVegas","NV")),
        Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
      Row("Maria",null,Map("hair"->"blond","eye"->"red"),
        Map("height"->"5.6")),
      Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")),
        Map("white"->"black","eye"->"black"),Map("height"->"5.2")))*/

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("addresses", StringType)
      .add("properties", StringType)
      .add("secondProp", StringType)
      .add("heightProp",StringType)
    //val mapType  = DataTypes.createMapType(StringType,StringType)

    /*val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("addresses", ArrayType(new StructType()
        .add("city",StringType)
        .add("state",StringType)))
      .add("properties", mapType)
      .add("secondProp", MapType(StringType,StringType))*/

    /*val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)*/
    //mapTypeDF.printSchema()
    val mapTypeDF = spark.read.schema(arrayStructureSchema).
      csv("D:\\Projects\\Learning Projects\\Spark\\complexTypesExampleData.txt")

    val arrayTypeDF = mapTypeDF.
      withColumn("addresses",split(col("addresses"),"\\|")).
      withColumn("propsKeys",split(col("properties"),":").getItem(0)).
      withColumn("propsValues",split(col("properties"),":").getItem(1)).
      withColumn("properties", map(col("propsKeys"),col("propsValues"))).
      withColumn("secondPropMap",map(split(col("secondProp"),":").getItem(0),
        split(col("secondProp"),":").getItem(1))).
      drop("propsKeys").drop("propsValues").drop("secondProp")

    arrayTypeDF.show(false)
    arrayTypeDF.printSchema()

    //mapTypeDF.select(col("name"),map_keys(col("properties"))).show(false)

    //mapTypeDF.select(col("name"),map_values(col("properties"))).show(false)

    //mapTypeDF.select(col("name"),map_concat(col("properties"),col("secondProp"))).show(false)

    spark.stop()

  }
}
