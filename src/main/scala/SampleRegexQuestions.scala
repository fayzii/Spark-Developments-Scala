import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object SampleRegexQuestions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().master("local").
      appName("Spark Regex Questions").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val Students = List(
      (1,"Alice","She likes programming and travelling."),
      (2,"Bob","He is interested in music and plays guitar."),
      (3,"Trudy","He likes to play basketball."),
      (4,"Martin","He is interested in singing.")
    )

    val studentsDF = spark.createDataFrame(Students)
    val filterList = List("guitar","dance","singing")
    val rgxList = filterList.mkString("|")
    val resDF = studentsDF.filter(col("_3").rlike(s"(?i)($rgxList)")).select("_1","_2")
    resDF.show()
  }
}
