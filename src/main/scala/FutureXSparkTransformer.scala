import java.util.Properties

import common.SparkCommon.{getClass, logger}
import common.{PostgressCommon, SparkCommon}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object FutureXSparkTransformer {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try{
      val spark : SparkSession = SparkCommon.createSparkSession()

      logger.info("Created Spark Session")
      val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

      val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
      df.show()
      //df.write.format("csv").save("samplesq")

      //Create a DataFrame from Postgres Course Catalog table
      logger.info("Creating Dataframe from Postgres")
      val pgConnectionProperties = PostgressCommon.getPostgressCommonProps()

      pgConnectionProperties.put("user","postgres")
      pgConnectionProperties.put("password","admin")

      val pgTable = "futureschema.futurex_course_catalog"
      //server:port/database_name
      //val pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://localhost:5432/futurex",pgTable,pgConnectionProperties)
      val pgCourseDataframe = PostgressCommon.fetchDataframeFromPgTable(spark, pgTable).get
      logger.info("Fetched")

      pgCourseDataframe.show()
      logger.info("Shown")
    } catch {
      case e: Exception =>
       logger.error("an error has occurred in the method main for FutureXSparkTransformer"+e.printStackTrace())
    }
  }
}
