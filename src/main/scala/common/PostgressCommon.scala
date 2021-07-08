package common

import java.util.Properties

import common.SparkCommon.{getClass, logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object PostgressCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)


  def getPostgressCommonProps(): Properties = {
    logger.info("Inside: PostgressCommon getPostgressCommonProps")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","admin")

    logger.info("Exit: PostgressCommon getPostgressCommonProps")
    pgConnectionProperties
  }

  def getPostgressServerDatabase():String = {
    val pgUrl = "jdbc:postgresql://localhost:5432/futurex"
    pgUrl
  }

  def fetchDataframeFromPgTable(spark: SparkSession, pgTable: String): Option[DataFrame] = {
    try{
      val pgCourseDataframe = spark.read.jdbc(getPostgressServerDatabase,pgTable,getPostgressCommonProps)
      Some(pgCourseDataframe)
    } catch {
      case e:Exception =>
        logger.error("an error has occurred in class PostgressCommon" + e.printStackTrace())
        None
    }
  }
}
