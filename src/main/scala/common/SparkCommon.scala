package common

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession():SparkSession = {
    logger.info("Inside: SparkCommon createSparkSession")
    // Create a Spark Session
    // For Windows
    System.setProperty("hadoop.home.dir", "E:\\notes\\Hadoop\\winutils")
    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    logger.info("Exit: SparkCommon createSparkSession")

    spark
  }

}
