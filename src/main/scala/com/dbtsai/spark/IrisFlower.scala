package com.dbtsai.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext

case class IrisFlower(val sepalLength: Double,
                      val sepalWidth: Double,
                      val petallength: Double,
                      val petalWidth: Double,
                      val species: String)

object IrisFlowerPrediction {

  def convertRDDtoSchemaRDD(input: RDD[String], cached: Boolean, hc: HiveContext): SchemaRDD = {
    val inputWithType = input.map(_.split(",")).map(flower =>
      IrisFlower(
        flower(0).trim.toDouble,
        flower(1).trim.toDouble,
        flower(2).trim.toDouble,
        flower(3).trim.toDouble,
        flower(4).trim)
    )
    import hc.createSchemaRDD
    inputWithType.registerTempTable("iris")

    if (cached) hc.cacheTable("iris")

    inputWithType.printSchema()
    inputWithType
  }

}
