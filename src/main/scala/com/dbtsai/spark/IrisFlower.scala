package com.dbtsai.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext

case class IrisFlower(val sepalLength: Double, val sepalWidth: Double, val petallength: Double, val petalWidth: Double, val species: String)

object IrisFlowerPrediction {

//  def convertRDDtoSchemaRDD(input: RDD[String], cached: Boolean, hc: HiveContext): SchemaRDD = {
//
//  }

}
