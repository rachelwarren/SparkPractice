package com.dbtsai.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class IrisFlowerSuites extends FunSuite with BeforeAndAfterAll {
  @transient var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  test("Iris Data Prediction") {
    // There are three types of follower, setosa, virginica, versicolor
    val file = this.getClass.getResource("/iris.csv").toURI.toString

    val input = sc.textFile(file)

    val hc: HiveContext = new HiveContext(sc)

    val schemaRDD = IrisFlowerPrediction.convertRDDtoSchemaRDD(input, cached = true, hc)

    val tenLines = schemaRDD.take(10)

    val setosaVersicolor = hc.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    val setosaVirginica = hc.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    val versicolorVirginica = hc.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    
    println("")

  }

}
