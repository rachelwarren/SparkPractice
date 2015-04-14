//package com.dbtsai.spark
//
//import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.sql.SchemaRDD
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//class IrisFlowerSuites extends FunSuite with BeforeAndAfterAll {
//  @transient var sc: SparkContext = _
//
//  override def beforeAll() {
//    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("test")
//    sc = new SparkContext(conf)
//    super.beforeAll()
//  }
//
//  override def afterAll() {
//    if (sc != null) {
//      sc.stop()
//    }
//    super.afterAll()
//  }
//
//  test("Iris Data Prediction") {
//    // There are three types of follower, setosa, virginica, versicolor
//    val file = this.getClass.getResource("/iris.csv").toURI.toString
//
//    val input = sc.textFile(file)
//
//    val hc: HiveContext = new HiveContext(sc)
//
//    val schemaRDD = IrisFlowerPrediction.convertRDDtoSchemaRDD(input, cached = true, hc)
//
//    val tenLines = schemaRDD.take(10)
//
//    val setosaVersicolor = hc.sql("SELECT * FROM iris WHERE species == 'setosa' OR species == 'versicolor'")
//
//    // Be very careful about this....
//    val setosaVersicolorCollection = setosaVersicolor
//
//    val setosaVirginica: SchemaRDD = hc.sql("SELECT * FROM iris WHERE species == 'setosa' OR species == 'virginica'")
//    val setosaVirginicaCollection = setosaVirginica.collect()
//
//    val versicolorVirginica = hc.sql("SELECT * FROM iris WHERE species == 'versicolor' OR species == 'virginica'")
//    val versicolorVirginicaCollection = setosaVirginica.collect()
//
//    val trainner = (new LogisticRegressionWithLBFGS).setIntercept(true).setValidateData(false)
//
//    val setosaVersicolorTraining = setosaVersicolor.map(x => {
//      var y = 0.0
//      if (x(4) == "setosa") y = 0.0 else y = 1.0
//      val v = Vectors.dense(Array[Double](
//        x(0).asInstanceOf[Double],
//        x(1).asInstanceOf[Double],
//        x(2).asInstanceOf[Double],
//        x(3).asInstanceOf[Double])
//      )
//      LabeledPoint(y, v)
//    }).persist()
//
//    val classifier1 = trainner.run(setosaVersicolorTraining)
//
//    val predictedResult = classifier1.predict(setosaVersicolorTraining.map(_.features))
//
//    val predicitedP = setosaVersicolorTraining.map(x => {
//      var margin: Double = (new breeze.linalg.DenseVector[Double](classifier1.weights.toArray))
//        .dot(new breeze.linalg.DenseVector[Double](x.features.toArray))
//      margin += classifier1.intercept
//      val score = 1.0 / (1.0 + math.exp(-margin))
//      score
//    }).collect()
//
//    val correctCounts = predictedResult.zip(setosaVersicolorTraining).map(x =>
//      if (x._1 == x._2.label) 1L else 0L
//    ).reduce(_ + _)
//
//    val acc = (correctCounts * 1.0) / setosaVersicolorTraining.count()
//
//    val setosaVersicolorTrainingCollection = setosaVersicolorTraining.collect()
//
//    setosaVersicolorTraining.count()
//
//    println("")
//
//  }
//
//}
