package com.dbtsai.spark

import java.io.{BufferedWriter, FileWriter, PrintWriter, File}

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import math._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
Helper object for testing Kmeans. returns true is objects beging compare are equal within some user
  * specified margin
  */
object DoubleCompare {
  /**
   * Compares two doubles given a margin of error
   *
   * @param a A double
   * @param b A double
   * @param margin the allowable error between a and b
   * @return true if abs(a-b) < margin
   */
  def fuzzyCompareDouble(a: Double, b: Double, margin: Double): Boolean = (abs(a - b) < margin)

  def fuzzyCompareArray(a: Array[Double], b: Array[Double], margin: Double): Boolean = {
    if (a.length != b.length) false
    else {
      var i = 0
      var bool = true
      while (i < a.length && bool) {
        bool = fuzzyCompareDouble(a(i), b(i), margin)
        i += 1
      }
      bool
    }
  }
}

/**
 * Test sweet for Rachel's KMeans implementation.
 */
class KMeanSuites extends FunSuite with BeforeAndAfterAll {
  @transient var sc: SparkContext = _
  val testMeans: Array[Vector] = Array(
    Vectors.dense(Array(1.0, 3.0, 4.0)),
    Vectors.dense(Array(29.0, 50.0, 100.0))
  )
  val v = Vectors.dense(Array(30.0, 51.0, 101.0))
  var obj: KMeans = _
  val fw = new FileWriter("log.txt", true)
  val w = new PrintWriter(fw)
  var model: KMeansModel = _
  var denseVectorData: RDD[Vector] = _
  var sparseData: RDD[Vector] = _
  var sparseModel: KMeansModel = _


  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      w.close()
      fw.close()
      sc.stop()
    }
    super.afterAll()
  }

  test("Initialize KMeans Object ") {

    w.println("Initialize KMeans")
    obj = new KMeans(testMeans)
    w.println(obj.getCenters(0))

    assert(obj.getCenters(0).equals(testMeans(0)))
    assert(obj.getCenters(1).equals(testMeans(1)))
  }
  test("Find Closest Method") {
    val (best, dist) = obj.findClosest(v)
    w.println("best index :" + best)
    w.println("dist = " + dist)

    assert(best == 1)
  }

  test("Add method of Kmeans Object") {
    w.println("Sums for Add method")
    obj.add(v)
    val sums = obj.getSums
    val counts = obj.getCounts
    w.println(sums(0))
    w.println(sums(1))
    assert(counts(0) == 0)
    assert(counts(1) == 1)
    assert(sums(1).equals(v))
  }

  test("merge method of Kmeans Object") {
    w.println("merge method")
    obj.add(v)
    val y = (new KMeans(testMeans)).add(Vectors.dense(Array(1.5, 3.0, 4.5)))
    w.println("x counts " + obj.getCounts(0) + ", " + obj.getCounts(1))
    w.println("y counts " + y.getCounts(0) + ", " + y.getCounts(1))
    obj.merge(y)
    val s = obj.getSums
    val c = obj.getCounts
    w.println("after merge, x counts " + c(0) + ", " + c(1))
    w.println("Sums for Merge method")
    w.println(s(0))
    w.println(s(1))
    assert(c(0) == 1)
    assert(s(0).equals(Vectors.dense(Array(1.5, 3.0, 4.5))))
    assert(s(1).equals(Vectors.dense(Array(60.0, 2 * 51.0, 2 * 101.0))))


  }

  test("update method ") {
    val d = obj.update
    val s = obj.getSums
    val c = obj.getCounts
    val centers = obj.getCenters
    assert(s(0).equals(Vectors.zeros(3)))
    assert(c(0) == 0)
    assert(c(1) == 0)
    val resultMeans = Array(
      Vectors.dense(Array(1.5, 3.0, 4.5)),
      Vectors.dense(Array(30.0, 51.0, 101.0))
    )
    w.println("Difference in centers gives : " + d)
    w.println("new means are ")
    w.println(centers(0))
    w.println(centers(1))
    assert(centers(0).equals(resultMeans(0)))
    assert(centers(1).equals(resultMeans(1)))

  }

  test("Test Dense vector function ") {
    //prepare the training data
    val file = this.getClass.getResource("/data.txt").toURI.toString
    val input = sc.textFile(file, 5)
    denseVectorData = KMeansUtils.toDenseVector(input.cache())
    //has the write dimension
    assert(denseVectorData.first().size == 3)
  }

  test("Test Get Random Centers ") {
    model = new KMeansModel
    model.setTrainingData(denseVectorData)
    model.setCentersRandom
    var centers = model.getCenters
    w.println("The randonly generated centers are: ")
    w.println(centers(0))
    w.println(centers(1))
    assert(centers(0) != Vectors.zeros(3))
    assert(centers(1).size == denseVectorData.first().size)

  }

  test("Test compute K means ") {
    model.computeKMeans
    var centers = model.getCenters
    w.println("The k means which were computed are")
    w.println(centers(0))
    w.println(centers(1))
    assert(centers(0) != Vectors.zeros(3))
    assert(centers(1).size == denseVectorData.first().size)
  }

  test("Test the train method of K means model") {
    model.train(denseVectorData)
    var centers = model.getCenters
    w.println("The k means which were computed are")
    w.println(centers(0))
    w.println(centers(1))
    assert(centers(0) != Vectors.zeros(3))
    assert(centers(1).size == denseVectorData.first().size)
  }

  test("Counts method, returns counts of elements near each centroid ") {
    val counts = model.getCounts.collect()
    counts.foreach(p => w.println(p._1 + ", " + p._2))
    val all = counts.reduce((x, y) => ("all Vals", x._2 + y._2))
    assert(all._2 == 10.0)
  }

  //Sparse data tests

  test("Sparse Vector Conversion from array of doubles") {
    val file = this.getClass.getResource("/sparseData").toURI.toString
    val input = sc.textFile(file)
    val data = Array(0.0, 0.0, 0.0, 5.8, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0, 0.0)
    val sparseV = Converter.arrayToSparse(data)
    w.print("An array converted to the sparse vector ")
    w.println(sparseV)
    assert(sparseV.size == 12)
    val result = Vectors.sparse(12, Array(3, 7), Array(5.8, 0.1))
    assert(sparseV.equals(result))
  }
  test("Load in sparse vector data  ") {
    val s = new genSparse(50)
    val a = s.getAsArray(200)
    sparseData = sc.parallelize(a).map(v => Converter.arrayToSparse(v))
    w.println(sparseData.first())
    assert(sparseData.count == 200)
  }

  test("init random centers sparse ") {
    sparseModel = new KMeansModel
    sparseModel.setK(4).setTrainingData(sparseData).setCentersRandom
    assert(sparseModel.getCenters.length == 4)
  }

  test("computing k means on sparse data") {
    sparseModel.computeKMeans
    val sparseMeans = sparseModel.getCenters
    sparseMeans.foreach(v => w.println(v))
    assert(sparseMeans.length == 4)
    val zeroV = Vectors.zeros(50)
    //make sure that no all the means are zero
    assert(!Vectors.equals(zeroV))
  }

  /*
  The iris data set, which DB has included in this test package is a reasonable example case for clustering
  The k means implementation in r gives the following result according to:
   http://www.rdatamining.com/examples/kmeans-clustering
   for cluster means:
    Sepal.Length Sepal.Width Petal.Length Petal.Width
1     6.850000    3.073684     5.742105    2.071053
2     5.006000    3.428000     1.462000    0.246000
3     5.901613    2.748387     4.393548    1.433871

the counts for the three clusters are: 38, 50, 62
  */
  test("K means on iris data set ") {
    w.println("begining k means test on iris data set")
    //File writer for the performance test

    //values to be used in the test
    val k = 3
    val c = 0.001
    val maxIt = 100
    //The results from R
    val R_Result: Set[Array[Double]] = Set(Array(6.850000, 3.073684, 5.742105, 2.071053),
      Array(5.006000, 3.428000, 1.462000, 0.246000),
      Array(5.901613, 2.748387, 4.393548, 1.433871))
    val R_Counts: Array[Int] = Array(38, 50, 62)

    //set up the data as an array of vectors
    val file = this.getClass.getResource("/iris.csv").toURI.toString
    val input = sc.textFile(file, 5)
    val cachedRDD = input.cache()
    val data: RDD[Array[Double]] = cachedRDD.map(line => {
      val a = Array.ofDim[Double](4)
      val v: Array[String] = line.split(",")
      a(0) = v(0).toDouble
      a(1) = v(1).toDouble
      a(2) = v(2).toDouble
      a(3) = v(3).toDouble
      a
    })
    var data2 = data.map(a => Vectors.dense(a))
    //bench mark1: how long does it take to initialize the model

    var model: KMeansModel = new KMeansModel
    model.setParams(k, c, maxIt, 42)

    model.train(data2)

    val myResult = model.getCenters
    var myResultArray = myResult.map(v => v.toArray)
    //    //check that for each element in myResult, an identical array exists in the RSet
    //    //since the Rset is the same size, this will be equal to set equality.
    val margin = 1.0
    var fuzzy = myResultArray.forall(element => R_Result.exists(setElement => DoubleCompare.fuzzyCompareArray(element, setElement, margin)))
    if (fuzzy) w.println("The results of Rachel's Kmean algorithm are within " + margin + " of the results of the R implementation of K means on the iris data set")
    //val counts = trained.getCounts
    else {
      w.println("The results of Rachel's algorithm are not within " + margin + " of the R keamns algorithm.  ")
      w.println("Rachel's results are : " + myResultArray.foreach(r => println(r)))
    }
    assert(fuzzy)
    //check that the cluster counts work
    val clusterCounts = model.getCounts
    val myCounts = clusterCounts.collect()
    myCounts.foreach(x => w.println(x._1 + " , " + x._2))
    val mySorted : Array[Int] = myCounts.map( x => x._2).sorted

    val rSorted = R_Counts.sorted
    //assert that the sizes of each cluster corresponds within 1.0
    w.println(mySorted(0))
    assert(abs(mySorted(0) - rSorted(0)) < 3)
    assert(abs(mySorted(1) - rSorted(1)) <  3)
  }

  test("Bench mark tests on Iris Data Set") {
    w.println("Begining K Means test on iris data set at : " + System.currentTimeMillis())
    var counts = 0
    var t1: Long = 0
    var t2: Long = 0

    val file = this.getClass.getResource("/iris.csv").toURI.toString
    val input = sc.textFile(file, 5)
    val cachedRDD = input.cache()
    val data: RDD[Array[Double]] = cachedRDD.map(line => {
      val a = Array.ofDim[Double](4)
      val v: Array[String] = line.split(",")
      a(0) = v(0).toDouble
      a(1) = v(1).toDouble
      a(2) = v(2).toDouble
      a(3) = v(3).toDouble
      a
    })
    //convert to sparse data
    var data2: RDD[Vector] = null
    counts = 0
    while (counts < 1000) {
      data2 = KMeansUtils.toSparseVector(data)
      counts += 1
    }
    t1 = System.currentTimeMillis()
    counts = 0
    while (counts < 1000) {
      data2 = KMeansUtils.toSparseVector(data)
      counts += 1
    }
    t2 = System.currentTimeMillis()
    w.println("benchmark2: time to convert to sparse vector RDD " + (t2 - t1))

    data2 = KMeansUtils.toSparseVector(data)

    val model2 = new KMeansModel
    model2.setParams(3, 0.001, 1000, 5L)
    model2.setTrainingData(data2)
    counts = 0
    while (counts < 1000) {
      model2.setCentersRandom
      counts += 1
    }
    t1 = System.currentTimeMillis()
    counts = 0
    while (counts < 1000) {
      model2.setCentersRandom
      counts += 1
    }
    t2 = System.currentTimeMillis()
    w.println("benchmark3: time to initialize centers " + (t2 - t1))

    val model3 = model
    counts = 0
    while (counts < 1000) {
      model3.computeKMeans
      counts += 1
    }
    counts = 0
    t1 = System.currentTimeMillis()
    while (counts < 1000) {
      model3.computeKMeans
      counts += 1
    }
    t2 = System.currentTimeMillis()
    w.println("benchmark4 : computing k means " + (t2 - t1))
    w.println("Performance tests complete ")
  }
}
