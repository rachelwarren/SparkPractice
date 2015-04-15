package com.dbtsai.spark

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vectors, KMeans, KMeansModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import math._
import util._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


/*
Utility methods for comparing arrays within a given margin
 */
object DoubleCompare {
  def fuzzyCompareDouble(a : Double, b: Double , margin : Double) : Boolean = (abs(a - b) < margin)

  def fuzzyCompareArray(a : Array[Double], b : Array[Double], margin : Double) : Boolean = {
    if (a.length != b.length)  false else {
      var i = 0
      var bool = true
      while(i < a.length && bool) {
        bool = fuzzyCompareDouble(a(i), b(i), margin)
        i +=1
      }
      bool
    }
  }

}
class ComputeMeanSuites extends FunSuite with BeforeAndAfterAll {
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

  test("Test on simple data") {
    println("Beginning simple K means test ")
    val k = 7
    val convergence = 1.0
    val file = this.getClass.getResource("/data.txt").toURI.toString
   val input = sc.textFile(file, 5)

    //parse the data in the file to doubles, and then use the take sample function to select k random start points
   // val labeleddata = MLUtils.loadLibSVMFile(sc,"/src/test/resources/data.txt" , 3)
   // val data = labeleddata.map( l => l.features )
    val model = new KMeansModel(k, convergence, 100)
    val data = model.toDenseVector(input)
    val cols = data.first().size
    val centers = model.getCentersRandom(data)
    val result = model.computeKMeans(data, centers)
    val counts = model.counts(data, result)
    counts.foreach( p => println(p._1 + ", " + p._2))
    val all = counts.reduce( (x, y) => ("all Vals", x._2 + y._2 ))
 //check that not all of the centroids are set equal to zero
    assert(!result.forall(v => v.equals(Vectors.zeros(cols))))
    //check that every row in the data set was assigned to some center
    assert(all._2 == 10.0)
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

   */
  test("K means on iris data set "){
    println("begining k means test on iris data set" )
    val k = 3
    val c = 0.001
    val maxIt = 100
    val R_Result : Set[Array[Double]] = Set(Array(6.850000, 3.073684 , 5.742105, 2.071053) ,
      Array( 5.006000, 3.428000, 1.462000, 0.246000),
       Array(5.901613, 2.748387, 4.393548, 1.433871))
    val file = this.getClass.getResource("/iris.csv").toURI.toString
    val input = sc.textFile(file, 5)
    val cachedRDD = input.cache()
    val data : RDD[Array[Double]] = cachedRDD.map( line => {
      val a = Array.ofDim[Double](4)
      val v : Array[String] = line.split(",")
      a(0) = v(0).toDouble
      a(1) = v(1).toDouble
      a(2) = v(2).toDouble
      a(3) = v(3).toDouble
      a
    }  )

    val model = new KMeansModel(k, c, maxIt)
    val data2 = model.toSparseVector(data)
    val centers = model.getCentersRandom(data2)
    val means = model.computeKMeans(data2, centers)
    val myResult = means.map(v => v.toArray)
    //check that for each element in myResult, an identical array exists in the RSet
    //since the Rset is the same size, this will be equal to set equality.
    val margin = 0.05
    var fuzzy = myResult.forall(element => R_Result.exists( setElement => DoubleCompare.fuzzyCompareArray(element, setElement, margin)))
    if( fuzzy) println("The results of Rachel's Kmean algorithm are within " + margin + " of the results of the R implementation of K means on the iris data set")
    else {  println("The results of Rachel's algorithm are not within " + margin + " of the R keamns algorithm.  ") ;  println("Rachel's results are : " + myResult.foreach( r => println(r))) }
    assert(fuzzy) }
  }



