package com.dbtsai.spark

//import ...
//import org.apache.spark.mllib.linalg._
//import org.apache.spark.mllib.linalg.{SparseVector, DenseVector}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.Vector
import math._
import util._

/**
 * Created by rachelwarren on 4/12/15.
 */
/*
* This class extends serializable and implements the finding the means part of
* k means, the repeated iterations will have to happen in the dirver
* do i want the param to be the k or the array of clusters
 */
class KMeans(centroids : Array[Vector]) extends Serializable {
  private val k : Int  = centroids.length
  private val firstRow : Vector = centroids(0)
  private val dim : Int = firstRow.length
  private var c : Array[Vector] = centroids
  private val zeroVector = Vector(Array.fill(dim)(0.0))
  private var sums : Array[Vector] =  Array.fill(k)(zeroVector)
  private var counts : Array[Vector] =  Array.fill(k)(zeroVector)


  def print = {
    var i = 0
    while(i < this.k){
      val s = "centroid " + i + ": " + centroids(i)
//      var j = 0
//      var temp = centroids(i)
//      while(j< this.dim){
//        s = s + temp(j) + ", "
//        j = j + 1
//      }
      println(s)
      i = i + 1
    }
  }

  def printSums = {
    println(this.sums.deep.mkString("\n"))
  }

  /*
  *stolen from
  * http://stackoverflow.com/questions/28949591/easiest-way-to-represent-euclidean-distance-in-scala
  *
   */

//  def sqdist(v1: Vector, v2: Vector) : Double = {
//
//
//
//  }

  def add(v: Vector): this.type = {
    var bestIndex = 0
    var closest = Double.MaxValue

    var i = 0
    while( i < this.k) {

      val tempD = this.c(i).squaredDist(v)
      if (tempD < closest) {
        bestIndex = i
        closest = tempD
      }
      i +=1
    }
    i = 0
    this.sums(bestIndex) = this.sums(bestIndex).add(v)
    this.counts(bestIndex) = this.counts(bestIndex).add(new Vector(Array.fill[Double](dim)(1.0)))
//    while (i < this.dim) {
//      this.sums(bestIndex)(i) = this.sums(bestIndex)(i)+ v(i)
//      this.counts(bestIndex)(i) = this.counts(bestIndex)(i) + 1
//      i +=1
//    }
    this
  }

  def merge(that: KMeans): this.type = {
    //replace this for loop with something less ugly

    var i = 0
//    while( i < this.k){
//      var j = 0
//      while( j < this.dim){
//        this.sums(i)(j) = this.sums(i)(j) + that.sums(i)(j)
//        this.counts(i)(j) = this.counts(i)(j) + that.counts(i)(j)
//        j +=1
//      }
//      i += 1
//    }
    while (i < this.k){
      this.sums(i) = this.sums(i).add(that.sums(i))
      this.counts(i) = this.counts(i).add(that.counts(i))
      i +=1
    }
    this
  }

  def update: Double = {
    var diff = 0.0
    var i  = 0
    while(i < this.k) {
      val m  = (Array.fill[Double](dim)(0.0))
      var j = 0
      val sTemp = this.sums(i)
      val cTemp = this.counts(i)
      while (j < this.dim) {
        m(j) = if (cTemp(j) > 0.0)  sTemp(j) / cTemp(j) else 0.0
        j = j + 1
    }
      val center = Vector(m)
      diff = diff + center.squaredDist(centroids(i))
      //update the centroids array
      centroids(i) = center
      i = i + 1
    }
    //zero out the sum and count arrays
    this.sums =  Array.fill(k)(new Vector(Array.fill(dim)(0.0)))
    this.counts =  Array.fill(k)(new Vector(Array.fill(dim)(0.0)))
    diff
  }
  def getCenters : Array[Vector] = this.c
}

class KMeansModel {
   var k : Int = 2
   var convergence : Double = 0.01
   var maxItterations : Int = 1000
   var seed : Long = 42
   var cols : Int = 5
  //uses the k-means ++ implementation given in the wikipedia article
  //http://en.wikipedia.org/wiki/K-means%2B%2B and implemented in ml lib
  def setParams(kVal : Int, c : Double,  maxIt : Int, s : Long) = {
    this.k = kVal
    this.convergence = c
    this.maxItterations = maxIt
    this.seed = s
  }
  /*
  utility method which given RDD input as string, converts it to a vector array of double s
   */
  def toDenseVector(input : RDD[String] )  = {
    val cachedRDD = input.cache()
    val vectors = cachedRDD.map(
    line => new Vector(line.split(",").map(_.toDouble))
    )
    vectors

  }
//  def toSparseVector(cachedRDD : RDD[String]) = {
//    val col = cachedRDD.first().split(",").map(_.toDouble).length
//    val vectors = cachedRDD.map(
//      line =>  {
//        val a =  line.split(",").map(_.toDouble)
//        new SparseVector(col, a.indices, a)
//      }
//    )
//    vectors
//  }

  def getCentersRandom(input : RDD[Vector]): Array[Vector] = {
    val cachedRDD = input.cache()
    this.cols = cachedRDD.first().length
    //chose centers randomly
   cachedRDD.takeSample(false, k, seed)

  }

  def computeKMeans(cachedRDD : RDD[Vector], centroids : Array[Vector])= {
    var m = new KMeans(centroids)
    var distance = Double.MaxValue
    val max_itt = 100
    var itt = 0
    while (distance > convergence && itt < max_itt) {
      val next = cachedRDD.aggregate(m)(
        (agg: KMeans, v: Vector) => agg.add(v),
        (a: KMeans, b: KMeans) => a.merge(b)
      )
      distance = next.update
      m = next
      itt +=1
    }
    //logging information
    if (distance > convergence) {
      println("Warning, algorithm did not reach convergence threashold, " + convergence + ", in " + max_itt + "itterations. Distance = " + distance)
    }
    else {
      println("K Means Random Completed, in " + itt + " iterations.")
    }
    //return the new centers
    m.print
    m.getCenters
  }


}
// object KMeansModel{
//   def apply(k, c, maxIt, s, input) = {
//     val model = new KMeansModel
//     model.setParams(k, c, maxIt, s)
//     val centers = model.getCentersRandom(input)
//     val means = model.computeKMeans(input, centers)
//   }
// }
