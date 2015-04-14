//package com.dbtsai.spark
package org.apache.spark.mllib.linalg
//import ...
//import org.apache.spark.mllib.linalg._

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{BLAS, SparseVector, DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.Vector
import math._
import scala.collection.mutable.ArrayBuffer
import util._

/**
 * Created by rachelwarren on 4/12/15.
 */
/*
Converter object is needed to map RDD to sparse arrays
 */
object converter extends Serializable {
  def denseToSparse(v : Vector) : Vector  = {
    var pairs : List[(Int, Double)]= Nil
    v.foreachActive((index : Int, value: Double) => if(value > 0.0) pairs = (index, value) :: pairs )
    Vectors.sparse(v.size, pairs)
  }
}
/*
* This class extends serializable and implements the finding the means part of
* k means, the repeated iterations will have to happen in the dirver
* do i want the param to be the k or the array of clusters
 */
class KMeans(centroids : Array[Vector]) extends Serializable {
  private val k : Int  = centroids.length
  private val firstRow  = centroids(0)
  private val dim : Int = firstRow.size
  private var c  =  centroids
  private var sums : Array[Vector] =  Array.fill(k)(Vectors.zeros(dim))
  private var counts : Array[Vector] =  Array.fill(k)(Vectors.zeros(dim))

  def print = {
    var i = 0
    while(i < this.k){
      val s = "centroid " + i + ": " + c(i)
      println(s)
      i = i + 1
    } }

  def printSums = {
    println(this.sums.deep.mkString("\n"))
  }

  /*
   @param v: a Vector, can be sparse or dense
   @ return (index, distance)
   where index = the index of the centroid closest to v and distance is distance between v and that centroid
    distance computed using the private sqdist computation in the Mlib vectors library.
   */
  def findClosest(v: Vector) : (Int , Double) = {
  var bestIndex = 0
  var closest = Double.MaxValue

  var i = 0
  while( i < this.k) {
    val tempD = Vectors.sqdist(this.c(i), v)
    if (tempD < closest) {
      bestIndex = i
      closest = tempD
    }
    i +=1
  }
  (bestIndex, closest)
}

  def add(v: Vector): this.type = {

    val (bestIndex, closest) = findClosest(v)
    linalg.BLAS.axpy(1.0, v, sums(bestIndex))

    //update the counts, this is a little more complicated
    var pairs : List[(Int, Double)] = Nil
    this.sums(bestIndex).foreachActive( (index, value) => {
      val v = counts(bestIndex).apply(index) + 1
              pairs = (index, v) :: pairs }
            )
    counts(bestIndex) = Vectors.sparse(this.dim, pairs)
    println("printing the counts")
    println( this.counts(bestIndex) )
    this
  }

  def merge(that: KMeans): this.type = {
    var i = 0
    while (i < this.k) {
      linalg.BLAS.axpy(1.0, that.sums(i), this.sums(i))
      linalg.BLAS.axpy(1.0,  that.counts(i), this.counts(i))
      i +=1
    }
      this
  }
    def getCenters  = this.c
/*
 To be called at the end of each K means itteration
 It sets the value of this.centroids to the new means, by using the sum and count fields to find the mean
 Returns the sum of the square distance between each new centroid and each previous centroid
 Once finished, sets the values of sum and count equal to the zero vector
 */
  def update: Double = {
    var diff = 0.0
    var i  = 0
    while(i < this.k) {
      var center_values : List[(Int, Double)] = Nil
      var j = 0
      val sTemp = this.sums(i)
      val cTemp = this.counts(i)
      while (j < this.dim) {
        if (cTemp(j) > 0.0) {
         center_values = (j, sTemp(j) / cTemp(j)) :: center_values
        }
        j = j + 1
    }
      val nextCenter = Vectors.sparse(dim, center_values)
      diff = diff + Vectors.sqdist(c(i), nextCenter)
      c(i) = nextCenter
      i = i+1
    }
    //zero out the sum and count arrays
    this.sums =  Array.fill(k)(Vectors.zeros(dim))
    this.counts =  Array.fill(k)(Vectors.zeros(dim))
  //return the difference in distance between previous centers and these centers
    diff
  }

}

class KMeansModel(kVal: Int, cVal: Double, MaxIt: Int) {
   var k : Int = kVal
   var convergence : Double = cVal
   var maxItterations : Int = 1000
   var seed : Long = 42

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
    line => Vectors.dense(line.split(",").map(_.toDouble))
    )
    vectors
  }

  def toSparseVector(input : RDD[String], kMeans: KMeans) = {
    val cachedRDD = input.cache()

    cachedRDD.map (
     line => converter.denseToSparse(Vectors.dense(line.split(",").map(_.toDouble)))
     )
  }
  //chose centers randomly
  def getCentersRandom(input : RDD[Vector]): Array[Vector] = {

       input.takeSample(false, k, seed)

  }

  def computeKMeans(cachedRDD : RDD[Vector], centroids : Array[Vector])= {
    var m = new KMeans(centroids)
    //cachedRDD.map(v => m.denseToSparse(v))
    var distance = Double.MaxValue
    var itt = 0
    while (distance > convergence && itt < this.maxItterations) {
      val next = cachedRDD.aggregate(m)(
        (agg: KMeans, v: Vector) => agg.add(v),
        (a: KMeans, b: KMeans) => a.merge(b)
      )
      distance = next.update
      m = next
      next.print
      itt +=1
    }
    //logging information
    if (distance > convergence) {
      println("Warning, algorithm did not reach convergence threshold, " + convergence + ", in " + maxItterations + "itterations. Distance = " + distance)
    }
    else {
      println("K Means Random Completed, in " + itt + " iterations.")
    }
    //return the new centers
    m.getCenters
  }

  def labelData(cachedRDD : RDD[Vector], centroids : Array[Vector]) = {
    val m = new KMeans(centroids)
    cachedRDD.map(
     v => LabeledPoint(m.findClosest(v)._1, v))

  }

//  def createPairs(cachedRDD : RDD[Vector], centroids : Array[Vector]) : RDD[(String, Double)] = {
//    val m = new KMeans(centroids)
//    cachedRDD.map( v => (m.findClosest(v)._1.toString, 1.0) )
//  }

  def counts(cachedRDD : RDD[Vector], centroids : Array[Vector]) = {
    val m = new KMeans(centroids)
    cachedRDD.map( v => (m.findClosest(v)._1.toString, 1.0) ).reduceByKey( (a, b) => a + b)
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
