package com.dbtsai.spark

//import ...

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
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
class KMeans(centroids : Array[Array[Double]]) extends Serializable {
  private val k : Int  = centroids.length
  private val firstRow : Array[Double] = centroids(0)
  private val dim : Int = firstRow.length
  private var c : Array[Array[Double]] = centroids
  private var sums =  Array.fill(k,dim)(0.0)
  private var counts = Array.fill(k, dim)(0)

  def print = {
    var i = 0
    while(i < this.k){
      var s = "centroid " + i + ": "
      var j = 0
      var temp = centroids(i)
      while(j< this.dim){
        s = s + temp(j) + ", "
        j = j + 1
      }
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
  def distance(xs: Array[Double], ys: Array[Double]) = {
    sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def add(v: Array[Double]): this.type = {
    var bestIndex = 0
    var closest = Double.MaxValue

    var i = 0
    while( i < this.k) {
      val tempD = distance(this.c(i), v)
      if (tempD < closest) {
        bestIndex = i
        closest = tempD
      }
      i +=1
    }
    i = 0
    while (i < this.dim) {
      this.sums(bestIndex)(i) = this.sums(bestIndex)(i)+ v(i)
      this.counts(bestIndex)(i) = this.counts(bestIndex)(i) + 1
      i +=1
    }
    this
  }

  def merge(that: KMeans): this.type = {
    //replace this for loop with something less ugly

    var i = 0
    while( i < this.k){
      var j = 0
      while( j < this.dim){
        this.sums(i)(j) = this.sums(i)(j) + that.sums(i)(j)
        this.counts(i)(j) = this.counts(i)(j) + that.counts(i)(j)
        j +=1
      }
      i += 1
    }
    this
  }

  def update: Double = {
    var diff = 0.0
    var i  = 0
    while(i < this.k) {
      val m = Array.fill[Double](dim)(0.0)
      var j = 0
      val sTemp = this.sums(i)
      val cTemp = this.counts(i)
      while (j < this.dim) {
        m(j) = if (cTemp(j) == 0) 0 else sTemp(j) / cTemp(j)
        j = j + 1
      }
      diff = diff + distance(m, centroids(i))
      //update the centroids array
      centroids(i) = m
      i = i + 1
    }
    //zero out the sum and count arrays
    this.sums =  Array.fill(k,dim)(0.0)
    this.counts = Array.fill(k, dim)(0)
    diff
  }
  def getCenters : Array[Array[Double]] = this.c
}

object GenerateKMeans {
  //uses the k-means ++ implementation given in the wikipedia article
  //http://en.wikipedia.org/wiki/K-means%2B%2B and implemented in ml lib
  def KMeansRandom(input : RDD[String], k : Int, c : Double): Array[Array[Double]] = {
    val cachedRDD = input.cache()
    val cols = cachedRDD.first().split(",").map(_.toDouble).length
    //chose centers randomly
    var centroids = cachedRDD.takeSample(false, k, 43).map(_.split(",").map(_.toDouble))

    var m = new KMeans(centroids)
    var distance = Double.MaxValue
    val max_itt = 100
    var itt = 0
    while (distance > c && itt < max_itt) {
      val next = cachedRDD.map(_.split(",").map(_.toDouble)).aggregate(m)(
        (agg: KMeans, v: Array[Double]) => agg.add(v),
        (a: KMeans, b: KMeans) => a.merge(b)
      )
      distance = next.update
      m = next
      itt +=1
    }
    //logging information
    if (distance > c) {
      println("Warning, algorithm did not reach convergence threashold, " + c + ", in " + max_itt + "itterations. Distance = " + distance)
    }
    else {
      println("K Means Random Completed, in " + itt + " iterations.")
    }
    //return the new centers
    m.getCenters
  }


}
