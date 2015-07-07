package com.dbtsai.spark

import org.apache.spark.rdd.RDD


class TTest(private val keys : Array[String]) extends Serializable{
  private var counts = Array.fill[Long](keys.length)(0)
  private var sums = Array.fill[Double](keys.length)(0)
  private var means = Array.fill[Double](keys.length)(0)
  private var variance = Array.fill[Double](keys.length)(0)
  private var meansComputed = false


  def addMeans(x : (String, Double)) : this.type = {
    val i = keys.indexOf(x._1)
    sums(i) += x._2
    counts(i) += 1
    this
  }
  def mergeMeans(that : TTest) : this.type = {
    var i = 0
    while(i < keys.length){
      this.sums(i) += that.sums(i)
      this.counts(i) += that.counts(i)
      i +=1
    }
    this
  }

  def computeMean : this.type = {
    var i = 0
    while(i < keys.length){
      means(i) = sums(i) / counts(i)
      i +=1
    }
    this.meansComputed = true
    this
  }
  def addVariance(x : (String, Double)) : this.type = {
    if (meansComputed) {
      val i = keys.indexOf(x._1)
      val diff = (means(i) - x._2)
      variance(i) += diff * diff
      this
    }
    else{
      throw new Exception("variance cannot be computed without first computing mean")
    }
  }

  def mergeVariance(that : TTest) : this.type = {
    var i = 0
    while(i < keys.length){
      this.variance(i) += that.variance(i)
      i +=1
    }
    this
  }

  def computeT(i : Int, j : Int) = {
    (means(i) - means(j))/( variance(i)/counts(i) - variance(j)/counts(j))
  }

  def calculateTStat : Array[Array[Double]] = {
    if (keys.length == 2){
      Array(Array(computeT(0,1)))
    }
    else {
      val a = Array.ofDim[Array[Double]](keys.length - 1)
      var i = 0
      while (i < a.length){
        val b = Array.ofDim[Double](keys.length -1)
        var j = i
        while( j < b.length){
          b(j) = computeT(i, j)
          j += 1
        }
        a(i) = b
        i+=1
      }
      a
    }
  }

  def listResults: List[String] = {
    var s = "Group, Mean, Variance, T-score"
    var l = List(s)
    var i = 0
    while(i < keys.length){
      s = keys(i) + ", " + means(i) + ", " + variance(i)
      l = s :: l
      i +=1
    }
    s = "T-Score is " + computeT(0,1)
    s :: l
  }
  //getter methods
  def getCounts = this.counts
  def getMeans = this.means
  def getVar = this.variance
  def getSums = this.sums
  def getKeys = this.keys

}

class TTestSparkPlugin{
  //filter gets rid of missing values and returns key value pairs
  def filter( indexIV : Int, indexDV: Int, ivValues : Array[String] , inputTextRDD : RDD[String]) : RDD[(String, Double)] = {
      inputTextRDD.map(
    line => {
      val array = line.split(", ")
      (array(indexIV), array(indexDV).toDouble)
    }
    )
  }

}