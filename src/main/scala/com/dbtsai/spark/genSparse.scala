package com.dbtsai.spark

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by rachelwarren on 4/16/15.
 */
class genSparse(dim : Int) {
  val fileName = "sparseDataSample.txt"

  val R = new Random
  var Rows = 0
  var Sums = Array.fill[Double](dim)(0.0)
  val density = 0.01

  def getNext: Double ={
    val d = R.nextDouble()
    if (d < density){
      val l = R.nextInt()
      d*l
    } else 0.0
  }

  def nextRow : String = {
    var d = 0
    var v = getNext
    var line = v.toString
    Sums(0) += v
    while (d < dim){
      v = getNext
      Sums(d) += v
      line = line + ", " + v
      d +=1
    }
    line
  }

  def write( maxRows : Int): Unit = {
    val fw = new FileWriter(fileName, true)
    val pw = new PrintWriter(fw)
    var r = 0
    while (r < maxRows){
      pw.println(nextRow)
      r +=1
    }
    Rows += maxRows
    pw.close()
    fw.close()
  }
  def getAsArray(maxRows :Int): Array[Array[Double]] = {
    val data = Array.ofDim[Array[Double]](maxRows)
    var r = 0
    while(r < maxRows){
      data(r) = Array.fill[Double](dim)(this.getNext)
      r += 1
    }
    data
  }

  def getAverage : Array[Double] = {
    Sums.foreach( v => v/Rows)
    Sums
  }
}


