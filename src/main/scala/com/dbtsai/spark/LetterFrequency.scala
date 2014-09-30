package com.dbtsai.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.Map


object LetterFrequency {

  def computeLF1(input: RDD[String]): Map[Char, Long] = {
    input.flatMap(line => {
      line.toCharArray().filter(_.isLetter).map(char => (char.toLower, 1L))
    }).reduceByKey(_ + _).collectAsMap()
  }

  def computeLF2(input: RDD[String]): Map[Char, Long] = {
    /*
    input.mapPartitions(iter => {
      val charMap = scala.collection.mutable.Map[Char, Long]()
      for (line <- iter) {
         ???
      }
      Iterator(charMap)
    }).reduce((map1, map2) => {
         ???
    })
    */
    computeLF1(input)
  }
}
