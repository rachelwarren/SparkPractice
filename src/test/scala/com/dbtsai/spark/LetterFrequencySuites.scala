package com.dbtsai.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

// http://en.wikipedia.org/wiki/Letter_frequency
class LetterFrequencySuites extends FunSuite with BeforeAndAfterAll {
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

  test("Letter frequency") {
    val file = this.getClass.getResource("/revenge.txt").toURI.toString
    val input = sc.textFile(file, 5)

    val lf1 = LetterFrequency.computeLF1(input).toList.sortBy(-_._2)
    assert(lf1(0)._1 == 'e' && lf1(0)._2 == 14385L)
    assert(lf1(1)._1 == 't' && lf1(1)._2 == 9054L)
    assert(lf1(2)._1 == 'a' && lf1(2)._2 == 8573L)
    assert(lf1(3)._1 == 'h' && lf1(3)._2 == 6960L)
    assert(lf1(4)._1 == 'o' && lf1(4)._2 == 6823L)

    val lf2 = LetterFrequency.computeLF2(input).toList.sortBy(-_._2)
    assert(lf2(0)._1 == 'e' && lf2(0)._2 == 14385L)
    assert(lf2(1)._1 == 't' && lf2(1)._2 == 9054L)
    assert(lf2(2)._1 == 'a' && lf2(2)._2 == 8573L)
    assert(lf2(3)._1 == 'h' && lf2(3)._2 == 6960L)
    assert(lf2(4)._1 == 'o' && lf2(4)._2 == 6823L)
  }

}
