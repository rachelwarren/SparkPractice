
package org.apache.spark.mllib.linalg

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{BLAS, SparseVector, DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by rachelwarren on 4/12/15.
 */

/**
Utility object for converting from an abstract vector type (dense or sparse) to sparse vector type
  */
object Converter extends Serializable {
  def arrayToSparse(a: Array[Double]): Vector = {
    val len = a.length
    var n = 0
    val indices: Array[Int] = Array.ofDim[Int](len)
    val values: Array[Double] = Array.ofDim[Double](len)
    while ( n < len) {
      if ( a(n) >0.0 || n < 0.0) {
        values(n) = a(n)
        indices(n) = n
      }
      else{
        values(n) = 0.0
        indices(n) = -2
      }
      n += 1
    }

    val v2 = values.filter(v => (v > 0.0 || v < 0.0))
    val i2 = indices.filter(v => v > -1)
    Vectors.sparse(len, i2, v2)
  }
}

/**
 * A KMeans object, represent the functions called within the outer loop of the Kmeans algorithm.
 * In particular it contains add and merge methods which can be called by the spark aggregate transformation
 *
 * @constructor create a new itteration fo the k means algorithm with the starting means
 * @param c current estimation of the means. An array of vectors of length K
 *          where each vector represents one center with the same dimensionality as the data
 */
class KMeans(private val c: Array[Vector]) extends Serializable {
  //static values
  private val k: Int = c.length
  private val dim: Int = c(0).size

  //class variables

  //the means used in this iteration
  private var sums: Array[Vector] = Array.fill(k)(Vectors.zeros(dim))
  //running sum of elements associated with each cluster
  private var counts: Array[Long] = Array.ofDim(dim)

  /**
   * Returns the index of the closest mean and the distance to that mean from a given vector.
   * Squared distance between vectors computed using the private sqdist computation in the Mlib vectors library.
   *
  @param v: a Vector, can be sparse or dense
  @return (index, distance)
   where index = the index of the centroid closest to v and distance is distance between v and that centroid

   */
  def findClosest(v: Vector): (Int, Double) = {
    var bestIndex = 0
    var closest = Double.MaxValue
    var i = 0
    while (i < this.k) {
      val tempD = Vectors.sqdist(this.c(i), v)
      if (tempD < closest) {
        bestIndex = i
        closest = tempD
      }
      i += 1
    }
    (bestIndex, closest)
  }

  /**
   * Returns an updated Kmeans object given a singled observation.
   * Given a vector finds the closest centroids and updates the sums and counts field.
   * For spark transformation step,
   * @param v-an observation to compare to current means
   * @return KMeans object whith sums and means field updated accordingly
   */
  def add(v: Vector): this.type = {
    val (bestIndex, closest) = findClosest(v)
    linalg.BLAS.axpy(1.0, v, sums(bestIndex))
    counts(bestIndex) += 1
    this
  }

  /**
   * adds the sums and counts of this Kmeans object with those in that Kmeans object
   * For spark action step
   *
   * @param that the other Kmeans object to merge
   * @return
   */
  def merge(that: KMeans): this.type = {
    var i = 0
    while (i < this.k) {
      linalg.BLAS.axpy(1.0, that.sums(i), this.sums(i))
      this.counts(i) = this.counts(i) + that.counts(i)
      i += 1
    }
    this
  }

  /*

   */
  /** To be called at the end of each K means itteration
   Updates the value of current means by dividing the sums field by the corresponding counts.
   Returns the sum of the square distance between each new centroid and each previous centroid and
   Once finished, sets the values of sum and count equal to the zero vector.
    *
    * @return the total distance between the new means and means given in the previous itteration of the kmeans algorithm
    */
  def update: Double = {
    var diff = 0.0
    var i = 0
    while (i < this.k) {
      // divide sums by counts (so sums will be means) as long as count is not zero
      if (this.counts(i) > 0) linalg.BLAS.scal(1.0 / this.counts(i), this.sums(i))
      diff = diff + Vectors.sqdist(c(i), this.sums(i))
      c(i) = this.sums(i)

      //zero out the sum and count arrays
      this.sums(i) = Vectors.zeros(dim)
      this.counts(i) = 0
      i = i + 1
    }
    //return the difference in distance between previous centers and these centers
    diff
  }

  //getter methods
  /**
   * @return The running total of the sums for values associated with each center for this Kmeans object
   */

  def getSums = this.sums

  /**
   * @return The current number of values associated with each center for this Kmeans object
   */
  def getCounts = this.counts

  def getCenters = this.c

  def getK = this.k

  /**
   * Prints each centroid associated with this KMeans object
   */
  def print = {
    var i = 0
    while (i < this.k) {
      val s = "centroid " + i + ": " + c(i)
      println(s)
      i = i + 1
    }
  }
}

/**
 * A full Kmeans model. Given RDD data from the user in the form of a vector of doubles, has methods to train a new
 * model and classify new data according to the means derived from the training data.
 * Defualt parameters are K=2, maximum iterations for the inner while loop = 1000 and the value used to determine
 * convergence of means = 0.001.
 *
 * Currently the initial centers can only be chosen randomly.
 *NOTE: The counts method is broken, I think the problem is that the map transformation needs access to the KMeans object
 * to compute distance, and I can't seem to correctly configure that broadcast variable
 */
class KMeansModel() extends Serializable {
  //model parameters, set to their default values.
  var k: Int = 2
  var convergence: Double = 0.001
  var maxItterations: Int = 1000
  var seed: Long = 42
  var trained: Boolean = false
  private var centers: KMeans = null
  private var input: RDD[Vector] = null
  private var log : Boolean = false

  /**
   * A quick method to set all of the models variable parameters.
   * @param kVal -the number of clusters
   * @param c - the maximum tolerance for convergence of the means
   * @param maxIt - the number of iterations to be run regardless of convergence to c
   * @param s - the seed for the random number generator used in the means initialization
   * @return the updated instance of this model
   */
  def setParams(kVal: Int, c: Double, maxIt: Int, s: Long): this.type = {
    this.k = kVal
    this.convergence = c
    this.maxItterations = maxIt
    this.seed = s

    this
  }

  /**
   * Set loging information. If true, will print out information about model run
   */
 def setLogging(l : Boolean ): this.type = {
    this.log = l
    this
 }
  /**
   * Set K
   * @param kVal the desired number of clusters
   * @return an updated instance of this model
   */
  def setK(kVal: Int): this.type = {
    this.k = kVal
    this
  }

  /**
   * Set the threshold  for convergence of means between iterations
   * @param c new threshold
   * @return
   */
  def setConvergence(c: Double): this.type = {
    this.convergence = c
    this
  }

  /**
   * Set maximum allowable iterations to run in determining optimal means.
   * @param m
   * @return
   */
  def setMaxIt(m: Int): this.type = {
    this.maxItterations = m
    this
  }

  /**
   *
   * @param s new seed for the random number generator
   * @return
   */
  def setSeed(s: Long): this.type = {
    this.seed = s
    this
  }

  def setTrainingData(data: RDD[Vector]): this.type = {
    this.input = data
    this
  }

  /**
   * uses the initial means by a random sweep of the training data.
   * Uses the training data associated with this model. Returns and exception if training data has not been set
   * @return an updated instance of this model with the initial means set
   */
  def setCentersRandom: this.type = {
    if (this.input == null) {
      throw new Exception("no input data")
    }
    else {
      val samples = input.takeSample(false, k, seed)
      this.centers = new KMeans(samples)
      this
    }
  }

  /**
   * computer the Kmeans using whichever means are set to the centers field and this models training data.
   * Eventually should  be private.
   * @return an instance of this model with means set to the optimal means according  to the training data
   */
  def computeKMeans : this.type = {
    if (this.centers == null) {
      throw new Exception("no input data")
    }
    else {
      var distance = Double.MaxValue
      var itt = 0
      while (distance > convergence && itt < maxItterations) {
        val next = input.aggregate(centers)(
          (agg: KMeans, v: Vector) => agg.add(v),
          (a: KMeans, b: KMeans) => a.merge(b)
        )
        distance = next.update
        centers = next
        itt += 1
      }
      //logging information
      if (log) {
        if (distance > convergence) {
          println("Warning, algorithm did not reach convergence threshold, " + convergence +
            ", in " + maxItterations + "itterations. Distance = " + distance)
        }
        else {
          println("K Means Random Completed, in " + itt + " iterations.")
        }
      }
      trained = true
      this
    }
  }



  /**
   * Given RDD data to train on finds the value of K means according to this models parameteres.
   * Initialized the means by randomly choosing K observations from the training data
   * @param cachedRDD
   * @return
   */
  def train(cachedRDD: RDD[Vector]): this.type = {
    this.input = cachedRDD
    this.setCentersRandom.computeKMeans
  }

  /**
   * Returns the means associated with this model.
   * If no means have been set (because the model has not been trained) throws and exception.
   * @return
   */
  def getCenters = {
    if (this.centers == null) throw new Exception("centers not set")
    else this.centers.getCenters
  }

  /**
   *
   * Returns RDD data converted to the Spark Labeled point type, where the label of each vector in
   * the given data is the index of the closest mean in this model.
   * If the model has not been trained yet throws and exception.
   *
   * @param data RDD vector data of the same format as the training data to label with this model
   * @return labeled RDD data
   */
  def LabelNewData(data: RDD[Vector]): RDD[LabeledPoint] = {
    if (trained) {
      data.map(
        v => LabeledPoint(centers.findClosest(v)._1, v))
    } else
      throw new Exception("Cannot label new data because Model is not trained")
  }

  /**
   * Returns RDD data with the count of vectors in the training data associated with each Mean
   * @return Key value pairs, where the keys are the indices of the means computed by a run of the K means algorithm
   *         and the values are the number of values associated with that key.
   */
  def getCounts = {
    if (trained) {
      //we need to broad cast the centers so that we can use them in the map step
      this.input.map(v => {
        val s = this.centers.findClosest(v)
        (s._1.toString, 1)
      }).reduceByKey(_ + _)
    }
    else {
      throw new Exception("Cannot produce counts in training data because Model is not trained")
    }
  }
}

/**
 * Utility method for processing data into Vector format
 */
object KMeansUtils {
  /**
   * Converts RDD input of comma delimited string of doubles to dense vectors
   * @param input RDD input where each line is a comma delimitted string of doubles
   * @return Vector RDD which can be used to train the Kmeans model
   */
  def toDenseVector(input: RDD[String]) = {
    val cachedRDD = input.cache()
    val vectors = cachedRDD.map(
      line => Vectors.dense(line.split(",").map(_.toDouble))
    )
    vectors
  }

  /**
   * DOESN'T WORK YET
   * Converts RDD input in the form of an array of Doubles to sparse vectors which can
   * be used in the Kmeans model
   * @param input RDD input which is an array doubles
   * @return Sparse Vector RDD representing by the MLIB utils sparse vector class
   */
  def toSparseVector(input: RDD[Array[Double]]) = {
    input.map(
      line => Converter.arrayToSparse(line))
  }

  /**
   * Converts RDD input of comma delimited string of doubles to RDD input of MLib sparse vectors which can be used
   * by the Kmeans aglorithm
   * Should be used if data has many missing or zero values.
   * @param input
   * @return
   */
  def toSparseVectorString(input: RDD[String]) = {
    input.map(
      line => Converter.arrayToSparse(line.split(",").map(_.toDouble))
    )
  }
}

