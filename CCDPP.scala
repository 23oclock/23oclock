package org.apache.spark.ml.recommendation

import org.apache.spark.ml.linalg.SparseMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD

import scala.util.Random

class CCDPP {

}

object CCDPP{
  def train(V: RDD[Rating[Int]], k: Int, maxIter: Int, numBlocks: Int): Unit = {
    val m = V.map(_.user).max() + 1
    val n = V.map(_.item).max() + 1

    val rowBlockSize = math.ceil(m.toDouble / numBlocks).toInt
    val colBlockSize = math.ceil(n.toDouble / numBlocks).toInt

    val R = partitionData(V, rowBlockSize, colBlockSize, m, n)
    val sc = V.sparkContext
    val H = generateH(sc, n, k, colBlockSize, numBlocks)
    val W = generateW(sc, m, k, rowBlockSize, numBlocks)

    for (_ <- 0 until maxIter) {
      for (t <- 0 until k) {
        val Ht = collectArray(H, t, numBlocks, n)
        val bcHt = sc.broadcast(Ht)
        R.map{case (r, rR) =>

        }




      }
    }






  }

  def rankOneUpdate(R: SparseMatrix, j: Int, Ht: Array[Double], lambda: Double) = {
    var g = 0.0
    var h = lambda
    if (R.colPtrs(j) == R.colPtrs(j + 1)) {
      return 0.0
    } else {
      var jj = R.colPtrs(j)
      while (jj < R.colPtrs(j + 1)) {


        jj += 1
      }
    }
  }



  def collectArray(H: RDD[(Int, Array[Array[Double]])], t: Int, numBlocks: Int, n: Int) = {
    val arr = H.map(x => (x._1, x._2(t))).collectAsMap()
    val res = new Array[Double](n)
    var i = 0
    var start = 0
    while (i < numBlocks) {
      val length = arr(i).length
      System.arraycopy(arr(i), 0, res, start, length)
      start += length
      i += 1
    }
    res
  }

  def generateH(sc: SparkContext, n: Int, k: Int, colBlockSize: Int, numBlocks: Int) = {
    sc.parallelize(0 until numBlocks).map(r => {
      val curBlockSize = math.min(colBlockSize, n - r * colBlockSize)
      (r, Iterator.tabulate(k)(_ => Array.fill(curBlockSize)(Random.nextDouble())).toArray)
    })
  }

  def generateW(sc: SparkContext, n: Int, k: Int, colBlockSize: Int, numBlocks: Int) = {
    sc.parallelize(0 until numBlocks).map(r => {
      val curBlockSize = math.min(colBlockSize, n - r * colBlockSize)
      (r, Iterator.tabulate(k)(_ => Array.fill(curBlockSize)(0.0)).toArray)
    })
  }

  def partitionData(V: RDD[Rating[Int]], rowBlockSize: Int, colBlockSize: Int, m: Int, n: Int) = {
    V.flatMap(r => {
      val rowBlockId = r.user / rowBlockSize
      val colBlockId = r.item / colBlockSize
      val entry = (r.user, r.item, r.rating.toDouble)
      if (rowBlockId == colBlockId){
        Array((rowBlockId, entry))
      } else {
        Array((rowBlockId, entry), (colBlockId, entry))
      }
    }).groupByKey().map{case (r, entry) =>
      (r, SparseMatrix.fromCOO(m, n, entry))
    }
  }

  def readData(sc: SparkContext, path: String) = {
    sc.textFile(path).map(s => {
      val ss = s.split(" ")
      Rating(ss(0).toInt, ss(1).toInt, ss(2).toFloat)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ccdpp")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\23oclock\\IdeaProjects\\test\\data\\test.txt"
    val ratings = readData(sc, path)
    ratings.foreach(x => println(x.user, x.item, x.rating))

    train(ratings, 4, 10, 3)

    sc.stop()
  }



}
