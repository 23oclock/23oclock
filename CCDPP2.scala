package org.apache.spark.ml.recommendation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.SparseMatrix
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD

import scala.util.Random

class CCDPP2 {

}

object CCDPP2{
  def train(V: RDD[ALS.Rating[Int]], k: Int, maxIter: Int, T: Int, numBlocks: Int, lambda: Double = 0.1) = {
    val sc = V.sparkContext

    val m = V.map(_.user).max() + 1
    val n = V.map(_.item).max() + 1

    val rowBlockSize = math.ceil(m.toDouble / numBlocks).toInt
    val colBlockSize = math.ceil(n.toDouble / numBlocks).toInt

    var R = partitionData(V, colBlockSize, numBlocks, m, n).cache()
    var RT = partitionData(V.map(x => Rating(x.item, x.user, x.rating)), rowBlockSize, numBlocks, n, m).cache()
//    R.count()
//    RT.count()
    val W = Array.fill(k)(Array.fill(m)(0.1))
    val H = Array.fill(k)(Array.fill(n)(0.0))


    for (oiter <- 0 until maxIter) {
      for (t <- 0 until k) {
        val u = W(t)
        val v = H(t)
        val time0 = System.currentTimeMillis()
        R = updateR(R, colBlockSize, u, v, true).cache()
        RT = updateR(RT, rowBlockSize, v, u, true).cache()
        R.count()
        RT.count()
//        println(s"update R and RT time: ${(System.currentTimeMillis() - time0) / 1000.0}")

        for (iiter <- 0 until T) {
          val time1 = System.currentTimeMillis()
          val bcU = sc.broadcast(u)
          val distV = R.mapPartitions(iter => {
            val valU = bcU.value
            iter.map{case (r, rR) =>
              val length = rR.numCols
              val newV = new Array[Double](length)
              for (j <- 0 until length) {
                newV(j) = rankOneUpdate(rR, j, valU, lambda)
              }
              (r, newV)
            }
          })
          System.arraycopy(collectArray(distV, n), 0, v, 0, n)
//          println(s"collect distV time: ${(System.currentTimeMillis() - time1) / 1000.0}")

          val bcV = sc.broadcast(v)
          val distU = RT.mapPartitions(iter => {
            val valV = bcV.value
            iter.map{case (r, rRt) =>
              val length = math.min(m - r * rowBlockSize, rowBlockSize)
              val newU = new Array[Double](length)
              for (i <- 0 until length) {
                newU(i) = rankOneUpdate(rRt, i, valV, lambda)
              }
              (r, newU)
            }
          })
          System.arraycopy(collectArray(distU, m), 0, u, 0, m)
        }

//        W(t) = u
//        H(t) = v
        R = updateR(R, colBlockSize, u, v, false).cache()
        RT = updateR(RT, rowBlockSize, v, u, false).cache()
//        R.count()
//        RT.count()

//        println(oiter, t)
        println(math.sqrt(R.map(x => x._2.toDense.values.map(x => x * x).sum).sum() / V.count()),
          RT.map(x => x._2.toDense.values.map(x => x * x).sum).sum() / V.count())
      }


    }
  }

  def collectArray(distV: RDD[(Int, Array[Double])], n: Int) = {
    val arr = distV.collect()
    val map = arr.toMap
    val res = new Array[Double](n)
    var i = 0
    var start = 0
    while (i < arr.length) {
      val length = map(i).length
      System.arraycopy(map(i), 0, res, start, length)
      start += length
      i += 1
    }
    res
  }


  def rankOneUpdate(R: SparseMatrix, j: Int, u: Array[Double], lambda: Double): Double = {
    if (R.colPtrs(j) == R.colPtrs(j + 1)) {
      return 0.0
    }
    var g = 0.0
    var h = lambda
    for (ptr <- R.colPtrs(j) until R.colPtrs(j + 1)) {
      val i = R.rowIndices(ptr)
      g += R.values(ptr) * u(i)
      h += u(i) * u(i)
    }
    val newvj = g / h
    if (newvj < 0) {
      0.0
    } else {
      newvj
    }
  }

  def updateR(R: RDD[(Int, SparseMatrix)], blockSize: Int, u: Array[Double], v: Array[Double], add: Boolean) = {
    val bcU = R.sparkContext.broadcast(u)
    val bcV = R.sparkContext.broadcast(v)
    if (add) {
      R.mapPartitions(iter => {
        val valU = bcU.value
        val valV = bcV.value
        iter.map { case (r, rR) =>
          val start = r * blockSize
          var j = 0
          while (j < rR.numCols) {
            val curCol = j + start
            for (ptr <- rR.colPtrs(j) until rR.colPtrs(j + 1)) {
              val i = rR.rowIndices(ptr)
              rR.values(ptr) += valU(i) * valV(curCol)
            }
            j += 1
          }
          (r, rR)
        }
      })
    } else {
      R.mapPartitions(iter => {
        val valU = bcU.value
        val valV = bcV.value
        iter.map { case (r, rR) =>
          val start = r * blockSize
          var j = 0
          while (j < rR.numCols) {
            val curCol = j + start
            for (ptr <- rR.colPtrs(j) until rR.colPtrs(j + 1)) {
              val i = rR.rowIndices(ptr)
              rR.values(ptr) -= valU(i) * valV(curCol)
            }
            j += 1
          }
          (r, rR)
        }
      })
    }
  }


  def partitionData(V: RDD[Rating[Int]], blockSize: Int, pt: Int, m: Int, n: Int) = {
    V.mapPartitions(iter => {
      iter.map{case Rating(user, item, rating) =>
        val blockId = item / blockSize
        val localId = item % blockSize
        (blockId, (user, localId, rating.toDouble))
      }
    }).groupByKey(pt).mapPartitions(iter => {
      iter.map{case (blockId, entries) =>
        val curCols = math.min(n - blockId * blockSize, blockSize)
        (blockId, SparseMatrix.fromCOO(m, curCols, entries))
      }
    })
  }

  def readData(sc: SparkContext, path: String) = {
    sc.textFile(path).map(s => {
      val ss = s.split("\t")
      Rating(ss(0).toInt, ss(1).toInt, ss(2).toFloat)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ccdpp")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\23oclock\\IdeaProjects\\test\\data\\ml-100k\\u.data"
    val time0 = System.currentTimeMillis()
    val ratings = readData(sc, path).cache()
    ratings.count()
    println(s"data read time: ${(System.currentTimeMillis() - time0) / 1000.0}")
//    ratings.foreach(x => println(x.user, x.item, x.rating))

    val time1 = System.currentTimeMillis()
    train(ratings, 10, 10, 1, 3)
    println(s"train time: ${(System.currentTimeMillis() - time1) / 1000.0}")


    Thread.sleep(1000000)
    sc.stop()
  }
}