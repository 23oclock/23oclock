package org.apache.spark.ml.recommendation

import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.CCDPP.readData
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ALS4NMF {

}

object ALS4NMF {
  def train(ratings: RDD[Rating[Int]], rank: Int, maxIter: Int, numUserBlocks: Int, numItemBlocks: Int) = {
    ratings.cache()

    val sc = ratings.sparkContext

    val numUsers = ratings.map(_.user).max() + 1
    val numItems = ratings.map(_.item).max() + 1

    val userFactorsAndLabels = partition4user(ratings, numUserBlocks, numUsers, rank)
    val itemFactorsAndLabels = partition4item(ratings, numItemBlocks, numItems, rank)
    val userLabels = userFactorsAndLabels._2
    userLabels.cache()
    var userFactors = userFactorsAndLabels._1
    val itemLabels = itemFactorsAndLabels._2
    itemLabels.cache()
    var itemFactors = itemFactorsAndLabels._1

//    var bcItemFactors = sc.broadcast(itemFactors.collectAsMap())
    for (i <- 0 until maxIter) {
      val userFactorsMap = userFactors.collectAsMap()
      val bcUserFactors = sc.broadcast(userFactorsMap)
      itemFactors = itemLabels.mapPartitions(iter => {
        val valUserFactors = bcUserFactors.value
        iter.map{case (itemId, itemLabels, userIds) =>
          val curUserFactors = userIds.map(valUserFactors(_))
          // TODO compute itemFactors
          (itemId, Array.fill(rank)(1.0))
        }
      }, true)
//      bcItemFactors.destroy()

      val itemFactorsMap = itemFactors.collectAsMap()
      val bcItemFactors = sc.broadcast(itemFactorsMap)
      userFactors = userLabels.mapPartitions(iter => {
        val valItemFactors = bcItemFactors.value
        iter.map{ case (userId, userLabels, itemIds) =>
          val curItemFactors = itemIds.map(valItemFactors(_))
          // TODO compute userFactors
          (userId, Array.fill(rank)(1.0))
        }
      }, true)
//      bcUserFactors.destroy()
      println(i)
    }




  }

  def partition4user(ratings: RDD[Rating[Int]], numUserBlocks: Int, numUsers: Int, rank: Int) = {
    require(numUsers >= numUserBlocks, "number of users should greater than number of user blocks")
    val blockSize = numUsers / numUserBlocks
    val partitioner = new NMFPartitioner(blockSize, numUsers)
    val partitionedRatings = ratings.map(r => (r.user, (r.item, r.rating))).partitionBy(partitioner)
    val userLabels = partitionedRatings.mapPartitions(iter => {
      iter.toArray.groupBy(_._1)
        .mapValues(_.map(_._2).sortBy(_._1))
        .map(x => (x._1, x._2.map(_._2), x._2.map(_._1)))
        .toIterator
    }, true)
    val userFactors = partitionedRatings.mapPartitions(iter => {
      iter.map(x => (x._1, Array.fill(rank)(Random.nextDouble())))
    }, true)
    (userFactors, userLabels)
  }

  def partition4item(ratings: RDD[Rating[Int]], numItemBlocks: Int, numItems: Int, rank: Int) = {
    require(numItems >= numItemBlocks, "number of users should greater than number of user blocks")
    val blockSize = numItems / numItemBlocks
    val partitioner = new NMFPartitioner(blockSize, numItems)
    val partitionedRatings = ratings.map(r => (r.item, (r.user, r.rating))).partitionBy(partitioner)
    val itemLabels = partitionedRatings.mapPartitions(iter => {
      iter.toArray.groupBy(_._1)
        .mapValues(_.map(_._2).sortBy(_._1))
        .map(x => (x._1, x._2.map(_._2), x._2.map(_._1)))
        .toIterator
    })
    val itemFactors = partitionedRatings.mapPartitions(iter => {
      iter.map(x => (x._1, Array.fill(rank)(Random.nextDouble())))
    }, true)
    (itemFactors, itemLabels)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ccdpp")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\23oclock\\IdeaProjects\\test\\data\\test.txt"
    val ratings = readData(sc, path)
    ratings.foreach(x => println(x.user, x.item, x.rating))

    train(ratings, 10, 10, 3, 3)

    Thread.sleep(10000000)
    sc.stop()
  }
}

class NMFPartitioner(blockSize: Int, n: Int) extends Partitioner {
  override def numPartitions: Int = math.ceil(n.toDouble / blockSize).toInt

  override def getPartition(key: Any): Int = {
    key match {
      case i: Int => i / blockSize
    }
  }
}
