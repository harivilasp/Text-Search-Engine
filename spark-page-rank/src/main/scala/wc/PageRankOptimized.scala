package wc

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object PageRankOptimized {

  def main(args: Array[String]) = {
    val logger = LogManager.getRootLogger

    val k = args(0).toInt
    val numIterations = args(1).toInt

    // Initialize SparkSession to build RDDs and perform transformations and actions on them.
    val sparkSession = SparkSession.builder.appName("PageRankAlgorithm").getOrCreate()

    // Calculate total number of nodes based on the k.
    val totalNumberOfNodes = k * k

    val initialPageRankValue = 1.0 / totalNumberOfNodes

    // Create an RDD of node IDs directly without storing them in memory.
    val nodesRDD = sparkSession.sparkContext.parallelize(1 to totalNumberOfNodes)

    // Initialize ranks with the initial PageRank value.
    var ranksRDD = nodesRDD.map(nodeId => (nodeId, initialPageRankValue))

    for (iteration <- 1 to numIterations) {
      // Dynamically generate prContributions based on node IDs.
      val prContributions = ranksRDD.flatMap { case (sourcePageId, rank) =>
        val neighborPages = if (sourcePageId % k == 0) Seq(0) else Seq((sourcePageId + 1))
        neighborPages.map(dest => (dest, rank / neighborPages.size)) :+ (sourcePageId, 0.0)
      }.reduceByKey(_ + _)

      val danglingMass = prContributions.lookup(0).headOption.getOrElse(0.0)

      ranksRDD = prContributions.filter(_._1 != 0).mapValues(
        rank => (0.15) / (totalNumberOfNodes) + 0.85 * (rank + (danglingMass / totalNumberOfNodes))
      )

      ranksRDD = ranksRDD.union(sparkSession.sparkContext.parallelize(List((0, 0.0)))).cache()
    }
    logger.info(ranksRDD.toDebugString)
    val sortedByIntKeyRDD = ranksRDD.sortBy(_._1, ascending = true) // Sort by the integer key
    sortedByIntKeyRDD.saveAsTextFile(args(2))
    sparkSession.stop()
  }

}
