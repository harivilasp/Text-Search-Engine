package wc

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object PageRank {

  def main(args: Array[String]) = {
    val logger = LogManager.getRootLogger

    val k = args(0).toInt
    val numIterations = args(1).toInt

    // Initialize SparkSession to build RDDs and perform transformations and actions on them.
    val sparkSession = SparkSession.builder.appName("PageRankAlgorithm").getOrCreate()

    // Calculate total number of nodes based on the k.
    val totalNumberOfNodes = k * k

    // Generate a list of nodes for the PageRank graph.
    val nodeList = List.range(1, totalNumberOfNodes + 2)

    // Initialize all nodes with an equal share of the PageRank value.
    val initialPageRankValue = 1.0 / totalNumberOfNodes

    // Generate a list of edges based on the node list and k.
    val edgeList = createEdgeList(nodeList, k)

    // Initialize ranks with the initial PageRank value for all nodes except the dummy node (nodeId == 0).
    val initialRanks = List.range(0, totalNumberOfNodes + 1)
      .map(nodeId => if (nodeId == 0) (nodeId, 0.0) else (nodeId, initialPageRankValue))

    // Parallelize the edge list into an RDD and cache it for efficient access.
    val edgesRDD = sparkSession.sparkContext.parallelize(edgeList).cache()

    // Parallelize the initial ranks into an RDD.
    var ranksRDD = sparkSession.sparkContext.parallelize(initialRanks)

    // Iterate over the PageRank algorithm calculation.
    for (iteration <- 1 to numIterations) {
      // Calculate prContributions from each page to its adjacent pages.
      // Also emit node with contribution of 0 for nodes with no incoming edges to ensure it remains in graph.
      val prContributions = edgesRDD.join(ranksRDD).flatMap {
        case (sourcePageId, (neighborPages, rank)) =>
          neighborPages.map(destination => (destination, rank / neighborPages.size)) :+ (sourcePageId, 0.0) // added so that sourcePageId doesn't vanish
      }.reduceByKey(_ + _)

      // Extract the dangling mass, which is the PageRank value contributed by dangling nodes.
      val danglingMass = prContributions.lookup(0).headOption.getOrElse(0.0)

      // Update ranks based on prContributions and redistribute the dangling mass evenly among all nodes.
      ranksRDD = prContributions.filter(_._1 != 0).mapValues(
        rank => (0.15)/(totalNumberOfNodes) + 0.85 * (rank + (danglingMass/totalNumberOfNodes))
      )
      // Ensure the dummy node (nodeId == 0) is always present with a PageRank value of 0.
      ranksRDD = ranksRDD.union(sparkSession.sparkContext.parallelize(List((0, 0.0)))).cache()
    }

    logger.info(ranksRDD.toDebugString)
    val sortedByIntKeyRDD = ranksRDD.sortBy(_._1, ascending = true) // Sort by the key
    val top20RanksRDD = sparkSession.sparkContext.parallelize(sortedByIntKeyRDD.take(20))
    top20RanksRDD.saveAsTextFile(args(2))
    sparkSession.stop()
  }

  def createEdgeList(nodes: Seq[Int], k: Int): List[(Int, Seq[Int])] = {
    nodes.sliding(2).map { case Seq(currentNode, nextNode) =>
      // Create an edge to the dummy node (nodeId == 0) for dangling nodes (those at the end of a row).
      if (currentNode % k == 0) (currentNode, Seq(0))
      else (currentNode, Seq(nextNode)) // Otherwise, create a normal edge.
    }.toList
  }
}
