package stats.mle

import org.apache.spark.sql.{Column, DataFrame}

abstract class EstimateDistrParams {
  def estimate(df: DataFrame): String

  def filterOutNonSupportedObservations(df: DataFrame): DataFrame

  def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column

  def buildMLEResultsMessage(paramMLEs: Seq[Double]): String

  def runEstimator(df: DataFrame): String = {
    val supportedObservations = filterOutNonSupportedObservations(df)

    estimate(supportedObservations)
  }

  def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.agg(aggFunc).head.get(0).asInstanceOf[Double]
}
