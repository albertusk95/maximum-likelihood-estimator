package stats.mle

import org.apache.spark.sql.{Column, DataFrame}
import stats.configs.BaseFittedDistrConfig
import stats.sources.SourceFactory

case class MLEStatus(
  columnName: String,
  fittedDistribution: String,
  paramMLEs: String,
  sourcePath: String)

abstract class EstimateDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig]) {
  def runEstimator(): List[MLEStatus] = {
    var allDistrMLEs: List[MLEStatus] = List()
    for (baseFittedDistrConfig <- baseFittedDistrConfigs) {
      val df = SourceFactory.of(baseFittedDistrConfig.source).get.readData()
      val supportedObservations =
        filterOutNonSupportedObservations(df, baseFittedDistrConfig.column)

      val distrMLEs = estimate(supportedObservations, baseFittedDistrConfig)
      allDistrMLEs = distrMLEs :: allDistrMLEs
    }

    allDistrMLEs
  }

  def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.agg(aggFunc).head.get(0).asInstanceOf[Double]

  def filterOutNonSupportedObservations(df: DataFrame, columnName: String): DataFrame = df

  def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): MLEStatus

  def getAggFunc(columnName: String, param: String, additionalElements: Option[Seq[Any]]): Column

  def buildMLEResultsMessage(paramMLEs: Seq[Double]): String
}
