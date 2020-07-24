package stats.mle

import org.apache.spark.sql.{Column, DataFrame}
import stats.configs.BaseFittedDistrConfig
import stats.constants.DistributionGeneralConstants
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
      var distrMLEs = MLEStatus(
        baseFittedDistrConfig.column,
        MLEUtils.getFittedDistribution(baseFittedDistrConfig),
        "[INVALID PRE CONDITIONS]",
        baseFittedDistrConfig.source.path)

      if (MLEUtils.validPreConditions(df, baseFittedDistrConfig)) {
        val standardizedColNameDf = MLEUtils.standardizeColName(
          df,
          baseFittedDistrConfig.column,
          DistributionGeneralConstants.MLE_TARGET_COLUMN)
        val supportedObservations = filterOutNonSupportedObservations(standardizedColNameDf)

        distrMLEs = estimate(
          supportedObservations.select(DistributionGeneralConstants.MLE_TARGET_COLUMN),
          baseFittedDistrConfig)
      }

      allDistrMLEs = distrMLEs :: allDistrMLEs
    }

    allDistrMLEs
  }

  def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.agg(aggFunc).head.get(0).asInstanceOf[Double]

  def filterOutNonSupportedObservations(df: DataFrame): DataFrame = df

  def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): MLEStatus

  def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column

  def buildMLEResultsMessage(paramMLEs: Seq[Double]): String
}
