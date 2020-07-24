package stats.mle

import org.apache.spark.sql.{Column, DataFrame}
import stats.configs.BaseFittedDistrConfig
import stats.constants.DistributionGeneralConstants
import stats.sources.SourceFactory

case class MLEStatus(
  columnName: String,
  fittedDistribution: String,
  var paramMLEs: String,
  sourcePath: String)

abstract class EstimateDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig]) {
  def runEstimator(): List[MLEStatus] = {
    var allMLEStatuses: List[MLEStatus] = List()
    for (baseFittedDistrConfig <- baseFittedDistrConfigs) {
      val df = SourceFactory.of(baseFittedDistrConfig.source).get.readData()
      val mleStatus = MLEStatus(
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

        mleStatus.paramMLEs = estimate(
          supportedObservations.select(DistributionGeneralConstants.MLE_TARGET_COLUMN),
          baseFittedDistrConfig)
      }

      allMLEStatuses = mleStatus :: allMLEStatuses
    }

    allMLEStatuses
  }

  def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.agg(aggFunc).head.get(0).asInstanceOf[Double]

  def filterOutNonSupportedObservations(df: DataFrame): DataFrame = df

  def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): String

  def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column

  def buildMLEResultsMessage(paramMLEs: Seq[Double]): String
}
