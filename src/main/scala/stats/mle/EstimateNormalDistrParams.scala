package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.configs.BaseFittedDistrConfig
import stats.constants.{DistributionConstants, DistributionParamConstants}

class EstimateNormalDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig])
    extends EstimateDistrParams(baseFittedDistrConfigs) {
  override def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): MLEStatus = {
    val totalObservations = df.count()

    val mleMean =
      computeMLE(
        df,
        getAggFunc(
          baseFittedDistrConfig.column,
          DistributionParamConstants.MEAN,
          Some(Seq(totalObservations))))
    val mleStdDev = computeMLE(
      df,
      getAggFunc(
        baseFittedDistrConfig.column,
        DistributionParamConstants.STD_DEV,
        Some(Seq(totalObservations, mleMean))))

    MLEStatus(
      baseFittedDistrConfig.column,
      DistributionConstants.NORMAL,
      buildMLEResultsMessage(Seq(mleMean, mleStdDev)),
      baseFittedDistrConfig.source.path)
  }

  override def getAggFunc(
    columnName: String,
    param: String,
    additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.MEAN =>
        val totalObservations = additionalElements.get.head
        F.sum(columnName) / totalObservations
      case DistributionParamConstants.STD_DEV =>
        val totalObservations = additionalElements.get.head
        val mleMean = additionalElements.get(1)
        F.sqrt(F.sum(F.pow(F.col(columnName) - F.lit(mleMean), 2)) / totalObservations)
    }
  }

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleMean = paramMLEs.head
    val mleStdDev = paramMLEs(1)

    s"mean: ${mleMean}, stdDev: ${mleStdDev}"
  }
}
