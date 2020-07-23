package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.configs.BaseFittedDistrConfig
import stats.constants.{DistributionConstants, DistributionParamConstants}

class EstimateExpDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig])
    extends EstimateDistrParams(baseFittedDistrConfigs) {
  override def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): MLEStatus = {
    val totalObservations = df.count()

    val mleRate =
      computeMLE(
        df,
        getAggFunc(
          baseFittedDistrConfig.column,
          DistributionParamConstants.RATE,
          Some(Seq(totalObservations))))

    MLEStatus(
      baseFittedDistrConfig.column,
      DistributionConstants.EXP,
      buildMLEResultsMessage(Seq(mleRate)),
      baseFittedDistrConfig.source.path)
  }

  override def filterOutNonSupportedObservations(df: DataFrame, columnName: String): DataFrame =
    df.filter(F.col(columnName) >= F.lit(0))

  override def getAggFunc(
    columnName: String,
    param: String,
    additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.RATE =>
        val totalObservations = additionalElements.get.head
        F.lit(totalObservations) / F.sum(columnName)
    }
  }

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleRate = paramMLEs.head

    s"rate: ${mleRate}"
  }
}
