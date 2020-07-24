package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.configs.{BaseFittedDistrConfig, FittedBinomialDistrConfig}
import stats.constants.{DistributionConstants, DistributionParamConstants}

class EstimateBinomialDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig])
    extends EstimateDistrParams(baseFittedDistrConfigs) {
  override def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): MLEStatus = {
    val binomialDistrConfig = baseFittedDistrConfig.asInstanceOf[FittedBinomialDistrConfig]
    val totalObservations = df.count()

    val mleRate =
      computeMLE(
        df,
        getAggFunc(
          baseFittedDistrConfig.column,
          DistributionParamConstants.SUCCESS_PROBA,
          Some(Seq(totalObservations, binomialDistrConfig.successEvent))))

    MLEStatus(
      baseFittedDistrConfig.column,
      DistributionConstants.BINOMIAL,
      buildMLEResultsMessage(Seq(mleRate)),
      baseFittedDistrConfig.source.path)
  }

  override def getAggFunc(
    columnName: String,
    param: String,
    additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.SUCCESS_PROBA =>
        val totalObservations = additionalElements.get.head
        val successEvent = additionalElements.get(1)

    }
  }

  override def computeMLE(df: DataFrame, aggFunc: Column): Double = {}

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleSuccessProba = paramMLEs.head

    s"success_probability: ${mleSuccessProba}"
  }
}
