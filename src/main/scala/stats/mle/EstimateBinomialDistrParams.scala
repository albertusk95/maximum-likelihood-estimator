package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.configs.{BaseFittedDistrConfig, FittedBinomialDistrConfig}
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

class EstimateBinomialDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig])
    extends EstimateDistrParams(baseFittedDistrConfigs) {
  override def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): String = {
    val binomialDistrConfig = baseFittedDistrConfig.asInstanceOf[FittedBinomialDistrConfig]
    val totalObservations = df.count()

    val mleSuccessProba =
      computeMLE(
        df,
        getAggFunc(
          DistributionParamConstants.SUCCESS_PROBA,
          Some(Seq(totalObservations, binomialDistrConfig.successEvent))))

    buildMLEResultsMessage(Seq(mleSuccessProba))
  }

  override def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.SUCCESS_PROBA =>
        val totalObservations = additionalElements.get.head
        val successEvent = additionalElements.get(1)

        F.when(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) === successEvent, 1)
          .otherwise(F.lit(0)) / totalObservations
    }
  }

  override def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.withColumn(DistributionGeneralConstants.MLE_TARGET_COLUMN, aggFunc)
      .agg(F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN))
      .head
      .get(0)
      .asInstanceOf[Double]

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleSuccessProba = paramMLEs.head

    s"successProba: ${mleSuccessProba}"
  }
}
