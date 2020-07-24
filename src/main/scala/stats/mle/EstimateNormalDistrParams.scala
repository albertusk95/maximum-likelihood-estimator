package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.configs.BaseFittedDistrConfig
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

class EstimateNormalDistrParams(baseFittedDistrConfigs: Seq[BaseFittedDistrConfig])
    extends EstimateDistrParams(baseFittedDistrConfigs) {
  override def estimate(df: DataFrame, baseFittedDistrConfig: BaseFittedDistrConfig): String = {
    val totalObservations = df.count()

    val mleMean =
      computeMLE(df, getAggFunc(DistributionParamConstants.MEAN, Some(Seq(totalObservations))))
    val mleStdDev = computeMLE(
      df,
      getAggFunc(DistributionParamConstants.STD_DEV, Some(Seq(totalObservations, mleMean))))

    buildMLEResultsMessage(Seq(mleMean, mleStdDev))
  }

  override def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column = {
    val totalObservations = additionalElements.get.head

    param match {
      case DistributionParamConstants.MEAN =>
        F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN) / totalObservations
      case DistributionParamConstants.STD_DEV =>
        val mleMean = additionalElements.get(1)
        F.sqrt(
          F.sum(
            F.pow(
              F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) - F.lit(mleMean),
              2)) / totalObservations)
    }
  }

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleMean = paramMLEs.head
    val mleStdDev = paramMLEs(1)

    s"mean: ${mleMean}, stdDev: ${mleStdDev}"
  }
}
