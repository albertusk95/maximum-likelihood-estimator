package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

object EstimateNormalDistrParams extends EstimateDistrParams {
  def estimate(df: DataFrame): String = {
    val totalObservations = df.count()

    val mleMean =
      computeMLE(df, getAggFunc(DistributionParamConstants.MEAN, Seq(totalObservations)))
    val mleStdDev = computeMLE(
      df,
      getAggFunc(DistributionParamConstants.STD_DEV, Seq(totalObservations, mleMean)))

    s"mean: ${mleMean}, stdDev: ${mleStdDev}"
  }

  override def getAggFunc(param: String, additionalElements: Seq[Any]): Column = {
    param match {
      case DistributionParamConstants.MEAN =>
        val totalObservations = additionalElements.head
        F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN) / totalObservations
      case DistributionParamConstants.STD_DEV =>
        val totalObservations = additionalElements.head
        val mleMean = additionalElements(1)
        F.sqrt(
          F.sum(
            F.pow(
              F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) - F.lit(mleMean),
              2)) / totalObservations)
    }
  }
}
