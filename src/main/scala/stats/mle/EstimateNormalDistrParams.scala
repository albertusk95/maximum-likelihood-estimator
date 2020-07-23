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

  private def getAggFunc(param: String, optionalElements: Seq[Any]): Column = {
    param match {
      case DistributionParamConstants.MEAN =>
        val totalObservations = optionalElements.head
        F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN) / totalObservations
      case DistributionParamConstants.STD_DEV =>
        val totalObservations = optionalElements.head
        val mleMean = optionalElements(1)
        F.sqrt(
          F.sum(
            F.pow(
              F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) - F.lit(mleMean),
              2)) / totalObservations)
    }
  }
}
