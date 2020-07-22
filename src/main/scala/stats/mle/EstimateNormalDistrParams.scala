package stats.mle

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.constants.DistributionGeneralConstants

object EstimateNormalDistrParams extends EstimateDistrParams {
  def estimate(df: DataFrame): String = {
    val totalObservations = df.count()

    val mleMeanAggFunc =
      F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN) / totalObservations
    val mleMean = computeMLE(df, mleMeanAggFunc)

    val mleStdDevAggFunc = F.sqrt(
      F.sum(
        F.pow(
          F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) - F.lit(mleMean),
          2)) / totalObservations)
    val mleStdDev = computeMLE(df, mleStdDevAggFunc)

    s"MLE => mean: ${mleMean}, stdDev: ${mleStdDev}"
  }
}
