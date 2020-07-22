package stats.mle

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.constants.{DistributionConstants, DistributionGeneralConstants}

case class MLEStatus(fittedDistribution: String, paramEstimates: String)

object EstimateParams {
  def estimate(df: DataFrame, colName: String, fittedDistribution: String): MLEStatus = {
    val filteredOutNullsDf = filterOutNulls(df, colName)
    val standardizedColNamedf = standardizeColName(filteredOutNullsDf, colName)

    val paramEstimates = fittedDistribution match {
      case DistributionConstants.NORMAL => EstimateNormalDistrParams.estimate(standardizedColNamedf)
    }

    MLEStatus(fittedDistribution, paramEstimates)
  }

  private def filterOutNulls(df: DataFrame, colName: String): DataFrame =
    df.filter(!F.isnull(F.col(colName)))

  private def standardizeColName(df: DataFrame, colName: String): DataFrame =
    df.withColumnRenamed(colName, DistributionGeneralConstants.MLE_TARGET_COLUMN)

  private def roundValues(df: DataFrame, rounding: Int): DataFrame = {
    df.withColumn(
      DistributionGeneralConstants.MLE_TARGET_COLUMN,
      F.round(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN), rounding))
  }
}
