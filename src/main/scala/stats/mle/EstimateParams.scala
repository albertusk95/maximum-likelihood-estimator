package stats.mle

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.constants.{DistributionConstants, DistributionGeneralConstants}

case class MLEStatus(columnName: String, fittedDistribution: String, paramMLEs: String)

object EstimateParams {
  def estimate(df: DataFrame, columnName: String, fittedDistribution: String): MLEStatus = {
    val filteredOutNullsDf = filterOutNulls(df, columnName)
    val standardizedColNamedf = standardizeColName(filteredOutNullsDf, columnName)

    val paramMLEs = fittedDistribution match {
      case DistributionConstants.NORMAL => EstimateNormalDistrParams.estimate(standardizedColNamedf)
      case DistributionConstants.EXP    => EstimateExpDistrParams.estimate(standardizedColNamedf)
    }

    MLEStatus(columnName, fittedDistribution, paramMLEs)
  }

  private def filterOutNulls(df: DataFrame, columnName: String): DataFrame =
    df.filter(!F.isnull(F.col(columnName)))

  private def standardizeColName(df: DataFrame, columnName: String): DataFrame =
    df.withColumnRenamed(columnName, DistributionGeneralConstants.MLE_TARGET_COLUMN)

  private def roundValues(df: DataFrame, rounding: Int): DataFrame = {
    df.withColumn(
      DistributionGeneralConstants.MLE_TARGET_COLUMN,
      F.round(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN), rounding))
  }
}
