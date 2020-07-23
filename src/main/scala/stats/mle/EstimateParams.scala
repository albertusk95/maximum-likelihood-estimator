package stats.mle

import org.apache.spark.sql.{DataFrame, functions => F}
import stats.constants.{DistributionConstants, DistributionGeneralConstants}

case class MLEStatus(
  columnName: String,
  fittedDistribution: String,
  paramMLEs: String,
  sourcePath: String)

object EstimateParams {
  def estimate(
    df: DataFrame,
    columnName: String,
    fittedDistribution: String,
    sourcePath: String): MLEStatus = {
    if (MLEUtils.hasInputCol(df, columnName) && MLEUtils.numericInputCol(df, columnName)) {
      val standardizedColNamedf = standardizeColName(df, columnName)
      val filteredOutNullsDf = filterOutNulls(standardizedColNamedf)

      val paramMLEs = fittedDistribution match {
        case DistributionConstants.NORMAL =>
          EstimateNormalDistrParams.runEstimator(filteredOutNullsDf)
        case DistributionConstants.EXP =>
          EstimateExpDistrParams.runEstimator(filteredOutNullsDf)
      }

      MLEStatus(columnName, fittedDistribution, paramMLEs, sourcePath)
    }
    else {
      MLEStatus(columnName, fittedDistribution, getInvalidPreConditionsMessage, sourcePath)
    }
  }

  private def standardizeColName(df: DataFrame, columnName: String): DataFrame =
    df.withColumnRenamed(columnName, DistributionGeneralConstants.MLE_TARGET_COLUMN)

  private def filterOutNulls(df: DataFrame): DataFrame =
    df.filter(!F.isnull(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN)))

  private def roundValues(df: DataFrame, rounding: Int): DataFrame = {
    df.withColumn(
      DistributionGeneralConstants.MLE_TARGET_COLUMN,
      F.round(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN), rounding))
  }

  private def getInvalidPreConditionsMessage: String =
    s"[INVALID_PRE_CONDITIONS] Column must be exist AND numerical type"
}
