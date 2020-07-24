package stats.mle

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import stats.configs.{
  BaseFittedDistrConfig,
  FittedBinomialDistrConfig,
  FittedExpDistrConfig,
  FittedNormalDistrConfig,
  FittedPoissonDistrConfig
}
import stats.constants.DistributionConstants

object MLEUtils {
  def validPreConditions(df: DataFrame, fittedDistrConfig: BaseFittedDistrConfig): Boolean = {
    fittedDistrConfig match {
      case _: FittedNormalDistrConfig =>
        hasInputCol(df, fittedDistrConfig.column) && numericInputCol(df, fittedDistrConfig.column)
      case _: FittedExpDistrConfig =>
        hasInputCol(df, fittedDistrConfig.column) && numericInputCol(df, fittedDistrConfig.column)
      case _: FittedBinomialDistrConfig =>
        hasInputCol(df, fittedDistrConfig.column)
      case _: FittedPoissonDistrConfig =>
        hasInputCol(df, fittedDistrConfig.column) && numericIntegerInputCol(
          df,
          fittedDistrConfig.column)
    }
  }

  def getFittedDistribution(fittedDistrConfig: BaseFittedDistrConfig): String = {
    fittedDistrConfig match {
      case _: FittedNormalDistrConfig   => DistributionConstants.NORMAL
      case _: FittedExpDistrConfig      => DistributionConstants.EXP
      case _: FittedBinomialDistrConfig => DistributionConstants.BINOMIAL
      case _: FittedPoissonDistrConfig  => DistributionConstants.POISSON
    }
  }

  def hasInputCol(df: DataFrame, columnName: String): Boolean =
    df.columns.contains(columnName)

  def numericInputCol(df: DataFrame, columnName: String): Boolean =
    df.schema(columnName).dataType != StringType

  def numericIntegerInputCol(df: DataFrame, columnName: String): Boolean =
    df.schema(columnName).dataType == IntegerType

  def standardizeColName(df: DataFrame, columnName: String, newColumnName: String): DataFrame =
    df.withColumnRenamed(columnName, newColumnName)
}
