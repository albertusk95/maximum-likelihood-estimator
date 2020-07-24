package stats.mle

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

object MLEUtils {
  def hasInputCol(df: DataFrame, columnName: String): Boolean =
    df.columns.contains(columnName)

  def numericInputCol(df: DataFrame, columnName: String): Boolean =
    df.schema(columnName).dataType != StringType

  def standardizeColName(df: DataFrame, columnName: String, newColumnName: String): DataFrame =
    df.withColumnRenamed(columnName, newColumnName)
}
