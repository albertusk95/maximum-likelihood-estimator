package stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

object Util {
  def hasInputCol(df: DataFrame, column: String): Boolean =
    df.columns.contains(column)

  def numericInputCol(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType != StringType
}
