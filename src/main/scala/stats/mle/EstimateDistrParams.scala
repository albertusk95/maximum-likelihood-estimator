package stats.mle

import org.apache.spark.sql.{Column, DataFrame}

abstract class EstimateDistrParams {
  def estimate(df: DataFrame): String

  def computeMLE(df: DataFrame, aggFunc: Column): Double =
    df.agg(aggFunc).head.get(0).asInstanceOf[Double]
}
