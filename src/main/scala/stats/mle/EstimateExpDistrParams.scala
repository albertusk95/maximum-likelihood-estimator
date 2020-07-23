package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

object EstimateExpDistrParams extends EstimateDistrParams {
  def estimate(df: DataFrame): String = {
    val totalObservations = df.count()

    val mleRate =
      computeMLE(df, getAggFunc(DistributionParamConstants.RATE, Some(Seq(totalObservations))))

    s"rate: ${mleRate}"
  }

  override def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.RATE =>
        val totalObservations = additionalElements.get.head
        F.lit(totalObservations) / F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN)
    }
  }
}
