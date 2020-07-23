package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

object EstimateExpDistrParams extends EstimateDistrParams {
  override def estimate(df: DataFrame): String = {
    val totalObservations = df.count()

    val mleRate =
      computeMLE(df, getAggFunc(DistributionParamConstants.RATE, Some(Seq(totalObservations))))

    buildMLEResultsMessage(Seq(mleRate))
  }

  override def filterOutNonSupportedObservations(df: DataFrame): DataFrame =
    df.filter(F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) >= F.lit(0))

  override def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.RATE =>
        val totalObservations = additionalElements.get.head
        F.lit(totalObservations) / F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN)
    }
  }

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleRate = paramMLEs.head

    s"rate: ${mleRate}"
  }
}
