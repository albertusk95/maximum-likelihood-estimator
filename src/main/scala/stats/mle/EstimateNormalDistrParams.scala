package stats.mle

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import stats.constants.{DistributionGeneralConstants, DistributionParamConstants}

object EstimateNormalDistrParams extends EstimateDistrParams {
  def estimate(df: DataFrame): String = {
    val totalObservations = df.count()

    val mleMean =
      computeMLE(df, getAggFunc(DistributionParamConstants.MEAN, Some(Seq(totalObservations))))
    val mleStdDev = computeMLE(
      df,
      getAggFunc(DistributionParamConstants.STD_DEV, Some(Seq(totalObservations, mleMean))))

    buildMLEResultsMessage(Seq(mleMean, mleStdDev))
  }

  override def getAggFunc(param: String, additionalElements: Option[Seq[Any]]): Column = {
    param match {
      case DistributionParamConstants.MEAN =>
        val totalObservations = additionalElements.get.head
        F.sum(DistributionGeneralConstants.MLE_TARGET_COLUMN) / totalObservations
      case DistributionParamConstants.STD_DEV =>
        val totalObservations = additionalElements.get.head
        val mleMean = additionalElements.get(1)
        F.sqrt(
          F.sum(
            F.pow(
              F.col(DistributionGeneralConstants.MLE_TARGET_COLUMN) - F.lit(mleMean),
              2)) / totalObservations)
    }
  }

  override def buildMLEResultsMessage(paramMLEs: Seq[Double]): String = {
    val mleMean = paramMLEs.head
    val mleStdDev = paramMLEs(1)

    s"mean: ${mleMean}, stdDev: ${mleStdDev}"
  }
}
