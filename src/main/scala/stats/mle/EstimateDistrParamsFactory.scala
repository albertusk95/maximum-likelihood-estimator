package stats.mle

import stats.configs.{
  BaseFittedDistrConfig,
  FittedBinomialDistrConfig,
  FittedExpDistrConfig,
  FittedNormalDistrConfig
}

object EstimateDistrParamsFactory {
  def getEstimateDistrParams(
    baseFittedDistrConfigs: Seq[BaseFittedDistrConfig]
  ): Option[EstimateDistrParams] = {
    baseFittedDistrConfigs.head match {
      case _: FittedNormalDistrConfig =>
        Some(new EstimateNormalDistrParams(baseFittedDistrConfigs))
      case _: FittedExpDistrConfig =>
        Some(new EstimateExpDistrParams(baseFittedDistrConfigs))
      case _: FittedBinomialDistrConfig =>
        Some(new EstimateBinomialDistrParams(baseFittedDistrConfigs))
      case _ =>
        None
    }
  }
}
