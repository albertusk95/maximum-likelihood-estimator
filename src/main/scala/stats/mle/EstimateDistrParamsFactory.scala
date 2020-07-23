package stats.mle

import stats.configs.{BaseFittedDistrConfig, FittedExpDistrConfig, FittedNormalDistrConfig}

object EstimateDistrParamsFactory {
  def getConstraint(
    baseFittedDistrConfigs: Seq[BaseFittedDistrConfig]
  ): Option[EstimateDistrParams] = {
    baseFittedDistrConfigs.head match {
      case _: FittedNormalDistrConfig =>
        Some(new EstimateNormalDistrParams(baseFittedDistrConfigs))
      case _: FittedExpDistrConfig =>
        Some(new EstimateExpDistrParams(baseFittedDistrConfigs))
      case _ =>
        None
    }
  }
}
