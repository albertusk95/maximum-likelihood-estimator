package stats.configs

case class FittedDistrsConfig(
  normal: Option[Seq[FittedNormalDistrConfig]],
  exp: Option[Seq[FittedExpDistrConfig]],
  binomial: Option[Seq[FittedBinomialDistrConfig]],
  poisson: Option[Seq[FittedPoissonDistrConfig]])
