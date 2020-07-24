package stats.configs

case class FittedNormalDistrConfig(column: String, source: SourceConfig)
    extends BaseFittedDistrConfig

case class FittedExpDistrConfig(column: String, source: SourceConfig) extends BaseFittedDistrConfig

case class FittedBinomialDistrConfig(column: String, source: SourceConfig, successEvent: String)
    extends BaseFittedDistrConfig

case class FittedPoissonDistrConfig(column: String, source: SourceConfig)
    extends BaseFittedDistrConfig
