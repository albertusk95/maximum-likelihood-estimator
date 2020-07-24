package stats.constants

object DistributionConstants {
  val NORMAL = "normal"
  val EXP = "exp"
  val BINOMIAL = "binomial"
}

object DistributionGeneralConstants {
  val MLE_TARGET_COLUMN = "mle_target_column"
}

object DistributionParamConstants {
  // normal
  val MEAN = "mean"
  val STD_DEV = "std_dev"

  // exponential
  val RATE = "rate"

  // binomial
  val SUCCESS_PROBA = "success_proba"
}
