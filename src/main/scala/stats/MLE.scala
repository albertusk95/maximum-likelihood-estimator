package stats

import org.apache.spark.sql.SparkSession
import stats.configs.{BaseFittedDistrConfig, ConfigUtils, MLEConfig}
import stats.mle.EstimateDistrParamsFactory

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.util.Try

object MLE {
  def main(args: Array[String]): Unit =
    RunWithSpark.run(() => process(args))

  def process(args: Array[String]): Unit = {
    val configs = args
      .map(arg => readConfig(arg).get)

    args
      .zip(configs)
      .toStream
      .map {
        case (_, config: MLEConfig) =>
          processOne(config)
      }
      .toList
  }

  private def processOne(config: MLEConfig): Unit = {
    val r = currentMirror.reflect(config.maxLikelihoodEstimates)
    val maxLikelihoodEstimates = r.symbol.typeSignature.members.toStream
      .collect { case s: TermSymbol if !s.isMethod => r.reflectField(s) }
      .map(r => r.symbol.name.toString.trim -> r.get)
      .map(x => x._2.asInstanceOf[Option[Seq[BaseFittedDistrConfig]]])

    val allMaxLikelihoodEstimates = maxLikelihoodEstimates
      .filter(_.isDefined)
      .map { fittedDistrConfigs =>
        fittedDistrConfigs
          .asInstanceOf[Option[Seq[BaseFittedDistrConfig]]]
          .flatMap(EstimateDistrParamsFactory.getEstimateDistrParams)
          .map(x => x.runEstimator())
          .get
      }
      .toList

    SparkSession.builder
      .getOrCreate()
      .createDataFrame(allMaxLikelihoodEstimates.flatten)
      .show(truncate = false)
  }

  private def readConfig(file: String): Try[MLEConfig] =
    Try(ConfigUtils.loadConfig(file)).recover {
      case e => throw new Error(s"Error parsing file: $file", e)
    }
}
