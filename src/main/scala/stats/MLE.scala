package stats

import org.apache.spark.sql.SparkSession
import stats.configs.{ConfigUtils, MLEConfigs}
import stats.mle.{EstimateParams, MLEStatus}
import stats.sources.SourceFactory

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
        case (_, config: MLEConfigs) =>
          processOne(config)
      }
      .toList
  }

  private def processOne(config: MLEConfigs): Unit = {
    config.maxLikelihoodEstimates match {
      case Some(mles) =>
        val mleStatuses: Seq[MLEStatus] = mles.map { distrMLEs =>
          val df = SourceFactory.of(distrMLEs.source.format, distrMLEs.source.path).get.readData()

          EstimateParams.estimate(
            df.select(distrMLEs.column),
            distrMLEs.column,
            distrMLEs.fittedDistribution,
            distrMLEs.source.path)
        }

        SparkSession.builder.getOrCreate
          .createDataFrame(mleStatuses)
          .show(truncate = false)
      case None =>
    }
  }

  private def readConfig(file: String): Try[MLEConfigs] =
    Try(ConfigUtils.loadConfig(file)).recover {
      case e => throw new Error(s"Error parsing file: $file", e)
    }
}
