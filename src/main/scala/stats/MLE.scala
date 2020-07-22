package stats

import org.apache.spark.sql.SparkSession
import stats.configs.{ConfigUtils, MLEConfig}
import stats.mle.EstimateParams
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
        case (config_path: String, config: MLEConfig) =>
          processOne(config_path, config)
      }
      .toList
  }

  private def processOne(config_path: String, config: MLEConfig): Unit = {
    val df = SourceFactory.of(config.source.format, config.source.path).get.readData()

    if (Util.hasInputCol(df, config.column) && Util.numericInputCol(df, config.column)) {
      val estimateResults =
        EstimateParams.estimate(df.select(config.column), config.column, config.fittedDistribution)

      SparkSession.builder.getOrCreate.createDataFrame(Seq(estimateResults)).show(truncate = false)
    }
    else {
      throw new Error("The column doesn't exist in the data OR the column is not numerical type")
    }
  }

  private def readConfig(file: String): Try[MLEConfig] =
    Try(ConfigUtils.loadConfig(file)).recover {
      case e => throw new Error(s"Error parsing file: $file", e)
    }
}
