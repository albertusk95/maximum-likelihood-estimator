package stats.sources

import stats.configs.SourceConfig
import stats.constants.SourceConstants

import scala.util.{Failure, Success, Try}

object SourceFactory {
  def of(source: SourceConfig): Try[DataReader] = {
    source.format match {
      case SourceConstants.PARQUET =>
        Success(new ParquetDataReader(source.path))
      case SourceConstants.CSV =>
        Success(new CsvDataReader(source.path))
      case _ => Failure(new ClassNotFoundException(s"DataReader ${source.format} not found"))
    }
  }
}
