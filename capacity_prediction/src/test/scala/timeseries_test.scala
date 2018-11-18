import org.scalatest.FunSuite
import timeseries.analysis
import org.apache.spark.mllib.linalg.Vectors

class timeseries_test extends FunSuite {
  test("analysis.arimaModel"){
    val ts = Vectors.dense(333,444,555,66,44,33,44,55,66,66)
    val forecast = analysis.arima_model(ts,2)
    assert(forecast != Vectors.dense(0))
  }
}
