package timeseries

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class configuration {

  def config():SparkContext={

    val sparkconf = new SparkConf().setAppName("Time Series Analysis using ARIMA").setMaster("yarn")
    val sc = new SparkContext(sparkconf)
    sc
  }

}
