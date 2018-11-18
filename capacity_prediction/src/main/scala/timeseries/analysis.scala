package timeseries
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{to_date, to_timestamp}

import scala.collection.mutable.ArrayBuffer


object analysis {

  def parse_data(textRDD: RDD[String], header: String, cbu: String , phub: String, cmts: String, macdomain: String, base: String): (RDD[(String,Double)],Double, String)={
    val seriesRDD = textRDD.filter(row=> row != header).flatMap {l =>
      l.split(",") match {
        case a: Array[String] if a.size >= 8 => Some((a(0), a(1), a(2), a(3), a(4), a(5).toDouble, a(6).toDouble, a(7).toFloat))
        case _ => None
      }
    }

    val spark: SparkSession = SparkSession.builder.master("yarn").getOrCreate
    var agg_series_rdd = seriesRDD.filter(x=>x._2 == phub && x._3==cmts && x._4 == macdomain).map(l=>(l._5.slice(0,10), l._7))

    if(base == "average") {
      agg_series_rdd = seriesRDD.filter(x=>x._2 == phub && x._3==cmts && x._4 == macdomain).map(l=>(l._5.slice(0,10), l._7))
      .aggregateByKey((0.0, 0.0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .mapValues(sumCount => 1.0 * sumCount._1 / sumCount._2)
    }
    else if (base =="peak")
    {
      agg_series_rdd = seriesRDD.filter(x=>x._2 == phub && x._3==cmts && x._4 == macdomain).map(l=>(l._5.slice(0,10), l._7))
      .aggregateByKey(0.0)(math.max(_, _), math.max(_,_))
    }

    val colnames = Seq("date","capacity")
    val df = spark.createDataFrame(agg_series_rdd).toDF(colnames: _*)
    df.show()
    val mdf = df.withColumn("Date",to_date(to_timestamp(df.col("date"), "MM/dd/yyyy").cast("timestamp"))).orderBy(desc("date"))
    val from_date = mdf.select(col("date")).first().toString()
    val from = from_date.replace("[","").replace("]","").split("-")
    val final_from_date = from(1) + "/" + from(2) + "/" + from(0)
    val maxTemp2 = seriesRDD.filter(x=>x._2 == phub && x._3==cmts && x._4 == macdomain).map(l=>l._6).max()
    (agg_series_rdd,maxTemp2,final_from_date)
  }


  def arima_model(ts: org.apache.spark.mllib.linalg.Vector, predictions: Int): org.apache.spark.mllib.linalg.Vector={
    val arimaModel = ARIMA.fitModel(1,0,1, ts)
    println("coefficients:" + arimaModel.coefficients.mkString("."))
    val forecast = arimaModel.forecast(ts, predictions)
    forecast
  }

  def generate_date(from: String, number_of_predictions:Int):ArrayBuffer[String]={
    val frm  = from.split("/")
    var month = frm(0).toInt
    var day = frm(1).toInt
    var year  = frm(2).toInt
    val ab = ArrayBuffer[String]()

    for(j<- 1 to number_of_predictions){
      if (day < 31) {
          day += 1
      }else
        {
          day = 1
          month += 1
        }
      if (month >= 12){
        month = 1
      }
      ab += month.toString + "/" + day.toString + "/" + year.toString
    }
    ab
  }

  def filter_trend(list_series:Array[(String,Double)]): (Array[(String,Double)] ,String) = {

      val date = list_series.map(_._1.replace(",","")).toList
      val capacity = list_series.map(_._2.toString.replace(",","").toDouble).toList
      val max_capacity = capacity.max
      var min_capacity = capacity.min
      val index_max = capacity.indexOf(max_capacity)
      val index_min = capacity.indexOf(min_capacity)
      val max_date = date(index_max)
      val min_date= date(index_min)
      println("Min Capacity=" + min_capacity)
      println("Max Capacity=" + max_capacity)
      println("Max Date=" + max_date)
      println("Min Date=" + min_date)
      println("Index of Max=" + index_max)
      println("Index of Min=" + index_min)
      val frm = min_date.split("/")
      val month = frm(0).toInt
      val day = frm(1).toInt
      val to = max_date.split("/")
      val month_to = to(0).toInt
      val day_to = to(1).toInt
      val diff_months = month_to - month
      var number_of_days = 0
      if (diff_months < 2) {
          number_of_days = 30 - day + day_to
      }
      else
      {
          number_of_days = 30 - day + day_to + (30 * diff_months)
      }
      print("Number of days="+ number_of_days)
      val daterange = generate_date(min_date, number_of_days)
      val distance = max_capacity/(number_of_days+4)
      val output = Array.ofDim[Double](number_of_days)
      output(0) = min_capacity
      output(number_of_days-1) = max_capacity
      for(j<-1 to number_of_days-2){
        min_capacity = min_capacity + distance
        output(j) = min_capacity
      }
      println(output.toList)
    var final_return = (daterange zip output).toArray
    (final_return, min_date)

  }

    def main(args: Array[String]): Unit =
    {
    val obj = new configuration()
    val sc = obj.config()
    val inputFile = args(0)
    val outputFile = args(1)
    val cbu = args(2)
    val phub = args(3)
    val cmts = args(4)
    val macdomain=args(5)
    val number_of_predictions=args(6)
    val base = args(7)
    val model = args(8)
    val trend = args(9)
    val textRDD = sc.textFile(inputFile)
    val header = textRDD.first()
    var (seriesRDD,max_temp, from_date) = parse_data(textRDD, header, cbu, phub, cmts ,macdomain, base)
    val list_series = seriesRDD.collect()

    if(trend == "True" & model=="ARIMA")
    {
      val (list_series_x, from_date)=filter_trend(list_series)
      var ts = Vectors.dense(list_series.map(_._2.toDouble))
      val forecast = List(arima_model(ts, number_of_predictions.toInt))
      val predictions = forecast.head.toArray.slice(list_series_x.length, list_series_x.length + number_of_predictions.toInt)
      val daterange = generate_date(from_date, number_of_predictions.toInt)
      val rdd = sc.parallelize(predictions zip daterange)
      rdd.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_predict")
      var forcasting = sc.makeRDD(list_series_x)
      forcasting.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_actual")
      sc.makeRDD(List(max_temp.toString)).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_capacity")
    }
    else if(trend == "False" & model == "ARIMA"){

      var ts = Vectors.dense(list_series.map(_._2.toDouble))
      val forecast = List(arima_model(ts, number_of_predictions.toInt))
      val predictions = forecast.head.toArray.slice(list_series.length, list_series.length + number_of_predictions.toInt)
      val daterange = generate_date(from_date, number_of_predictions.toInt)
      val rdd = sc.parallelize(predictions zip daterange)
      rdd.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_predict")
      var forcasting = sc.makeRDD(list_series)
      forcasting.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_actual")
      sc.makeRDD(List(max_temp.toString)).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_capacity")

    }
    else if(trend == "True" & model == "LSTM"){

      val (list_series_x, from_date)=filter_trend(list_series)
      var forcasting = sc.makeRDD(list_series_x)
      forcasting.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_actual")
      sc.makeRDD(List(max_temp.toString)).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_capacity")

    }
    else if(trend=="False" & model =="LSTM")
    {

        var forcasting = sc.makeRDD(list_series)
        forcasting.map(x => x._1 + "," + x._2).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_actual")
        sc.makeRDD(List(max_temp.toString)).repartition(1).saveAsTextFile(outputFile + "/" + cbu + "/" + phub + "/" + cmts + "/" + macdomain + "/" + "_capacity")
      }
  }
}

