***Time Series Analysis***

Business Objective: Predictive Model around Capacity Data. Forecast Upstream and Downstream traffic to ensure if its
within or will cross capacity threshold in the few coming days.

To Run Locally Spark-Code

```
spark-submit --class timeseries.analysis --jars target/scala-2.11/timeseries_2.11-0.1.jar sparkts-0.4.0.jar  data/ output.csv 'SWO' 'PG07' 'cmts77.ktgc' 'CATV-MAC 29' 20 'peak' 'LSTM'

```