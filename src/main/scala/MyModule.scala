/**
  * Created by jakub on 10.11.2016.
  */
import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode

object MyModule extends App {

    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").master("local").getOrCreate()
    case class TimedEvent(id: Long, value: Int, timestamp: Timestamp)

    import spark.implicits._

    implicit val sqlContext: SQLContext = spark.sqlContext

    val intsInput = MemoryStream[TimedEvent]

    val query = intsInput.toDS
      .groupBy(window('timestamp, "10 seconds", "5 seconds"))
      .agg(sum('value))
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete)
      .start

    intsInput.addData(
      TimedEvent(123123, 5, new Timestamp(System.currentTimeMillis())),
      TimedEvent(123123, 1, new Timestamp(System.currentTimeMillis())),
      TimedEvent(124412, 5, new Timestamp(System.currentTimeMillis())),
      TimedEvent(141, 3, new Timestamp(System.currentTimeMillis() + 20000)))
    query.awaitTermination()


}
