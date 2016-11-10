/**
  * Created by jakub on 10.11.2016.
  */
import java.sql.Timestamp

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode

object MyModule extends App {

  LogManager.getLogger("org").setLevel(Level.OFF)

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
      .option("truncate", false)
      .outputMode(OutputMode.Complete)
      .start

  (0 to 5).foreach { x =>
    intsInput.addData(
      TimedEvent(123123, x, new Timestamp(System.currentTimeMillis())),
      TimedEvent(1231234, x+1, new Timestamp(System.currentTimeMillis() + 2000)))
    Thread.sleep(3000)
  }
    query.awaitTermination()


}
