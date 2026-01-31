package com.example.flink

import java.text.SimpleDateFormat
import java.util.Properties
import java.time.Duration
import java.math.{BigDecimal, RoundingMode}

import com.example.flink.model.OrderEvent
import com.example.flink.util.RedisSink
import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object OrderMonitor {
  def main(args: Array[String]): Unit = {
    // 1. Env Setup
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Using EventTime
    env.setParallelism(1) // Simplify for exam (Global Top 3 needs single parallelism eventually anyway)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.12.41:9092")
    properties.setProperty("group.id", "order-monitor-group")
    properties.setProperty("auto.offset.reset", "latest")

    // 2. Source
    val kafkaSource = new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties)
    val rawStream = env.addSource(kafkaSource)

    // 3. Parse & Watermark
    val orderStream = rawStream.map(json => {
      try {
        new Gson().fromJson(json, classOf[OrderEvent])
      } catch {
        case _: Exception => null
      }
    }).filter(_ != null)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness[OrderEvent](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = {
              try {
                val createTime = sdf.parse(element.create_time).getTime
                var operateTime = 0L
                if (element.operate_time != null && element.operate_time.trim.nonEmpty) {
                  operateTime = sdf.parse(element.operate_time).getTime
                }
                Math.max(createTime, operateTime)
              } catch {
                case _: Exception => recordTimestamp
              }
            }
          })
      )

    val sideOutputTag = OutputTag[OrderEvent]("order-detail")

    // 4. Split Streams
    val mainStream = orderStream.process(new ProcessFunction[OrderEvent, OrderEvent] {
      override def processElement(value: OrderEvent, ctx: ProcessFunction[OrderEvent, OrderEvent]#Context, out: Collector[OrderEvent]): Unit = {
        // Emit to Side Output for Details
        ctx.output(sideOutputTag, value)
        // Emit to Main for Count
        out.collect(value)
      }
    })

    // 5. Task 1: Order Count
    val validStatuses = Set("1001", "1002", "1004")
    mainStream
      .filter(e => validStatuses.contains(e.order_status))
      .map(_ => ("totalcount", 1L))
      .keyBy(_._1)
      .sum(1)
      .map(t => ("totalcount", t._2.toString))
      .addSink(new RedisSink("192.168.12.41", 6379))

    // 6. Side Stream Logic
    val sideStream = mainStream.getSideOutput(sideOutputTag)

    // Task 2: Top 3 Volume
    sideStream
      .keyBy(_ => "global-vol") // Route all to one instance
      .process(new Top3VolumeProcessFunction())
      .addSink(new RedisSink("192.168.12.41", 6379))

    // Task 3: Top 3 Consumption
    sideStream
      .keyBy(_ => "global-rev") // Route all to one instance
      .process(new Top3RevenueProcessFunction())
      .addSink(new RedisSink("192.168.12.41", 6379))

    env.execute("Order Monitor Job")
  }

  // --- Process Functions ---

  class Top3VolumeProcessFunction extends KeyedProcessFunction[String, OrderEvent, (String, String)] {
    // State: SKU_ID -> Count
    lazy val countState: MapState[String, Int] = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Int]("volume-state", classOf[String], classOf[Int])
    )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
      val current = if (countState.contains(value.sku_id)) countState.get(value.sku_id) else 0
      countState.put(value.sku_id, current + value.sku_num)

      // Calculate Top 3
      val allItems = countState.entries().asScala.map(e => (e.getKey, e.getValue)).toList
      val top3 = allItems.sortBy(-_._2).take(3)

      // Format: [1:700,42:500,41:100]
      val formatted = top3.map { case (id, num) => s"$id:$num" }.mkString("[", ",", "]")

      out.collect(("top3itemamount", formatted))
    }
  }

  class Top3RevenueProcessFunction extends KeyedProcessFunction[String, OrderEvent, (String, String)] {
    // State: SKU_ID -> Revenue
    lazy val revenueState: MapState[String, Double] = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Double]("revenue-state", classOf[String], classOf[Double])
    )

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
      val revenue = value.sku_num * value.order_price
      val current = if (revenueState.contains(value.sku_id)) revenueState.get(value.sku_id) else 0.0
      revenueState.put(value.sku_id, current + revenue)

      // Calculate Top 3
      val allItems = revenueState.entries().asScala.map(e => (e.getKey, e.getValue)).toList
      val top3 = allItems.sortBy(-_._2).take(3)

      // Format: [1:10020.2,42:4540.0,12:540]
      // Use BigDecimal to avoid scientific notation
      val formatted = top3.map { case (id, rev) =>
        // Convert to BigDecimal for clean string representation
        val bd = BigDecimal.valueOf(rev).setScale(1, RoundingMode.HALF_UP).stripTrailingZeros()
        // If it's an integer like 540.0, stripTrailingZeros might make it 5.4E2 if not careful,
        // but toPlainString works well.
        // Actually prompt example shows "10020.2", "4540.0", "540".
        // "4540.0" implies keeping one decimal if it's .0? Or just generic float formatting.
        // Let's use generic toPlainString after setting scale if needed, or just `toString` if simple.
        // Safer: `BigDecimal.valueOf(rev).toPlainString`
        s"$id:${BigDecimal.valueOf(rev).toPlainString}"
      }.mkString("[", ",", "]")

      out.collect(("top3itemconsumption", formatted))
    }
  }
}
