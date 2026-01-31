package com.example.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. Get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. Configure Kafka Consumer
    val properties = new Properties()
    // Connecting to the Kafka broker on the specified server
    properties.setProperty("bootstrap.servers", "192.168.12.41:9092")
    // Group ID for the consumer
    properties.setProperty("group.id", "flink-wordcount-group")
    // Start from the latest offset if no offset is committed
    properties.setProperty("auto.offset.reset", "latest")

    val topic = "wordcount_input"

    // Create the FlinkKafkaConsumer
    val kafkaSource = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      properties
    )

    // 3. Define the data processing pipeline
    val stream = env.addSource(kafkaSource)
      .flatMap(line => line.split("\\s+")) // Split lines into words by whitespace
      .filter(_.nonEmpty)                   // Filter out empty strings
      .map(word => (word, 1))               // Map each word to a tuple (word, 1)
      .keyBy(_._1)                          // Group by the word
      .sum(1)                               // Sum the counts

    // 4. Print the result to stdout (logs)
    stream.print()

    // 5. Execute the job
    env.execute("Flink Kafka WordCount Job")
  }
}
