package com.example.flink.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.Jedis

class RedisSink(host: String, port: Int) extends RichSinkFunction[(String, String)] {
  var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    try {
      jedis = new Jedis(host, port)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def invoke(value: (String, String), context: SinkFunction.Context): Unit = {
    try {
      if (jedis != null) {
        jedis.set(value._1, value._2)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}
