package com.example.flink.model

case class OrderEvent(
    order_id: String,
    order_status: String,
    create_time: String,
    operate_time: String,
    sku_id: String,
    sku_num: Int,
    order_price: Double
)
