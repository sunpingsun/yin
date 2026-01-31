# Flink Order Monitor - 任务 D 解决方案

本项目完整实现了“任务 D：数据采集与实时计算”的所有要求。项目基于 Flink 1.14.0 和 Scala 2.11 构建，集成了 Flume 数据采集、Kafka 消息队列、Redis 实时存储以及 HDFS 备份功能。

## 目录
1. [项目结构](#1-项目结构)
2. [环境准备](#2-环境准备)
3. [Flume 数据采集配置](#3-flume-数据采集配置)
4. [Flink 任务编译与运行](#4-flink-任务编译与运行)
5. [结果验证](#5-结果验证)
6. [常见问题排查](#6-常见问题排查)

---

## 1. 项目结构

```text
flink-kafka-wordcount/
├── pom.xml                                 # Maven 配置文件
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── flume-task1-kafka.conf      # 子任务一：仅写入 Kafka
│   │   │   └── flume-task2-multiplexing.conf # 子任务二：多路复用写入 Kafka + HDFS
│   │   └── scala/
│   │       └── com/example/flink/
│   │           ├── OrderMonitor.scala      # Flink 主程序
│   │           ├── model/
│   │           │   └── OrderEvent.scala    # 实体类
│   │           └── util/
│   │               └── RedisSink.scala     # Redis 工具类
└── README.md                               # 说明文档
```

## 2. 环境准备

在开始之前，请确保服务器 (`192.168.12.41`) 上的以下基础服务已正常运行。

- **Kafka**: Port 9092
- **Redis**: Port 6379
- **HDFS**: Namenode Port 8020 (或 9000，视具体配置)

---

## 3. Flume 数据采集配置

Flume 负责从端口监听数据。按照任务要求，分为两个阶段进行。

### 3.1 启动数据模拟源
在发送数据的机器上（通常为本地 localhost），启动 Netcat 监听端口 `10050`：
```bash
# 保持该窗口开启，后续在此输入测试数据
nc -lk 10050
```

### 3.2 子任务一：实时数据采集 (仅 Kafka)
使用 `flume-task1-kafka.conf` 配置文件。此配置仅将数据 sink 到 Kafka。

```bash
# 上传配置文件并运行 Agent
flume-ng agent --name a1 --conf conf --conf-file flume-task1-kafka.conf -Dflume.root.logger=INFO,console
```

**验证步骤 (Task 1):**
1. 在 `nc` 窗口输入数据。
2. 使用 Kafka 自带消费者查看数据：
   ```bash
   /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server 192.168.12.41:9092 --topic order --from-beginning
   ```
3. 截图前 2 条数据结果。

### 3.3 子任务二：多路复用模式 (Kafka + HDFS)
使用 `flume-task2-multiplexing.conf` 配置文件。此配置将数据双写到 Kafka 和 HDFS。

```bash
# 停止之前的 Agent，运行新的 Agent
flume-ng agent --name a1 --conf conf --conf-file flume-task2-multiplexing.conf -Dflume.root.logger=INFO,console
```

**验证步骤 (Task 2):**
1. 继续在 `nc` 窗口输入数据。
2. 验证 Kafka 依然能收到数据。
3. 验证 HDFS 备份：
   ```bash
   hdfs dfs -ls /user/test/flumebackup
   hdfs dfs -text /user/test/flumebackup/events-*.txt | head -n 2
   ```
4. 截图命令与结果。

---

## 4. Flink 任务编译与运行

Flink 任务负责消费 Kafka 数据并计算结果写入 Redis。

### 4.1 编译打包
```bash
mvn clean package
```
生成：`target/flink-kafka-wordcount-1.0-SNAPSHOT.jar`。

### 4.2 提交任务
```bash
# 提交到 Yarn 集群
flink run -m yarn-cluster -yjm 1024 -ytm 1024 -c com.example.flink.OrderMonitor ./flink-kafka-wordcount-1.0-SNAPSHOT.jar
```

---

## 5. 结果验证

### 5.1 发送测试数据
```json
{"order_id":"1","order_status":"1001","create_time":"2023-10-01 10:00:00","sku_id":"1","sku_num":10,"order_price":100.0}
{"order_id":"2","order_status":"1002","create_time":"2023-10-01 10:00:01","sku_id":"2","sku_num":5,"order_price":50.0}
{"order_id":"3","order_status":"1004","create_time":"2023-10-01 10:00:02","sku_id":"1","sku_num":2,"order_price":100.0}
{"order_id":"4","order_status":"1003","create_time":"2023-10-01 10:00:03","sku_id":"3","sku_num":1,"order_price":200.0}
```

### 5.2 验证 Redis 结果
```bash
redis-cli -h 192.168.12.41
```

```redis
# 1. 查看总订单数 (排除取消订单 1003)
get totalcount

# 2. 查看销量前3商品
get top3itemamount

# 3. 查看销售额前3商品
get top3itemconsumption
```

---

## 6. 常见问题排查

1.  **HDFS 连接失败**:
    - 检查 `flume-task2-multiplexing.conf` 中的端口 (8020) 是否与实际环境一致。

2.  **Redis 连接失败**:
    - 检查 Redis 是否绑定了 `192.168.12.41` 或 `0.0.0.0`，默认 `127.0.0.1` 无法被外部连接。

3.  **数据无输出**:
    - Flink 任务使用了 Watermark (5s 延迟)。请确保输入数据的 `create_time` 持续递增。
