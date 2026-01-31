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
├── pom.xml                         # Maven 配置文件，包含 Flink, Kafka, Redis, Gson 依赖
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── flume-job.conf      # Flume 任务配置文件 (Netcat -> Kafka + HDFS)
│   │   └── scala/
│   │       └── com/example/flink/
│   │           ├── OrderMonitor.scala    # Flink 主程序 (包含所有业务逻辑：清洗、侧流、统计)
│   │           ├── model/
│   │           │   └── OrderEvent.scala  # 订单数据实体类
│   │           └── util/
│   │               └── RedisSink.scala   # 自定义 Redis Sink (适配 Flink 1.14)
└── README.md                       # 项目说明文档
```

## 2. 环境准备

在开始之前，请确保服务器 (`192.168.12.41`) 上的以下基础服务已正常运行。

### 2.1 检查 Kafka
Kafka 必须在端口 `9092` 运行。
```bash
# 检查 Kafka 进程
jps | grep Kafka

# 创建 Topic (如果尚未创建)
/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.12.41:9092 --replication-factor 1 --partitions 4 --topic order

# 查看 Topic 列表确认
/usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server 192.168.12.41:9092
```

### 2.2 检查 Redis
Redis 必须在端口 `6379` 运行。
```bash
# 检查端口监听
netstat -tulpn | grep 6379

# 测试连接
redis-cli -h 192.168.12.41 ping
# 输出 PONG 表示正常
```

### 2.3 检查 HDFS
HDFS Namenode 必须在端口 `8020` (或配置的端口) 运行。
```bash
# 检查 HDFS 目录是否存在
hdfs dfs -ls /user/test/
# 如果报错目录不存在，请先创建
hdfs dfs -mkdir -p /user/test/flumebackup
```

---

## 3. Flume 数据采集配置

Flume 负责从端口监听数据，并双写到 Kafka 和 HDFS。

### 3.1 启动数据模拟源
在发送数据的机器上（通常为本地 localhost），启动 Netcat 监听端口 `10050`：
```bash
# 保持该窗口开启，后续在此输入测试数据
nc -lk 10050
```

### 3.2 启动 Flume Agent
1. 找到项目中的配置文件：`src/main/resources/flume-job.conf`。
2. 将该文件上传到运行 Flume 的服务器。
3. 启动 Flume Agent (假设 Agent 名称为 `a1`)：

```bash
# 请根据实际 Flume 安装路径调整命令
flume-ng agent --name a1 --conf conf --conf-file flume-job.conf -Dflume.root.logger=INFO,console
```

**配置说明：**
- **Source**: Netcat (`localhost:10050`)
- **Channel**: Memory Channel
- **Sink 1**: Kafka Sink (Topic: `order`)
- **Sink 2**: HDFS Sink (Path: `/user/test/flumebackup`)
- **Selector**: Replicating (复制模式，确保数据同时发往两个 Sink)

---

## 4. Flink 任务编译与运行

### 4.1 编译打包
在开发环境（项目根目录）执行 Maven 打包命令。确保 JDK 版本为 1.8，Maven 版本为 3.x。

```bash
mvn clean package
```

编译成功后，将在 `target/` 目录下生成 `flink-kafka-wordcount-1.0-SNAPSHOT.jar`。

### 4.2 提交任务到集群
将生成的 Jar 包上传到 Flink 客户端节点（`192.168.12.41`）。

使用 `flink run` 命令提交任务。建议使用 Per-Job 模式（Yarn Cluster）以方便资源回收：

```bash
# 提交任务
flink run -m yarn-cluster -yjm 1024 -ytm 1024 -c com.example.flink.OrderMonitor ./flink-kafka-wordcount-1.0-SNAPSHOT.jar
```

*参数说明：*
- `-m yarn-cluster`: 在 Yarn 上启动集群
- `-yjm 1024`: JobManager 内存 1024MB
- `-ytm 1024`: TaskManager 内存 1024MB
- `-c com.example.flink.OrderMonitor`: 指定主类入口

---

## 5. 结果验证

### 5.1 发送测试数据
在 `nc -lk 10050` 的窗口中输入以下 JSON 数据（每行一条）：

```json
{"order_id":"1","order_status":"1001","create_time":"2023-10-01 10:00:00","sku_id":"1","sku_num":10,"order_price":100.0}
{"order_id":"2","order_status":"1002","create_time":"2023-10-01 10:00:01","sku_id":"2","sku_num":5,"order_price":50.0}
{"order_id":"3","order_status":"1004","create_time":"2023-10-01 10:00:02","sku_id":"1","sku_num":2,"order_price":100.0}
{"order_id":"4","order_status":"1003","create_time":"2023-10-01 10:00:03","sku_id":"3","sku_num":1,"order_price":200.0}
```
*(注意：订单状态 1003 为“取消订单”，不会计入 totalcount，但会通过侧流计入商品销量统计)*

### 5.2 验证 Redis 结果 (Task 1, 2, 3)
连接 Redis 查看计算结果：

```bash
redis-cli -h 192.168.12.41
```

执行查询命令：
```redis
# 1. 查看总订单数 (Task 1)
# 预期结果：2 (id:1 和 id:2, id:3 是 1004 也算，但 id:4 是 1003 不算。如果上述数据都输入，应为 3)
get totalcount

# 2. 查看销量前3商品 (Task 2)
# 格式示例: [1:12,2:5,3:1]
get top3itemamount

# 3. 查看销售额前3商品 (Task 3)
# 格式示例: [1:1200.0,3:200.0,2:250.0] (注意排序)
get top3itemconsumption
```

### 5.3 验证 HDFS 备份 (Flume Task)
检查 HDFS 上是否有 Flume 生成的文件：

```bash
hdfs dfs -ls /user/test/flumebackup
```

查看文件内容的前 2 条：
```bash
hdfs dfs -text /user/test/flumebackup/events-*.txt | head -n 2
```
请截图此命令的执行结果作为“任务D提交结果”。

---

## 6. 常见问题排查

1.  **Redis 连接失败**:
    - 检查 `OrderMonitor.scala` 中的 Redis IP 是否为 `192.168.12.41`。
    - 检查 Redis 配置文件 `redis.conf` 中的 `bind` 是否允许远程连接 (或设为 `0.0.0.0`)。

2.  **Flink 任务报错 "ClassNotFoundException: redis.clients.jedis.Jedis"**:
    - 请确认 `pom.xml` 中 `maven-shade-plugin` 已正确配置，能够将 Jedis 依赖打包进 Fat Jar。
    - 可以通过 `jar -tf target/flink-kafka-wordcount-1.0-SNAPSHOT.jar | grep Jedis` 检查 Jar 包内容。

3.  **Flume 报错 "KafkaSink Exception"**:
    - 检查 Kafka 服务是否正常。
    - 检查 `flume-job.conf` 中的 Topic 名称是否已创建。

4.  **数据无输出**:
    - Flink 任务使用了 EventTime 和 Watermark (允许延迟 5s)。请确保输入数据的 `create_time` 是递增的，或者输入足够多的数据以推动 Watermark 前进。
