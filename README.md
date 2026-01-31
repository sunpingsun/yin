# Flink Order Monitor - 任务 D 解决方案

本项目实现了“任务 D：数据采集与实时计算”的所有要求，包括 Flume 数据采集与 Flink 实时计算。

## 1. 环境准备 (Environment Preparation)

请确保 `192.168.12.41` 服务器上运行以下服务：
- **Kafka** (Port 9092)
- **Redis** (Port 6379)
- **HDFS** (Namenode Port 8020)

### 创建 Kafka Topic
```bash
/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.12.41:9092 --replication-factor 1 --partitions 4 --topic order
```

## 2. 启动数据采集 (Flume)

### 启动数据生成器 (Netcat)
在发送数据的机器上运行（通常是 localhost）：
```bash
nc -lk 10050
```

### 启动 Flume Agent
配置文件位于 `src/main/resources/flume-job.conf`。

```bash
# 将 flume-job.conf 上传到 Flume 安装目录，然后运行：
flume-ng agent --name a1 --conf conf --conf-file flume-job.conf -Dflume.root.logger=INFO,console
```

此时，在 `nc` 终端输入的数据将被发送到 Kafka 的 `order` Topic，并备份到 HDFS。

**测试数据格式示例 (JSON):**
请逐行输入以下 JSON 数据：
```json
{"order_id":"1","order_status":"1001","create_time":"2023-10-01 10:00:00","sku_id":"1","sku_num":10,"order_price":100.0}
{"order_id":"2","order_status":"1002","create_time":"2023-10-01 10:00:01","sku_id":"2","sku_num":5,"order_price":50.0}
```

## 3. 运行 Flink 任务

### 编译打包
在项目根目录运行：
```bash
mvn clean package
```
成功后将在 `target/` 目录下生成 `flink-kafka-wordcount-1.0-SNAPSHOT.jar`。

### 提交任务
将 Jar 包上传到 Flink 客户端节点 (192.168.12.41) 并提交：

```bash
# 提交到 Yarn 集群
flink run -m yarn-cluster -yjm 1024 -ytm 1024 -c com.example.flink.OrderMonitor ./flink-kafka-wordcount-1.0-SNAPSHOT.jar
```

## 4. 结果验证 (Verification)

### 查看 Redis 结果
在 Redis 服务器 (192.168.12.41) 上运行：

```bash
redis-cli
> get totalcount
> get top3itemamount
> get top3itemconsumption
```

### 查看 HDFS 备份
```bash
hdfs dfs -ls /user/test/flumebackup
hdfs dfs -text /user/test/flumebackup/events-*.txt | head -n 2
```
