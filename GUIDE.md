# Task D: Data Collection and Real-time Computation Guide

## 1. Environment Preparation

Ensure the following services are running on `192.168.12.41`:
- Kafka (Port 9092)
- Redis (Port 6379)
- HDFS (Namenode Port 8020)

### Create Kafka Topic
```bash
/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.12.41:9092 --replication-factor 1 --partitions 4 --topic order
```

## 2. Start Data Collection (Flume)

### Start Data Generator (Netcat)
On the machine acting as the source (localhost):
```bash
nc -lk 10050
```

### Start Flume Agent
Run the Flume agent using the configuration file provided in `src/main/resources/flume-job.conf`.
Assuming you are in the project root:

```bash
# Upload flume-job.conf to the server where Flume is installed, then run:
flume-ng agent --name a1 --conf conf --conf-file flume-job.conf -Dflume.root.logger=INFO,console
```

Now, any data typed into the `nc` terminal will be sent to Kafka topic `order` and backed up to HDFS.

**Data Format Example (JSON):**
Input one JSON object per line into Netcat:
```json
{"order_id":"1","order_status":"1001","create_time":"2023-10-01 10:00:00","sku_id":"1","sku_num":10,"order_price":100.0}
{"order_id":"2","order_status":"1002","create_time":"2023-10-01 10:00:01","sku_id":"2","sku_num":5,"order_price":50.0}
```

## 3. Run Flink Job

### Compile & Package
In the project root:
```bash
mvn clean package
```
This will generate `target/flink-kafka-wordcount-1.0-SNAPSHOT.jar`.

### Submit Job
Upload the jar to the Flink client node (192.168.12.41) and submit:

```bash
flink run -m yarn-cluster -yjm 1024 -ytm 1024 -c com.example.flink.OrderMonitor ./flink-kafka-wordcount-1.0-SNAPSHOT.jar
```
*(Note: Remove `-m yarn-cluster` and yarn options if running in Standalone mode)*

## 4. Verification

### Check Redis Results
On the Redis server (192.168.12.41):

```bash
redis-cli
> get totalcount
> get top3itemamount
> get top3itemconsumption
```

### Check HDFS Backup
```bash
hdfs dfs -ls /user/test/flumebackup
hdfs dfs -text /user/test/flumebackup/events-*.txt | head -n 2
```
