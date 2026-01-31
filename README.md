# Flink Kafka WordCount

这是一个使用 Flink 1.14.0 和 Scala 2.12 编写的 Kafka WordCount 示例程序。
程序会从 Kafka 读取数据，进行实时 WordCount 统计，并将结果打印到 Standard Output (Web UI Log)。

## 环境要求

- Flink 1.14.0
- Kafka 2.4.1
- Scala 2.12
- Java 8 (JDK 1.8)

## 编译打包

如果需要重新编译，请在项目根目录下运行：

```bash
mvn clean package
```

编译成功后，会在 `target/` 目录下生成 `flink-kafka-wordcount-1.0-SNAPSHOT.jar`。

## 运行步骤 (在 192.168.12.41 上)

### 1. 准备 Kafka Topic

确保 Kafka 已经启动，并创建一个名为 `wordcount_input` 的 Topic (如果尚未创建)：

```bash
# 假设 Kafka 安装在 /usr/local/kafka (请根据实际路径调整)
# 创建 Topic
/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.12.41:9092 --replication-factor 1 --partitions 1 --topic wordcount_input

# 查看 Topic 是否创建成功
/usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server 192.168.12.41:9092
```

### 2. 启动 Flink 任务

将编译好的 jar 包上传到服务器 `192.168.12.41`。

提交 Flink 任务：

```bash
# 提交任务到 Flink 集群
flink run -c com.example.flink.WordCount ./flink-kafka-wordcount-1.0-SNAPSHOT.jar
```

*注意：如果 Flink 是以 Standalone 模式运行，请确保环境变量 `FLINK_HOME` 已配置，且 `flink` 命令在 PATH 中。*

### 3. 生产测试数据

启动一个 Kafka Console Producer 向 Topic 发送数据：

```bash
/usr/local/kafka/bin/kafka-console-producer.sh --broker-list 192.168.12.41:9092 --topic wordcount_input
```

在控制台中输入一些单词，例如：
```text
hello world
hello flink
flink kafka
```

### 4. 查看结果

由于代码中使用的是 `stream.print()`，结果会输出到 TaskManager 的标准输出日志中。
你可以通过 Flink Web UI 查看 TaskManager 的 Stdout 日志，或者直接在服务器上查看日志文件 (通常在 `FLINK_HOME/log` 目录下)。

输出示例：
```text
(hello,1)
(world,1)
(hello,2)
(flink,1)
(flink,2)
(kafka,1)
```
