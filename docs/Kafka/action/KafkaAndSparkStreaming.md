# Kafka与Spark Streaming整合

## 概述
Spark Streaming是一个可扩展，高吞吐，容错能力强的实时流式处理处理系统。一般的系统架构图是，数据从一个源点，经过Sparing Streaming处理，最后汇聚到一个系统。Spark Streaming的数据来源可以非常丰富，比如Kafka, Flume, Twitter, ZeroMQ, Kinesis 或者是任何的TCP sockets程序。对于数据的处理，Spark Streaming提供了非常丰富的高级api，例如map，redue，joini和窗口函数等等。数据处理完成后，可以存储到其他地方，比如文件系统，对象存储，数据库。典型的数据处理流程图：
![avatar](./streaming-arch.png)

### 概念简介
RDD：Resilient Distributed Datasets，弹性分部署数据集，支持两种操作：转换（transformation）从现有的数据集创建一个新的数据集；动作（actions）在数据集上运行计算后，返回一个值给驱动程序。在这里简单理解为某个时间片的数据集合即可。
<br><br>
DStream：和RDD概念有点类似，是RDD的集合，代表着整个数据流。简单来说Spark Streaming中的数据量就是DStream，然后每个时间片的数据就是RDD。

## Kafka与Spark Streaming整合

## 整合方式
Kafka与Spark Streaming整合，首先需要从Kafka读取数据过来，读取数据有两种方式
*  方法一：Receiver-based <br>
   这种方式使用一个Receiver接收Kafka的消息，如果使用默认的配置，存在丢数据的风险，因为这种方式会把从kafka接收到的消息存放到Spark的exectors，然后再启动streaming作业区处理，如果exectors挂了，那么消息可能就丢失了。可以通过开启Write Ahead Logs来保证数据的可靠性（Spark 1.2后开始支持），这种方式和大多数存储系统的Write Ahead Logs类似，Spark会把接收到的消息及kafka消息偏移存放到分布式文件系统中（例如HDFS），如果运行期间出现了故障，那么这些信息会被用于故障恢复。
*  方法二：Direc<br>
   这种方式是Spark 1.3引入的，Spark会创建和Kafka partition一一对应的的RDD分区，然后周期性的去轮询获取分区信息，这种方式和Receier-based不一样的是，它不需要Write Ahead Logs，而是通过check point的机制记录kafka的offset，通过check point机制，保证Kafka中的消息不会被遗漏。这种方式相对于第一种方式有多种优点，一是天然支持并发，建了了和Kafka的partition分区对应的RDD分区，第二点是更高效，不需要write ahead logs，减少了写磁盘次数，第三种优点是可以支持exactly-once语义，通过checkpoint机制记录了kafka的offset，而不是通过zk或者kafka来记录offset能避免分布式系统中数据不一致的问题，从而能支持exactly-once语义，当然这里不是说这样就完全是exactly-once了，还需要消费端配合做消息幂等或事物处理。这种模式是较新的模式，推荐使用该模式，第一种方式已经逐步被淘汰。

## 整合示例
下面使用一个示例，展示如何整合Kafka和Spark Streaming，这个例子中，使用一个生产者不断往Kafka随机发送数字，然后通过Spark Streaming统计时间片段内数字之和。<br>
1.往kafka随机发送数字代码：
```
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-demo-client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String,String> producer = new KafkaProducer<>(properties);

        Random random = new Random();
        while (true) {
            int value = random.nextInt(10);
            ProducerRecord<String, String> message =
                    new ProducerRecord<>(topic, value+"");
            producer.send(message, (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" +
                            recordMetadata.offset());
                }
            });
            TimeUnit.SECONDS.sleep(1);
```
2.提交到spark集群代码，用于统计2秒时间间隔的数字之和，这里我们使用的是Direct模式：
```
 def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("StreamingWithKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2)) // 1
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, // 2
      Subscribe[String, String](List(topic), kafkaParams))
    val value = stream.map(record => {
      val intVal = Integer.valueOf(record.value())
      println(intVal)
      intVal
    }).reduce(_+_)

    value.print()

    ssc.start
    ssc.awaitTermination
  }
```
上面代码中1处的代码表示聚合处理的时间分片为2秒，计算两秒内的随机数之和。2处的代码用于指定spark执行器上面的kafka consumer分区分配策略，一共有三种类型，PreferConsistent是最常用的，表示订阅主题的分区均匀分配到执行器上面，然后还有PreferBrokers，这种机制是优先分配到和broker相同机器的执行器上，还有一种是PreferFixed，这种是手动配置，用的比较少。上面程序每次计算2秒时间间隔内的数字之和，输入会类似如下：
```
3
4
...
```
