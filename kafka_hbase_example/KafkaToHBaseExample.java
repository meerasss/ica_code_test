
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class KafkaToHBaseExample {

    private static final String KAFKA_TOPIC = "test_kafka_topic";
    private static final String HBASE_TABLE = "test_hbase_table";
    private static final String HBASE_COLUMN_FAMILY = "cf";

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToHBaseExample");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_bootstrap_servers");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Set<String> topics = Collections.singleton(KAFKA_TOPIC);

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // Extract key-value pairs from Kafka messages
        JavaPairDStream<String, String> keyValuePairs = kafkaStream.mapToPair(record ->
                new Tuple2<>(record.key(), record.value())
        );

        // Save data to HBase
        keyValuePairs.foreachRDD((VoidFunction<JavaPairRDD<String, String>>) rdd -> {
            rdd.foreachPartition(records -> {
                org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
                try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                     Table table = connection.getTable(TableName.valueOf(HBASE_TABLE_NAME))) {

                    while (records.hasNext()) {
                        Tuple2<String, String> record = records.next();
                        String key = record._1();
                        String value = record._2();

                        // Modify the column name and value as per the HBase schema,(sample hiven in Assumption.txt)
                        Put put = new Put(Bytes.toBytes(key));
                        put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("cust_id"), Bytes.toBytes(value));
						put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("cust_name"), Bytes.toBytes(value));
						put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("country"), Bytes.toBytes(value));

                        table.put(put);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}

