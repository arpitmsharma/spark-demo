package com.github.arpitmsharma;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class SparkDemo {
    public static final String APP_NAME = "spark-demo";
    public static final String MASTER = "local";

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(1000));
//        javaStreamingContext.sparkContext().setLogLevel("ERROR");
        Map<String, Object> kafkaParams = kafkaParams();

        Collection<String> topics = Arrays.asList("temperature-event");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        stream.map(record -> record.key() + ":" + record.value()).print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        SpringApplication.run(SparkDemo.class, args);
    }

    private static Map<String, Object> kafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", String.valueOf(Math.random()));
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
}
