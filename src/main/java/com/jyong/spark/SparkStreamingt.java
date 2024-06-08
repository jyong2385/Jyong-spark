package com.jyong.spark;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jyong on 2020/11/22 10:33
 */
public class SparkStreamingt {


    private static Logger logger = LoggerFactory.getLogger(SparkStreamingt.class);
    public static void main(String[] args) throws InterruptedException {

        String topic="mysql_topic_person";
        List<String> topics = Arrays.asList(topic);

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","node01:9092,node02: ,node03:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id","topic_person1");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);


        SparkConf sparkConf = new SparkConf().setAppName("consume_person_topic").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("warn");
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));

        //读取kafka数据
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
        directStream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD, Time time) throws Exception {
                consumerRecordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                    @Override
                    public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                            while(consumerRecordIterator.hasNext()){
                                String str = consumerRecordIterator.next().value();
                                JSONObject jsonObject = JSONUtil.parseObj(str);
                                String id = jsonObject.getStr("id");
                                String name = jsonObject.getStr("name");
                                String sex = jsonObject.getStr("sex");
                                String amount = jsonObject.getStr("amount");
                                String address = jsonObject.getStr("address");
                                String birthday = jsonObject.getStr("birthday");
                                String creditcard = jsonObject.getStr("creditcard");
                                String phone = jsonObject.getStr("phone");
                                String job = jsonObject.getStr("job");
                                String education = jsonObject.getStr("education");
                                String marry = jsonObject.getStr("marry");
                                HashMap<String, String> map = Maps.newHashMap();
                                map.put("id",id);
                                map.put("name",name);
                                map.put("sex",sex);
                                map.put("amount",amount);
                                map.put("address",address);
                                map.put("birthday",birthday);
                                map.put("creditcard",creditcard);
                                map.put("phone",phone);
                                map.put("job",job);
                                map.put("education",education);
                                map.put("marry",marry);
                                String indexName = "jyong-person-consume-kafka-"+DateUtil.format(new Date(),"yyyyMMdd");

                                String url = "http://node01:9200/" + indexName+"/_doc/"+id;
                                boolean idOk = HttpRequest.post(url)
                                        .timeout(1000 * 60)
                                        .body(JSONUtil.toJsonPrettyStr(map))
                                        .execute().isOk();
                                logger.warn("=============数据处理完毕,落入ELASTICSEARCH,序号：["+id+"]==============status:"+idOk);
                            }
                    }
                });
            }
        });
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public static  boolean exisitIndexName(String url){
        return  HttpRequest.get(url+"/_search")
                .timeout(5000*5)
                .execute()
                .body().contains("error");

    }


}
