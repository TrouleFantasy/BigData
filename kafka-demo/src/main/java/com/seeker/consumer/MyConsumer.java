package com.seeker.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sun.tools.jconsole.inspector.IconManager;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) throws InterruptedException {
        //1.创建消费者配置信息
        Properties properties=new Properties();
        //2.给配置信息赋值
        //连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        //开启自动提交 (提交offset)
        //offset并不是每次都会提交到kafka的 当开启自动提交的时候 每经过自动提交延迟设定的时间后才会进行提交 在此之前consumer会在内存当中维护offset
        //如果关闭了自动提交 在单次会话中并不会受到影响 但是如果断开重新连接后 consumer就又会从上次提交的offset处开始消费
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //自动提交延迟 毫秒 默认1000
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        //Key.Value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");
        //重置消费者的offset 生效条件：组第一次消费 或者 offset所对应的数据已经不存在
        //此属性表示从保存的offset消费第一次消费属于offset属于0也就是从头消费 默认是latest-从最新的开始消费
        //kafka会在一个新组来消费的时候，下发最大值和最小值的offset 用哪个取决于这个属性
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题 消费者订阅不存在的主题不会创建
        consumer.subscribe(Arrays.asList("kfk"));

        while (true){
            //获取数据
//        for(int i=0;i<5;i++){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
//            Thread.sleep(1000);
            //解析并打印ConsumerRecords
            for (ConsumerRecord<String, String> consumerRecord:consumerRecords){
                System.out.println(consumerRecord.key()+"--"+consumerRecord.value());
            }
        }
        //关闭连接
//        consumer.close();
    }
}
