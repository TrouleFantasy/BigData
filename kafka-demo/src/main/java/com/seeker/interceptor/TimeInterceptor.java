package com.seeker.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //1.取出具体数据的value
        String value = producerRecord.value();
        //2.创建一个新的PriducerRecord对象并返回
        return new ProducerRecord<String, String>(producerRecord.topic(),producerRecord.partition(),producerRecord.key(),System.currentTimeMillis()+","+value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }
}
