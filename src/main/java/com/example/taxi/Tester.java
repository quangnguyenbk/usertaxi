package com.example.taxi;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import taxi.TaxiPositionValue;
import taxi.UserPositionValue;
import taxi.UserTaxiKey;
import taxi.UserTaxiValue;

import java.util.Properties;

public class Tester {
    public static void  main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<String, UserPositionValue> kafkaProducer = new KafkaProducer<String, UserPositionValue>(properties);

        while (true){
            UserPositionValue userPositionValue = UserPositionValue.newBuilder()
                    .setLatId(randomLatLong(10,9) + "")
                    .setLongId(randomLatLong(10,9)+"")
                    .build();
            int userId = randomId(3,2);
            kafkaProducer.send(new ProducerRecord<String, UserPositionValue>("userposition", userId+"", userPositionValue));

//            //
            send1();
//
//            //
            send2();

            Thread.sleep(50000);
        }
    }

    public static double randomLatLong(int max, int min){
        return  ((Math.random() * (max - min)) + min);
    }

    public static int randomId(int max, int min){
        return (int) ((Math.random() * (max - min)) + min);
    }

    public static void send1(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<String, TaxiPositionValue> kafkaProducer = new KafkaProducer<String, TaxiPositionValue>(properties);
        TaxiPositionValue taxiPositionValue = TaxiPositionValue.newBuilder()
                .setLatId(randomLatLong(10,9) + "")
                .setLongId(randomLatLong(10,9)+"")
                .build();
        int taxiId = randomId(15,14);
        kafkaProducer.send(new ProducerRecord<String, TaxiPositionValue>("taxiposition", taxiId+"", taxiPositionValue));
    }

    public static void send2(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<UserTaxiKey, UserTaxiValue> kafkaProducer = new KafkaProducer<UserTaxiKey, UserTaxiValue>(properties);
        int userId = randomId(3,2);
        int taxiId = randomId(15,14);
        UserTaxiValue userTaxiValue = UserTaxiValue.newBuilder()
                .setTaxiId(taxiId)
                .setUserId(userId)
                .setIsActive(true)
                .build();
        UserTaxiKey userTaxiKey = UserTaxiKey.newBuilder()
                .setTaxiId(taxiId)
                .setUserId(userId)
                .build();
        kafkaProducer.send(new ProducerRecord<UserTaxiKey, UserTaxiValue>("usertaxi", userTaxiKey, userTaxiValue));
    }
}
