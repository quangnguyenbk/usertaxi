package com.example.taxi;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import taxi.TaxiPositionValue;
import taxi.UserPositionValue;
import taxi.UserTaxiKey;
import taxi.UserTaxiValue;

import java.util.Properties;

@Component
public class Config {
    @Bean
    KafkaProducer<String, TaxiPositionValue> taxiPositionProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<String, TaxiPositionValue> kafkaProducer = new KafkaProducer<String, TaxiPositionValue>(properties);
        return kafkaProducer;
    }

    @Bean
    KafkaProducer<String, UserPositionValue> userPositionProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<String, UserPositionValue> kafkaProducer = new KafkaProducer<String, UserPositionValue>(properties);
        return kafkaProducer;
    }

    @Bean
    KafkaProducer<UserTaxiKey, UserTaxiValue> userTaxiProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrap);
        properties.setProperty("schema.registry.url", Constants.schemaRegistry);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        KafkaProducer<UserTaxiKey, UserTaxiValue> kafkaProducer = new KafkaProducer<UserTaxiKey, UserTaxiValue>(properties);
        return kafkaProducer;
    }
}
