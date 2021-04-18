package com.example.taxi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import taxi.UserTaxiKey;
import taxi.UserTaxiValue;

@Component
public class UserTaxiProducerHandler {
    @Autowired
    KafkaProducer<UserTaxiKey, UserTaxiValue> userTaxiProducer;

    public void sendData(UserTaxiKey userTaxiKey, UserTaxiValue userTaxiValue){
        userTaxiProducer.send(new ProducerRecord<UserTaxiKey, UserTaxiValue>("usertaxi", userTaxiKey, userTaxiValue));
    }
}
