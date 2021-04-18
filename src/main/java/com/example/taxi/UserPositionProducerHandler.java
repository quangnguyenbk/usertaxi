package com.example.taxi;

import com.example.taxi.dto.UserPositionDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import taxi.UserPositionValue;

@Component
public class UserPositionProducerHandler {
    @Autowired
    KafkaProducer<String, UserPositionValue> userPositionProducer;

    public void sendData(int user_id, UserPositionDTO userPositionDTO){
        userPositionProducer.send(new ProducerRecord<String, UserPositionValue>("userposition", user_id+"",
                UserPositionValue.newBuilder().setLatId(userPositionDTO.getLat_id()).setLongId(userPositionDTO.getLong_id()).build()));
    }

}
