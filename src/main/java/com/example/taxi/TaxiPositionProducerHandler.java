package com.example.taxi;

import com.example.taxi.dto.TaxiPositionDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import taxi.TaxiPositionValue;

@Service
public class TaxiPositionProducerHandler {
    @Autowired
    KafkaProducer<String, TaxiPositionValue> taxiPositionProducer;

    public void sendData(int taxi_id, TaxiPositionDTO taxiPositionDTO){
        taxiPositionProducer.send(new ProducerRecord<String, TaxiPositionValue>("taxiposition", taxi_id+"", TaxiPositionValue.newBuilder()
                .setLongId(taxiPositionDTO.getLong_id()).setLatId(taxiPositionDTO.getLat_id()).build()));
    }
}
