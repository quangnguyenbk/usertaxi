package com.example.taxi;

import com.example.taxi.dto.TaxiPositionDTO;
import com.example.taxi.dto.UserPositionDTO;
import com.example.taxi.dto.UserTaxiDTO;
import com.example.taxi.dto.UserTaxiPositionDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import taxi.*;

import java.util.List;

@RestController
public class TaxiController {
    @Autowired
    TaxiPositionProducerHandler taxiPositionProducer;
    @Autowired
    UserPositionProducerHandler userPositionProducer;
    @Autowired
    UserTaxiProducerHandler userTaxiProducer;

    @PostMapping(value="/taxi/{taxi_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String taxiPosition(@PathVariable Integer taxi_id,@RequestBody TaxiPositionDTO taxiPositionDTO){
        taxiPositionProducer.sendData(taxi_id, taxiPositionDTO);
        return "success";
    }

    @PostMapping("/user/{user_id}")
    public String userPosition(@PathVariable Integer user_id,@RequestBody UserPositionDTO userPositionDTO){
        userPositionProducer.sendData(user_id, userPositionDTO);
        return "success";
    }

    @PostMapping("/usertaxi")
    public String userTaxi(@RequestBody UserTaxiDTO userTaxiDTO){
        UserTaxiKey userTaxiKey = UserTaxiKey.newBuilder()
                .setTaxiId(userTaxiDTO.getTaxi_id())
                .setUserId(userTaxiDTO.getUser_id())
                .build();
        UserTaxiValue userTaxiValue = UserTaxiValue.newBuilder()
                .setTaxiId(userTaxiDTO.getTaxi_id())
                .setUserId(userTaxiDTO.getUser_id())
                .setIsActive(userTaxiDTO.is_active())
                .build();
        userTaxiProducer.sendData(userTaxiKey, userTaxiValue);
        return "success";
    }

    @GetMapping("/usertaxiposition/taxi/{taxi_id}")
    public ResponseEntity<Object> getUserTaxiPositionByTaxi(@PathVariable int taxi_id){
        KafkaStreamTaxi kafkaStreamTaxi = new KafkaStreamTaxi();
        List<UserTaxiPositionDTO> userTaxiPositionValue = kafkaStreamTaxi.queryByTaxiId(taxi_id);
        return new ResponseEntity<Object>(ResponseEntity.ok(userTaxiPositionValue), HttpStatus.OK);
    }

    @GetMapping("/usertaxiposition/user/{user_id}")
    public ResponseEntity<Object> getUserTaxiPositionByUser(@PathVariable int user_id){
        KafkaStreamTaxi kafkaStreamTaxi = new KafkaStreamTaxi();
        List<UserTaxiPositionDTO> userTaxiPositionValue = kafkaStreamTaxi.queryByUserId(user_id);
        return new ResponseEntity<Object>(ResponseEntity.ok(userTaxiPositionValue), HttpStatus.OK);
    }

}
