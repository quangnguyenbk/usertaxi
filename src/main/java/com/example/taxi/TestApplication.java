package com.example.taxi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class TestApplication {

	public static void main(String[] args){
		SpringApplication.run(TestApplication.class, args);
		KafkaStreamTaxi.run();
	}

}
