package com.example.producer;

import com.example.demo.avro.Trade;
import com.example.demo.avro.Vehicle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@EnableKafka
@SpringBootApplication
@EnableScheduling
public class ProducerApplication {

	long tradeCount = 0;
	long vehicleCount = 10;

	@Autowired
	KafkaTemplate<Long, Trade> kafkaTemplateTrade;

	@Autowired
	KafkaTemplate<Long, Vehicle> kafkaTemplateVehicle;

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}



	@Scheduled(fixedRate = 1000)
	public void produce() {

		if (vehicleCount > 0) {
			Vehicle vehicle = Vehicle.newBuilder()
					.setId(vehicleCount--)
					.setVehiclePayload("vehicle id is " + vehicleCount)
					.build();
			kafkaTemplateVehicle.send("yajVehicle", vehicle.getId(),  vehicle);
		}

		if (tradeCount < 10) {
			Trade trade = Trade.newBuilder()
					.setId(tradeCount)
					.setVehicleId(vehicleCount + 1)
					.setTradePayload("trade id is " + tradeCount)
					.build();
			kafkaTemplateTrade.send("yajTrade", trade.getId(), trade);
		}

		if (tradeCount < 10)
			tradeCount++;
	}

}
