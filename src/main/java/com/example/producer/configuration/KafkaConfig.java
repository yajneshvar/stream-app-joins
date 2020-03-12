package com.example.producer.configuration;


import avro.AvroDataFileDeserializer;
import avro.AvroDataFileSerializer;
import com.example.demo.avro.Trade;
import com.example.demo.avro.Vehicle;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {


    @Bean
    public ProducerFactory<Long, Trade> producerTradeFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public ProducerFactory<Long, Vehicle> producerVehicleFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        //props.put(ProducerConfig.)
//        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "testTransaction");
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

    @Bean
    public KafkaTemplate<Long, Trade> kafkaTemplateTrade() {
        //SpecificAvroSerializer<Trade> tradeSerde = new SpecificAvroSerializer<Trade>();
        return new KafkaTemplate<Long, Trade>(producerTradeFactory());
    }

    @Bean
    public KafkaTemplate<Long, Vehicle> kafkaTemplateVehicle() {
        //SpecificAvroSerializer<Trade> tradeSerde = new SpecificAvroSerializer<Trade>();
        return new KafkaTemplate<Long, Vehicle>(producerVehicleFactory());
    }

}
