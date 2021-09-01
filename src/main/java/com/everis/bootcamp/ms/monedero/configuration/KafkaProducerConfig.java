package com.everis.bootcamp.ms.monedero.configuration;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoTransactionDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaProducerConfig {

    @Value("${custom.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, MonederoTransactionDto> monederoProducerFactory() {

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, MonederoTransactionDto> monederoKafkaTemplate() {

        KafkaTemplate<String, MonederoTransactionDto> kafkaTemplate = new KafkaTemplate<>(monederoProducerFactory());

        kafkaTemplate.setProducerListener(new ProducerListener<String, MonederoTransactionDto>() {

            @Override
            public void onSuccess(ProducerRecord<String, MonederoTransactionDto> producerRecord, RecordMetadata recordMetadata) {

                log.info("Success to publish message: {}  offset:  {}", producerRecord.value(),
                        recordMetadata.offset());
            }

            @Override
            public void onError(ProducerRecord<String, MonederoTransactionDto> producerRecord, RecordMetadata recordMetadata,
                                Exception exception) {
                log.warn("Error to publish message [{}] exception: {}", producerRecord.value(), exception.getMessage());
            }
        });
        return kafkaTemplate;
    }
}
