package com.everis.bootcamp.ms.monedero.producer;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoTransactionDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MonederoProducer {

	@Value("${custom.kafka.topic-name-monedero-account}")
	private String topicName;

	  @Autowired
	  private KafkaTemplate<String, MonederoTransactionDto> monederoKafkaTemplate;

	  public void producer(MonederoTransactionDto monederoAutorizacionMessageDTO) {
		  monederoKafkaTemplate.send(topicName, monederoAutorizacionMessageDTO);
	  }
}
