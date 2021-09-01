package com.everis.bootcamp.ms.monedero.domain.repository;

import com.everis.bootcamp.ms.monedero.domain.entity.MonederoDebitCard;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface MonederoDebitCardRepository extends ReactiveMongoRepository<MonederoDebitCard, String> {

    Mono<MonederoDebitCard> findByNroPhone(int nroPhone);
}
