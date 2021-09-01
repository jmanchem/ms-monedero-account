package com.everis.bootcamp.ms.monedero.service.generic;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GenericService<T> {

    Flux<T> findAll();

    Mono<T> findById(String id);

    Mono<Void> delete(String id);
}
