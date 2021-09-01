package com.everis.bootcamp.ms.monedero.service.impl;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoAccountDto;
import com.everis.bootcamp.ms.monedero.domain.dto.MonederoDebitCardDto;
import com.everis.bootcamp.ms.monedero.domain.entity.DebitCard;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoAccount;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoDebitCard;
import com.everis.bootcamp.ms.monedero.domain.repository.MonederoAccountRepository;
import com.everis.bootcamp.ms.monedero.domain.repository.MonederoDebitCardRepository;
import com.everis.bootcamp.ms.monedero.service.srv.MonederoAccountService;
import com.everis.bootcamp.ms.monedero.service.srv.MonederoDebitCardService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log4j2
@Service
public class MonederoDebitCardImpl implements MonederoDebitCardService {

    @Autowired
    private ObjectMapper objectMapper;
    private final WebClient webClient;
    String uri = "http://localhost:8090/api";

    public MonederoDebitCardImpl() {
        this.webClient = WebClient.builder().baseUrl(this.uri).build();
    }

    @Override
    public Mono<DebitCard> getDebitCarById(String id) {
        return webClient.get().uri(this.uri + "/ms-debit-card/debit-cards/{id}", id)
                .accept(MediaType.APPLICATION_JSON).retrieve().bodyToMono(DebitCard.class);
    }
    @Autowired
    private MonederoDebitCardRepository monederoDebitCardRepository ;
    @Override
    public Mono<MonederoDebitCard> save(MonederoDebitCard monederoAccount) {
        return monederoDebitCardRepository.save(monederoAccount).flatMap(this::getDebitCardAccount);
    }
    @Override
    public Mono<MonederoDebitCard> getDebitCardAccount(MonederoDebitCard monederoAccount) {
        return Mono.just(objectMapper.convertValue(monederoAccount, MonederoDebitCard.class));
    }

    @Override
    public Mono<MonederoDebitCard> findByNroPhone(int nroPhone) {
        return monederoDebitCardRepository.findByNroPhone(nroPhone).flatMap(this::getMonederoDebitCard);
    }

    @Override
    public Mono<MonederoDebitCard> getMonederoDebitCard(MonederoDebitCard monederoAccount) {
        return Mono.just(objectMapper.convertValue(monederoAccount, MonederoDebitCard.class));
    }


    @Override
    public Flux<MonederoDebitCardDto> findAll() {
        return null;
    }

    @Override
    public Mono<MonederoDebitCardDto> findById(String id) {
        return null;
    }

    @Override
    public Mono<Void> delete(String id) {
        return null;
    }
}
