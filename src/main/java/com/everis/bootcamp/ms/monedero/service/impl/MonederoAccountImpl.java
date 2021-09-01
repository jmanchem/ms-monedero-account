package com.everis.bootcamp.ms.monedero.service.impl;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoAccountDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoAccount;
import com.everis.bootcamp.ms.monedero.domain.repository.MonederoAccountRepository;
import com.everis.bootcamp.ms.monedero.service.srv.MonederoAccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class MonederoAccountImpl implements MonederoAccountService {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MonederoAccountRepository repository;

    @Override
    public Flux<MonederoAccountDto> findAll() {
        return repository.findAll().flatMap(this::getMonederoDto);
    }

    @Override
    public Mono<MonederoAccountDto> findById(String id) {
        return repository.findById(id).flatMap(this::getMonederoDto);
    }

    @Override
    public Mono<MonederoAccountDto> save(MonederoAccount monederoAccount) {
        return repository.save(monederoAccount).flatMap(this::getMonederoDto);
    }

    @Override
    public Mono<MonederoAccountDto> findByNroPhone(int nroPhone) {
        return repository.findByNroPhone(nroPhone).flatMap(this::getMonederoDto);
    }

    @Override
    public Mono<Void> delete(String id) {
        return repository.deleteById(id);
    }




    @Override
    public Mono<MonederoAccountDto> getMonederoDto(MonederoAccount monederoAccount) {
        return Mono.just(objectMapper.convertValue(monederoAccount, MonederoAccountDto.class));
    }

    @Override
    public Mono<MonederoAccount> getMonedero(MonederoAccountDto monederoAccountDto) {
        return Mono.just(objectMapper.convertValue(monederoAccountDto, MonederoAccount.class));
    }

}