package com.everis.bootcamp.ms.monedero.service.srv;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoAccountDto;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoAccount;
import com.everis.bootcamp.ms.monedero.service.generic.GenericService;
import reactor.core.publisher.Mono;

public interface MonederoAccountService extends GenericService<MonederoAccountDto> {

    Mono<MonederoAccountDto> save(MonederoAccount monederoAccount);
    Mono<MonederoAccountDto> findByNroPhone(int nroPhone);
    Mono<MonederoAccount> getMonedero(MonederoAccountDto monederoAccountDto);
    Mono<MonederoAccountDto> getMonederoDto(MonederoAccount monederoAccount);

}
