package com.everis.bootcamp.ms.monedero.service.srv;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoAccountDto;
import com.everis.bootcamp.ms.monedero.domain.dto.MonederoDebitCardDto;
import com.everis.bootcamp.ms.monedero.domain.entity.DebitCard;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoDebitCard;
import com.everis.bootcamp.ms.monedero.service.generic.GenericService;
import reactor.core.publisher.Mono;

public interface MonederoDebitCardService extends GenericService<MonederoDebitCardDto> {

    Mono<MonederoDebitCard> save(MonederoDebitCard monederoAccount);
    Mono<MonederoDebitCard> getDebitCardAccount(MonederoDebitCard monederoAccount);
    Mono<MonederoDebitCard> findByNroPhone(int nroPhone);
    Mono<DebitCard> getDebitCarById(String id);
    Mono<MonederoDebitCard> getMonederoDebitCard(MonederoDebitCard monederoAccount);
}
