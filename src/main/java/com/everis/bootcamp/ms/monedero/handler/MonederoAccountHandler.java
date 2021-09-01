package com.everis.bootcamp.ms.monedero.handler;

import com.everis.bootcamp.ms.monedero.domain.dto.MonederoAccountDto;
import com.everis.bootcamp.ms.monedero.domain.entity.DebitCard;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoAccount;
import com.everis.bootcamp.ms.monedero.domain.entity.MonederoDebitCard;
import com.everis.bootcamp.ms.monedero.service.srv.MonederoAccountService;
import com.everis.bootcamp.ms.monedero.service.srv.MonederoDebitCardService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.springframework.web.reactive.function.BodyInserters.fromValue;

@Log4j2
@Component
public class MonederoAccountHandler {

    @Autowired
    private final MonederoAccountService service;
    @Autowired
    private final MonederoDebitCardService monederoDebitCardService;

    @Autowired
    public MonederoAccountHandler(MonederoAccountService service, MonederoDebitCardService monederoDebitCardService) {
        this.service = service;
        this.monederoDebitCardService = monederoDebitCardService;
    }

    @Autowired
    private Validator validator;

    public Mono<ServerResponse> findAll(ServerRequest request) {
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(service.findAll(), MonederoAccountDto.class);
    }
    public Mono<ServerResponse> findId(ServerRequest request) {
        String id = request.pathVariable("id");

        return service.findById(id)
                .flatMap(dto -> ServerResponse.status(HttpStatus.FOUND)
                        .contentType(MediaType.APPLICATION_JSON).bodyValue(dto))
                .switchIfEmpty(
                        ErrorResponse.buildBadResponse("El monedero account con el id: ".concat(id).concat(" no se encontró."), HttpStatus.NOT_FOUND))
                .onErrorResume(throwable ->
                        ErrorResponse.buildBadResponse(throwable.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR));
    }
    public Mono<ServerResponse> findByNroPhone(ServerRequest request) {
        int id = Integer.parseInt (request.pathVariable("id"));

        return monederoDebitCardService.findByNroPhone(id)
                .flatMap(dto -> ServerResponse.status(HttpStatus.FOUND)
                        .contentType(MediaType.APPLICATION_JSON).bodyValue(dto))
                .switchIfEmpty(
                        ErrorResponse.buildBadResponse("El monedero account con el id: ".concat(" no se encontró."), HttpStatus.NOT_FOUND))
                .onErrorResume(throwable ->
                        ErrorResponse.buildBadResponse(throwable.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR));
    }
    public Mono<ServerResponse> create(ServerRequest request) {
        log.info("ENTRO AL CREATE");
        Mono<MonederoAccount> monedero = request.bodyToMono(MonederoAccount.class);
        MonederoDebitCard mdc= new MonederoDebitCard();
        return monedero.flatMap(m -> {
            Errors errors = new BeanPropertyBindingResult(m, MonederoAccount.class.getName());
            validator.validate(m, errors);

            if (errors.hasErrors()) {
                return Flux.fromIterable(errors.getFieldErrors())
                        .map(fieldError -> "El campo " + fieldError.getField() + " " + fieldError.getDefaultMessage())
                        .collectList()
                        .flatMap(list -> ServerResponse.badRequest().body(fromValue(list)));
            } else {
                if (m.getCreateAt() == null) {
                    m.setCreateAt(LocalDateTime.now());
                }
                log.info("m.getCardNumber(): "+m.getCardNumber());
                log.info("m.getBalance(): "+m.getBalance());
                log.info("m.getEmail(): "+m.getEmail());
                String cardNumber=m.getCardNumber();
                //m.setCardNumber("No tiene Tarjeta debito asociada");
                monederoDebitCardService.getDebitCarById(cardNumber).flatMap(qdc->{
                    log.info("getCardNumber: "+ qdc.getCardNumber());
                    log.info("getDocumentNumber: "+qdc.getCustomer().getDocumentNumber());
                    mdc.setCardNumber(qdc.getCardNumber());
                    mdc.setNroPhone(m.getNroPhone());
                    m.setCardNumber(qdc.getCardNumber());
                    log.info("m.getCardNumber(): "+m.getCardNumber());
                    return monederoDebitCardService.save(mdc).flatMap(mdb -> ServerResponse
                            .status(HttpStatus.CREATED)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(fromValue(mdb)));
                }).subscribe();

                return service.save(m).flatMap(mdb -> ServerResponse
                        .status(HttpStatus.CREATED)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(fromValue(mdb)));
            }
        });

    }

}
