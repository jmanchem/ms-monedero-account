package com.everis.bootcamp.ms.monedero.handler;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {
    private String  message;
    private Date    date;
    private Integer status;

    static public Mono<ServerResponse> buildBadResponse(String message, HttpStatus status) {
        return ServerResponse.status(status).bodyValue(new ErrorResponse(message, new Date(),status.value()));
    }
}
