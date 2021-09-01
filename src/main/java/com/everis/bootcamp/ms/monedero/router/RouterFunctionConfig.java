package com.everis.bootcamp.ms.monedero.router;

import com.everis.bootcamp.ms.monedero.handler.MonederoAccountHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import static org.springframework.web.reactive.function.server.RequestPredicates.*;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class RouterFunctionConfig {

    @Bean
    public RouterFunction<ServerResponse> routes (MonederoAccountHandler handler) {
        return route(GET("/monederoAccount"), handler::findAll)
                .andRoute(GET("/monederoAccount/{id}"), handler::findId)
                .andRoute(POST("/monederoAccount"),handler::create);
    }
}
