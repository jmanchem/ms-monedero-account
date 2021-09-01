package com.everis.bootcamp.ms.monedero.domain.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class MonederoAccountDto {

    private String id;

    private String typeDocument;

    private String nroDocument;

    private String phoneOperator;

    private int nroPhone;

    private String imeiPhone;

    private String email;

    private Double balance;

    private String cardNumber;

    private LocalDateTime createAt;

}
