package com.everis.bootcamp.ms.monedero.domain.dto;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
public class MonederoDebitCardDto {
    private String id;
    private int nroPhone;
    private String cardNumber;
}
