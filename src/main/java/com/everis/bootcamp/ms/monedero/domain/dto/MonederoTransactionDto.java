package com.everis.bootcamp.ms.monedero.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper=false)
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class MonederoTransactionDto {

    private static final long serialVersionUID = 1L;

    private String id;

    private int originNumber;

    private int destinationNumber;

    private Double amount;

    private Status status;

    private LocalDateTime createAt;

    public enum Status {
        PENDING,
        REJECTED,
        SUCCESSFUL
    }

}
