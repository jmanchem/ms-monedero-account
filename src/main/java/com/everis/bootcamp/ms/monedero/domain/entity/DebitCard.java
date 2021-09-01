package com.everis.bootcamp.ms.monedero.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Document("DebitCard")
@Getter
@Setter
@NoArgsConstructor
public class DebitCard {
    @NotNull
    private String id;

    @NotEmpty
    private String cardNumber;

    private Customer customer;

    private List<BankAccount> registeredAccounts = new ArrayList<>();

    @NotNull
    private LocalDate creationDate;

    @NotNull
    private LocalDate expirationDate;

}
