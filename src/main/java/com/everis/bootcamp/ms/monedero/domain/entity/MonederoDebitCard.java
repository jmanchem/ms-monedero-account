package com.everis.bootcamp.ms.monedero.domain.entity;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Data
@Document("MonederoDebitCard")
public class MonederoDebitCard {
    @Id
    private String id;
    @NotNull
    private int nroPhone;
    @NotEmpty
    private String cardNumber;

}
