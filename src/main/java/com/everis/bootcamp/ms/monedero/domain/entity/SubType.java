package com.everis.bootcamp.ms.monedero.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.Valid;


@Getter
@Setter
@NoArgsConstructor
public class SubType {
    String id;

    @Valid
    EnumSubType value;

    enum EnumSubType{
        NORMAL, VIP, PYME
    }
}
