package org.hibernatecorp.projecthibernatefinal.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
@Data
@EqualsAndHashCode

public class Language {

    private String language;
    private Boolean isOfficial;
    private BigDecimal percentage;




}