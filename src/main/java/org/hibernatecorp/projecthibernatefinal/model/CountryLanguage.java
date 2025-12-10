package org.hibernatecorp.projecthibernatefinal.model;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Entity
@Table(name = "country_language")
public class CountryLanguage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "country_id")
    private Country country;

    @Column(name = "language")
    private String language;

    @Column(name = "is_official")
    private Boolean isOfficial;

    @Column(name = "percentage")
    private BigDecimal percentage;

}
