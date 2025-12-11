package org.hibernatecorp.projecthibernatefinal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernatecorp.projecthibernatefinal.DAO.CityDAO;
import org.hibernatecorp.projecthibernatefinal.DAO.CountryDAO;
import org.hibernatecorp.projecthibernatefinal.model.City;
import org.hibernatecorp.projecthibernatefinal.model.Country;
import org.hibernatecorp.projecthibernatefinal.model.CountryLanguage;
import org.hibernatecorp.projecthibernatefinal.redis.CityCountry;
import org.hibernatecorp.projecthibernatefinal.redis.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;

    private final ObjectMapper mapper;

    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;
//---------------------------------------------------------------------------------------------------------------

    public Main() {
        this.sessionFactory = prepareRelationalDb();
        this.redisClient = prepareRedisClient();

        this.mapper = new ObjectMapper();
        this.cityDAO = new CityDAO(sessionFactory);
        this.countryDAO = new CountryDAO(sessionFactory);
    }
//---------------------------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        Main main = new Main();
        List<City> cities = main.fetchData(main);
        main.prepareRedisClient();
        List<CityCountry> preparedData = main.transformData(cities);
        main.shutdown();
    }
//---------------------------------------------------------------------------------------------------------------

    private List<City> fetchData(Main main) {
        try (Session session = main.sessionFactory.getCurrentSession()) {
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();
            List<Country> countries = main.countryDAO.getAll();

            int totalCount = main.cityDAO.getTotalCount();
            int step = 500;
            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(main.cityDAO.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }
//---------------------------------------------------------------------------------------------------------------

    private SessionFactory prepareRelationalDb() {
        try {
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("hibernate.properties"));
            return new Configuration().addProperties(props).
                    addAnnotatedClasses(City.class, Country.class, CountryLanguage.class).
                    buildSessionFactory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
//---------------------------------------------------------------------------------------------------------------

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
       if (nonNull(redisClient)) {
            redisClient.shutdown();
       }

    }
//---------------------------------------------------------------------------------------------------------------


  private RedisClient prepareRedisClient() {
       RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
       try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
           System.out.println("\nConnected to Redis\n");
     }
       return redisClient;
   }

//---------------------------------------------------------------------------------------------------------------
    private List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry res = new CityCountry();
            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();
            res.setAlternativeCountryCode(country.getCode2());
            res.setContinent(country.getContinent());
            res.setCountryCode(country.getCode());
            res.setCountryName(country.getName());
            res.setCountryPopulation(country.getPopulation());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());
            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(cl -> {
                Language language = new Language();
                language.setLanguage(cl.getLanguage());
                language.setIsOfficial(cl.getIsOfficial());
                language.setPercentage(cl.getPercentage());
                return language;
            }).collect(Collectors.toSet());
            res.setLanguages(languages);

            return res;
        }).collect(Collectors.toList());
    }

//---------------------------------------------------------------------------------------------------------------

}
