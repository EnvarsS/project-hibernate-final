package org.hibernatecorp.projecthibernatefinal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernatecorp.projecthibernatefinal.DAO.CityDAO;
import org.hibernatecorp.projecthibernatefinal.DAO.CountryDAO;
import org.hibernatecorp.projecthibernatefinal.model.City;
import org.hibernatecorp.projecthibernatefinal.model.Country;
import org.hibernatecorp.projecthibernatefinal.model.CountryLanguage;

import java.util.Properties;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;

    private final ObjectMapper mapper;

    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {
        this.sessionFactory = prepareRelationalDb();
        this.redisClient = prepareRedisClient();

        this.mapper = new ObjectMapper();
        this.cityDAO = new CityDAO(sessionFactory);
        this.countryDAO = new CountryDAO(sessionFactory);
    }

    private SessionFactory prepareRelationalDb() {
        try {
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("app.prop"));
            return new Configuration().addProperties(props).
                    addAnnotatedClasses(City.class, Country.class, CountryLanguage.class).
                    buildSessionFactory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }

    }

    private RedisClient prepareRedisClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            System.out.println("\nConnected to Redis\n");
        }
        return redisClient;
    }

}
