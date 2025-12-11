package org.hibernatecorp.projecthibernatefinal.DAO;

import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.hibernatecorp.projecthibernatefinal.model.Country;

import java.util.List;

public class CountryDAO {
    private final SessionFactory sessionFactory;

    public CountryDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<Country> getAll() {
        Query<Country> query = sessionFactory.getCurrentSession().createQuery("select c from Country c join fetch c.languages", Country.class);
        return query.list();
    }
}
