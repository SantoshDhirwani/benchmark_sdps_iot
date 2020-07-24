package de.adrianbartnik.data.nexmark;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Schema: timestamp,person_id,name,email_address,phone,street,city,country,province,zipcode,homepage,creditcard
 *
 * Class needs public field with default, no-argument constructor to be serializable.
 */
public class NewPersonEvent implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NewPersonEvent.class);

    public long timestamp;
    public long personId;
    public String name;
    public String email;
    public String city;
    public String country;
    public String province;
    public String zipcode;
    public String homepage;
    public String creditcard;
    public long ingestionTimestamp;

    public NewPersonEvent() {
        LOG.debug("Created person event with default constructor");
    }

    public NewPersonEvent(long timestamp,
                          long personId,
                          String name,
                          String email,
                          String city,
                          String country,
                          String province,
                          String zipcode,
                          String homepage,
                          String creditcard) {
        this(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, System.currentTimeMillis());
    }

    public NewPersonEvent(long timestamp,
                          long personId,
                          String name,
                          String email,
                          String city,
                          String country,
                          String province,
                          String zipcode,
                          String homepage,
                          String creditcard,
                          long ingestionTimestamp) {
        LOG.debug("Created person event with id {} and name {}", personId, name);
        this.timestamp = timestamp;
        this.personId = personId;
        this.email = email;
        this.creditcard = creditcard;
        this.city = city;
        this.name = name;
        this.country = country;
        this.province = province;
        this.zipcode = zipcode;
        this.homepage = homepage;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public String getName() {
        return name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public long getPersonId() {
        return personId;
    }

    public String getEmail() {
        return email;
    }

    public String getCreditcard() {
        return creditcard;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getProvince() {
        return province;
    }

    public String getZipcode() {
        return zipcode;
    }

    public String getHomepage() {
        return homepage;
    }

    public long getIngestionTimestamp() {
        return ingestionTimestamp;
    }
}
