package org.biouno.databasesparql;

import java.sql.Driver;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.jdbc.tdb.TDBDriver;
import org.jenkinsci.plugins.database.AbstractRemoteDatabase;
import org.kohsuke.stapler.DataBoundConstructor;

import hudson.util.Secret;

public class JenaTDBDatabase extends AbstractRemoteDatabase {

    /**
     * Define whether the TDB database must exist beforehand or not. Defaults to
     * {@code false}.
     */
    private final Boolean mustExist;

    private final String location;

    @DataBoundConstructor
    public JenaTDBDatabase(String location, Boolean mustExist) {
        super("", "", "", Secret.fromString(""), "");
        this.location = StringUtils.defaultIfBlank(location, "");
        this.mustExist = BooleanUtils.toBooleanDefaultIfNull(mustExist, Boolean.FALSE);
    }

    @Override
    protected Class<? extends Driver> getDriverClass() {
        return TDBDriver.class;
    }

    @Override
    protected String getJdbcUrl() {
        return String.format("jdbc:jena:tdb:location=%s&must-exist=%s", location, mustExist.toString());
    }

}
