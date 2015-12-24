package org.biouno.databasesparql;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.jdbc.tdb.TDBDriver;
import org.jenkinsci.plugins.database.AbstractRemoteDatabase;
import org.jenkinsci.plugins.database.AbstractRemoteDatabaseDescriptor;
import org.jenkinsci.plugins.database.Database;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import hudson.Extension;
import hudson.util.FormValidation;
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

    @Extension
    public static class DescriptorImpl extends AbstractRemoteDatabaseDescriptor {
        @Override
        public String getDisplayName() {
            return "SPARQL TDB";
        }

        public FormValidation doValidate(@QueryParameter String location, @QueryParameter Boolean mustExist)
                throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
                InstantiationException {

            try {
                Database db = clazz.getConstructor(String.class, String.class).newInstance(location, mustExist);
                DataSource ds = db.getDataSource();
                Connection con = ds.getConnection();
                con.createStatement().execute("SELECT ?a ?b ?c WHERE { ?a ?b ?c }");
                con.close();
                return FormValidation.ok("OK");
            } catch (SQLException e) {
                return FormValidation.error(e, "Failed to connect to " + getDisplayName());
            }

        }
    }

}
