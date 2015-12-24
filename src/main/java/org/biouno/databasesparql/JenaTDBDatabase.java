package org.biouno.databasesparql;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.jdbc.tdb.TDBDriver;
import org.jenkinsci.plugins.database.AbstractRemoteDatabase;
import org.jenkinsci.plugins.database.AbstractRemoteDatabaseDescriptor;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import hudson.Extension;
import hudson.Util;
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

        public FormValidation doCheckProperties(@QueryParameter String properties) throws IOException {
            try {
                Set<String> validPropertyNames = new HashSet<String>();
                Properties props = Util.loadProperties(properties);
                for (DriverPropertyInfo p : new Driver().getPropertyInfo("jdbc:postgresql://localhost/dummy", props)) {
                    validPropertyNames.add(p.name);
                }

                for (Map.Entry e : props.entrySet()) {
                    String key = e.getKey().toString();
                    if (!validPropertyNames.contains(key))
                        return FormValidation.error("Unrecognized property: " + key);
                }
                return FormValidation.ok();
            } catch (Throwable e) {
                return FormValidation.warning(e, "Failed to validate the connection properties");
            }
        }
    }

}
