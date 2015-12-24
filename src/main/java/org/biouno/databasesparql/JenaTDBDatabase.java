/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Bruno P. Kinoshita
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.biouno.databasesparql;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.jdbc.tdb.TDBDriver;
import org.apache.jena.system.JenaSystem;
import org.jenkinsci.plugins.database.AbstractRemoteDatabase;
import org.jenkinsci.plugins.database.AbstractRemoteDatabaseDescriptor;
import org.jenkinsci.plugins.database.Database;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import hudson.Extension;
import hudson.util.FormValidation;
import hudson.util.Secret;

/**
 * Adds SPARQL through Jena TDB to the Database plugin.
 *
 * @author Bruno P. Kinoshita
 * @since 1.0
 */
public class JenaTDBDatabase extends AbstractRemoteDatabase {

    private final static Logger LOGGER = Logger.getLogger(JenaTDBDatabase.class.getName());

    /**
     * Define whether the TDB database must exist beforehand or not. Defaults to
     * {@code false}.
     */
    private final Boolean mustExist;
    /**
     * The TDB database location. Will be created if mustExist is {@code false}.
     */
    private final String location;

    /*
     * Register JDBC driver and init ARQ.
     */
    static {
        ClassLoader contextClassLoader = null;
        try {
            contextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(JenaTDBDatabase.class.getClassLoader());
            JenaSystem.DEBUG_INIT = Boolean.FALSE;
            TDBDriver.register();
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, String.format("Failed to register Jena TDB driver: %s", e.getMessage()), e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * Create a Jena TDB Database. This constructor is exposed to the UI via the {@link DataBoundConstructor}
     * annotation. It overrides the default constructor from the {@link AbstractRemoteDatabase}, as Jena TDB uses
     * only location and other flags, different than other JDBC drivers.
     *
     * @param location TDB database location
     * @param mustExist must-exist flag
     */
    @DataBoundConstructor
    public JenaTDBDatabase(String location, Boolean mustExist) {
        super("", "", "", Secret.fromString(""), "");
        this.location = StringUtils.defaultIfBlank(location, "");
        this.mustExist = BooleanUtils.toBooleanDefaultIfNull(mustExist, Boolean.FALSE);
    }

    /**
     * Get must exist flag.
     *
     * @return {@code true} if the TDB location must exist before being used,
     *         {@code false} otherwise.
     */
    public Boolean getMustExist() {
        return mustExist;
    }

    /**
     * Get the TDB location, used in the JDBC connection URL.
     *
     * @return TDB location string
     */
    public String getLocation() {
        return location;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.jenkinsci.plugins.database.AbstractRemoteDatabase#getDriverClass()
     */
    @Override
    protected Class<? extends Driver> getDriverClass() {
        return TDBDriver.class;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.jenkinsci.plugins.database.AbstractRemoteDatabase#getJdbcUrl()
     */
    @Override
    protected String getJdbcUrl() {
        final String jdbcUrl = String.format("jdbc:jena:tdb:location=%s&must-exist=%s", location, mustExist.toString());
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(String.format("JDBC URL: %s", jdbcUrl));
        }
        return jdbcUrl;
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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.entering(JenaTDBDatabase.class.getName(), "doValidate");
            }
            try {
                Database db = JenaTDBDatabase.class.getConstructor(String.class, Boolean.class).newInstance(location,
                        mustExist);
                DataSource ds = db.getDataSource();
                Connection con = ds.getConnection();
                con.createStatement().execute("SELECT * WHERE { ?a ?b ?c }");
                con.close();
                return FormValidation.ok("OK");
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to validate Jena TDB connection: " + e.getMessage(), e);
                return FormValidation.error(e, "Failed to connect to " + getDisplayName());
            }

        }
    }

}
