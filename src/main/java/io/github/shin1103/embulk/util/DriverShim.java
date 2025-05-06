package io.github.shin1103.embulk.util;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/*
When load JDBC driver to DriverManager, if load JDBC Driver directly, fail to load because of Classloader.
To use driver shim, load JDBC driver collect.
 */
public class DriverShim implements Driver
{
    private final Driver driver;

    public DriverShim(Driver d)
    {
        this.driver = d;
    }

    public Connection connect(String u, Properties p) throws SQLException
    {
        return this.driver.connect(u, p);
    }

    public boolean acceptsURL(String url) throws SQLException
    {
        return this.driver.acceptsURL(url);
    }

    public int getMajorVersion()
    {
        return this.driver.getMajorVersion();
    }

    public int getMinorVersion()
    {
        return this.driver.getMinorVersion();
    }

    public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException
    {
        return this.driver.getPropertyInfo(u, p);
    }

    public boolean jdbcCompliant()
    {
        return this.driver.jdbcCompliant();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        return this.driver.getParentLogger();
    }
}
