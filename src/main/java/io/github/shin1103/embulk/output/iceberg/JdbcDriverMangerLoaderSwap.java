package io.github.shin1103.embulk.output.iceberg;

import io.github.shin1103.embulk.util.DriverShim;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/*
Load JDBC driver to DriverManager
 */
public class JdbcDriverMangerLoaderSwap implements AutoCloseable{

    private final URLClassLoader classLoader;
    public JdbcDriverMangerLoaderSwap(IcebergOutputPlugin.PluginTask task)
    {
        if (IcebergCatalogFactory.CatalogType.valueOf(task.getCatalogType().toUpperCase()) != IcebergCatalogFactory.CatalogType.JDBC){
            this.classLoader = null;
            return;
        }

        try {
            URL url = null;
            if (task.getJdbcDriverPath().isPresent()) {
                File file = new File(task.getJdbcDriverPath().get());
                url = file.toURI().toURL();
            }
            this.classLoader = new URLClassLoader(new URL[]{url}, IcebergOutputPlugin.class.getClassLoader());


            Class<?> driverClass = null;

            if (task.getJdbcDriverClassName().isPresent()){
                driverClass = Class.forName(task.getJdbcDriverClassName().get(), true, classLoader);
            }

            Driver driver = (Driver) Objects.requireNonNull(driverClass).getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(new DriverShim(driver));
        } catch (MalformedURLException | SQLException | ClassNotFoundException | InvocationTargetException |
                 InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if(this.classLoader != null) {
                this.classLoader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
