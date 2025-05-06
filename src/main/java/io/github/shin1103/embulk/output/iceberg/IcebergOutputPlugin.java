package io.github.shin1103.embulk.output.iceberg;

import io.github.shin1103.embulk.util.ClassLoaderSwap;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.*;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;

import org.embulk.util.config.*;

import java.util.*;

import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Official Developer Guild
// https://docs.google.com/document/d/1oKpvgstKlgmgUUja8hYqTqWxtwsgIbONoUaEj8lO0FE/edit?pli=1&tab=t.0
// https://dev.embulk.org/topics/get-ready-for-v0.11-and-v1.0-updated.html

public class IcebergOutputPlugin implements OutputPlugin
{

    public enum Mode
    {
        APPEND,
        DELETE_APPEND
    }

    private static final Logger logger = LoggerFactory.getLogger(IcebergOutputPlugin.class);

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    protected Class<? extends PluginTask> getTaskClass()
    {
        return PluginTask.class;
    }

    public interface PluginTask extends Task
    {
        @Config("catalog_name")
        @ConfigDefault("\"embulk_internal_catalog\"")
        /*
          Catalog Name. Need to set if JDBC Catalog.
         */
        Optional<String> getCatalogName();

        @Config("namespace")
        @ConfigDefault("null")
        /*
          Catalog Namespace.
         */
        String getNamespace();

        @Config("table")
        @ConfigDefault("null")
        /*
          Catalog Namespace.
         */
        String getTable();

        @Config("catalog_type")
        @ConfigDefault("null")
        /*
          Now only REST catalog is supported.
         */
        String getCatalogType();

        @Config("uri")
        @ConfigDefault("null")
        /*
          Example
          REST: http://localhost:8181
         */
        Optional<String> getUri();

        @Config("warehouse_location")
        @ConfigDefault("null")
        /*
          root warehouse location
          Example
          s3://warehouse/
         */
        String getWarehouseLocation();

        @Config("file_io_impl")
        @ConfigDefault("null")
        /*
          io class to read and write warehouse
          Example
          org.apache.iceberg.aws.s3.S3FileIO
         */
        String getFileIoImpl();

        @Config("endpoint")
        @ConfigDefault("null")
        /*
          Object Storage Endpoint
          Example
          http://localhost:9000/
         */
        Optional<String> getEndpoint();

        @Config("path_style_access")
        @ConfigDefault("true")
        /*
          Use path_style_access.
          If you use Example settings, the actual path is "http://localhost:9000/warehouse/".
         */
        Optional<String> getPathStyleAccess();

        @Config("jdbc_driver_path")
        @ConfigDefault("null")
        /*
          JDBC driver jar file path
         */
        Optional<String> getJdbcDriverPath();

        @Config("jdbc_driver_class_name")
        @ConfigDefault("null")
        /*
          JDBC driver class name
         */
        Optional<String> getJdbcDriverClassName();

        @Config("jdbc_user")
        @ConfigDefault("null")
        /*
          JDBC database user name
         */
        Optional<String> getJdbcUser();

        @Config("jdbc_pass")
        @ConfigDefault("null")
        /*
          JDBC database password
         */
        Optional<String> getJdbcPass();

        @Config("file_format")
        @ConfigDefault("\"PARQUET\"")
        /*
          datafile format
         */
        Optional<String> getFileFormat();

        @Config("file_size")
        @ConfigDefault("134217728")
        /*
          each datafile size. default value is 128MB
         */
        Optional<Long> getFileSize();

        @Config("mode")
        @ConfigDefault("\"append\"")
        /*
          insert mode. "append", "delete_append"
         */
        String getMode();

    }

    @Override
    public ConfigDiff transaction(ConfigSource configSource, Schema schema, int taskCount, Control control)
    {

        try (ClassLoaderSwap<? extends IcebergOutputPlugin> ignored = new ClassLoaderSwap<>(this.getClass())) {
            final PluginTask task = CONFIG_MAPPER.map(configSource, this.getTaskClass());

            control.run(task.toTaskSource());
            return CONFIG_MAPPER_FACTORY.newConfigDiff();
        }
    }

    private Table getTable(PluginTask task)
    {
        try (JdbcDriverMangerLoaderSwap ignored = new JdbcDriverMangerLoaderSwap(task)) {
            Catalog catalog = IcebergCatalogFactory.createCatalog(task.getCatalogType(), task);
            Namespace namespace = Namespace.of(task.getNamespace());
            TableIdentifier name = TableIdentifier.of(namespace, task.getTable());

            return catalog.loadTable(name);
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control)
    {
        throw new UnsupportedOperationException("embulk-output-iceberg does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int i, List<TaskReport> list)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, this.getTaskClass());

        try (PageReader reader = Exec.getPageReader(schema)) {
            Table table = getTable(task);
            return new IcebergTransactionalPageOutput(reader, this.createIcebergWriter(table, taskIndex, task), table, schema, task);
        }
    }

    private BaseTaskWriter<Record> createIcebergWriter(Table table, int taskIndex, PluginTask task)
    {

        long fileSize = task.getFileSize().orElse(134217728L);
        FileFormat format = FileFormat.valueOf(task.getFileFormat().map(String::toUpperCase).orElse("PARQUET"));

        var appendFactory = new GenericAppenderFactory(table.schema());
        var outputFileFactory = OutputFileFactory.builderFor(table, this.getCurrentMetadataIndex(table) + 1, taskIndex)
                .format(format)
                .build();

        if (table.spec().isPartitioned()) {
            PartitionKey partitionKey = new PartitionKey(table.spec(), table.spec().schema());

            return new PartitionedFanoutWriter<>(table.spec(), format, appendFactory, outputFileFactory,
                    table.io(), fileSize)
            {
                @Override
                protected PartitionKey partition(Record record)
                {
                    // https://github.com/apache/iceberg/issues/11899
                    var wrapper = new InternalRecordWrapper(table.schema().asStruct());
                    partitionKey.partition(wrapper.copyFor(record));
                    return partitionKey;
                }
            };

        }
        else {
            return new UnpartitionedWriter<>(
                    table.spec(), format, appendFactory, outputFileFactory, table.io(), fileSize);
        }
    }

    /*
    To put metadata version to datafile version.
     */
    private int getCurrentMetadataIndex(Table table)
    {
        var metadata = ((BaseTable) table).operations().current();
        String metadataLocation = metadata.metadataFileLocation();
        String filename = metadataLocation.substring(metadataLocation.lastIndexOf('/') + 1);

        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("^(\\d+)-.*\\.metadata\\.json$");
        java.util.regex.Matcher matcher = pattern.matcher(filename);

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        else {
            return 1;
        }
    }
}
