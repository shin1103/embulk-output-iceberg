package io.github.shin1103.embulk.output.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.embulk.config.TaskReport;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.modules.ZoneIdModule;

import java.io.IOException;

class IcebergTransactionalPageOutput implements TransactionalPageOutput {
    private final PageReader reader;
    private final BaseTaskWriter<Record> writer;
    private final Table table;
    private final Schema embulkSchema;
    private final IcebergTransactionCoordinator coordinator;

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    public IcebergTransactionalPageOutput(PageReader reader, BaseTaskWriter<Record> writer, Table table, Schema embulkSchema, IcebergOutputPlugin.PluginTask task) {
        this.reader = reader;
        this.writer = writer;
        this.table = table;
        this.embulkSchema = embulkSchema;
        this.coordinator = IcebergTransactionCoordinator.createIcebergTransactionCoodinator(this.table, task);
    }
    @Override
    public void add(Page page) {
        reader.setPage(page);
        while (reader.nextRecord()) {
            Record record = GenericRecord.create(table.schema());
            IcebergColumnVisitor visitor = new IcebergColumnVisitor(this.reader, this.table, record);
            embulkSchema.visitColumns(visitor);
            try {
                this.writer.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void finish() {
        if (this.writer == null) {
            return;
        }

        try {
            WriteResult result = writer.complete();

            AppendFiles appender = this.coordinator.getTransaction().newAppend();
            for (DataFile datafile : result.dataFiles()){
                appender.appendFile(datafile);
            }
            appender.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (reader != null) {
            reader.close();
        }

        if (this.coordinator != null) {
            try {
                this.coordinator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void abort() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public TaskReport commit() {
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }
}
