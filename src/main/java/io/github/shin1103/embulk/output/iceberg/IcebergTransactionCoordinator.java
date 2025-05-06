package io.github.shin1103.embulk.output.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expressions;

import java.util.concurrent.atomic.AtomicInteger;

/*
 The processing of each thread of "embulk" is consolidated into one "iceberg" transaction.
 */
public class IcebergTransactionCoordinator implements AutoCloseable
{
    private static final AtomicInteger count = new AtomicInteger(0);
    private static volatile Table table;
    private static volatile IcebergTransactionCoordinator instance;
    private static Transaction transaction;

    public static IcebergTransactionCoordinator createIcebergTransactionCoodinator(Table argTable, IcebergOutputPlugin.PluginTask task)
    {
        if (instance == null) {
            synchronized (IcebergTransactionCoordinator.class) {
                if (instance == null) {
                    instance = new IcebergTransactionCoordinator();
                }
            }
        }
        if (table == null) {
            synchronized (Table.class) {
                if (table == null) {
                    table = argTable;
                    transaction = table.newTransaction();
                }
            }

            if (IcebergOutputPlugin.Mode.valueOf(task.getMode().toUpperCase()) == IcebergOutputPlugin.Mode.DELETE_APPEND) {
                transaction.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
            }
        }

        count.incrementAndGet();
        return instance;
    }

    private IcebergTransactionCoordinator() {}

    @Override
    public void close()
    {
        count.decrementAndGet();

        if (count.get() == 0) {
            transaction.commitTransaction();
        }
    }

    public Transaction getTransaction()
    {
        return transaction;
    }
}
