package io.github.shin1103.embulk.output.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;

public class IcebergScanBuilder {

    public static IcebergGenerics.ScanBuilder createBuilder(Table table, IcebergOutputPlugin.PluginTask task) {
        IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table);

        // determine select columns
        task.getColumns().ifPresent(builder::select);

        // determine select rows
        task.getTableFilters().ifPresent(filters -> {
            for (IcebergFilterOption filter : filters) {
                // support filter is predicate expressions only
                // https://iceberg.apache.org/docs/1.8.1/api/#expressions
                switch (filter.getFilterType().toUpperCase()) {
                    case "ISNULL":
                        builder.where(Expressions.isNull(filter.getColumn()));
                        continue;
                    case "NOTNULL":
                        builder.where(Expressions.notNull(filter.getColumn()));
                        continue;
                    case "EQUAL":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.equal(filter.getColumn(), f)));
                        continue;
                    case "NOTEQUAL":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.notEqual(filter.getColumn(), f)));
                        continue;
                    case "LESSTHAN":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.lessThan(filter.getColumn(), f)));
                        continue;
                    case "LESSTHANOREQUAL":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.lessThanOrEqual(filter.getColumn(), f)));
                        continue;
                    case "GREATERTHAN":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.greaterThan(filter.getColumn(), f)));
                        continue;
                    case "GREATERTHANOREQUAL":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.greaterThanOrEqual(filter.getColumn(), f)));
                        continue;
                    case "IN":
                        filter.getInValues().ifPresent(f -> builder.where(Expressions.in(filter.getColumn(), f)));
                        continue;
                    case "NOTIN":
                        filter.getInValues().ifPresent(f -> builder.where(Expressions.notIn(filter.getColumn(), f)));
                        continue;
                    case "STARTSWITH":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.startsWith(filter.getColumn(), (String) f)));
                        continue;
                    case "NOTSTARTSWITH":
                        filter.getValue().ifPresent(f -> builder.where(Expressions.notStartsWith(filter.getColumn(), (String) f)));
                        continue;
                    default:
                }
            }
        });
        return builder;
    }
}
