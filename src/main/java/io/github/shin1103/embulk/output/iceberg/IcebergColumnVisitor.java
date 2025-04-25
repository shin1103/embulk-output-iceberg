package io.github.shin1103.embulk.output.iceberg;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.data.Record;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;

import java.math.BigDecimal;
import java.time.*;

public class IcebergColumnVisitor implements ColumnVisitor {
    private final Record data;
    private final PageBuilder pageBuilder;

    public IcebergColumnVisitor(Record data, PageBuilder pageBuilder){
        this.data = data;
        this.pageBuilder = pageBuilder;
    }
    public void booleanColumn(Column column) {
        if (data.getField(column.getName()) == null) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setBoolean(column, (Boolean) data.getField(column.getName()));
    }

    public void longColumn(Column column) {
        if (data.getField(column.getName()) == null) {
            pageBuilder.setNull(column);
            return;
        }
        if (data.getField(column.getName()).getClass() == Long.class) {
            pageBuilder.setLong(column, (Long) data.getField(column.getName()));
        } else {
            pageBuilder.setLong(column, ((Integer) data.getField(column.getName())).longValue());
        }
    }

    public void doubleColumn(Column column) {
        if (data.getField(column.getName()) == null) {
            pageBuilder.setNull(column);
            return;
        }
        if (data.getField(column.getName()).getClass() == BigDecimal.class) {
            pageBuilder.setDouble(column, ((BigDecimal) data.getField(column.getName())).doubleValue());
        } else if (data.getField(column.getName()).getClass() == Float.class) {
            pageBuilder.setDouble(column, ((Float) data.getField(column.getName())).doubleValue());
        } else {
            pageBuilder.setDouble(column, (Double) data.getField(column.getName()));
        }
    }

    public void stringColumn(Column column) {
        if (data.getField(column.getName()) == null) {
            pageBuilder.setNull(column);
            return;
        }
        if (data.getField(column.getName()).getClass() == LocalTime.class) {
            pageBuilder.setString(column, ((LocalTime) data.getField(column.getName())).toString());
        } else if (data.getField(column.getName()).getClass() == BigDecimal.class) {
            pageBuilder.setString(column, ((BigDecimal) data.getField(column.getName())).toPlainString());
        } else {
            pageBuilder.setString(column, (String) data.getField(column.getName()));
        }
    }

    public void timestampColumn(Column column) {
        if (data.getField(column.getName()) == null) {
            pageBuilder.setNull(column);
            return;
        }
        if (data.getField(column.getName()).getClass() == LocalDate.class) {
            pageBuilder.setTimestamp(column, ((LocalDate) data.getField(column.getName())).atStartOfDay(ZoneId.systemDefault()).toInstant());
        } else if (data.getField(column.getName()).getClass() == LocalDateTime.class) {
            pageBuilder.setTimestamp(column, ((LocalDateTime) data.getField(column.getName())).atZone(ZoneId.systemDefault()).toInstant());
        } else {
            pageBuilder.setTimestamp(column, ((OffsetDateTime) data.getField(column.getName())).toInstant());
        }
    }

    public void jsonColumn(Column column) {
        throw new NotImplementedException("JSON Type is not supported");
    }

}
