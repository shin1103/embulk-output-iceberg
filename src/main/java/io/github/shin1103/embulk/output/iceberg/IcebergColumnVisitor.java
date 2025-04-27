package io.github.shin1103.embulk.output.iceberg;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.embulk.spi.*;

import java.math.BigDecimal;
import java.time.*;
import java.util.Objects;
import java.util.UUID;

public class IcebergColumnVisitor implements ColumnVisitor {

    private final PageReader reader;
    private final org.apache.iceberg.Schema icebergSchema;
    private final Record record;

    public IcebergColumnVisitor(PageReader reader, Table table, Record record){
        this.reader = reader;
        this.icebergSchema = table.schema();
        this.record = record;
    }

    public void booleanColumn(Column column) {
        if (reader.isNull(column)) {
            this.record.setField(column.getName(), null);
            return;
        }

        var value = reader.getBoolean(column);

        this.record.setField(column.getName(), value);
    }

    public void longColumn(Column column) {
        if (reader.isNull(column)) {
            this.record.setField(column.getName(), null);
            return;
        }

        var icebergTypeId = this.getIcebergTypeId(column.getName());
        var value = reader.getLong(column);

        switch (Objects.requireNonNull(icebergTypeId)) {
            case INTEGER:
                this.record.setField(column.getName(), (int) value);
                break;
            case LONG:
            default:
                this.record.setField(column.getName(), value);
                break;
        }
    }

    public void doubleColumn(Column column) {
        if (reader.isNull(column)) {
            this.record.setField(column.getName(), null);
            return;
        }

        var icebergTypeId = this.getIcebergTypeId(column.getName());
        var value = reader.getDouble(column);

        switch (Objects.requireNonNull(icebergTypeId)) {
            case DECIMAL:
                this.record.setField(column.getName(), new BigDecimal(value));
                break;
            case FLOAT:
                this.record.setField(column.getName(), (float) value);
                break;
            case DOUBLE:
            default:
                this.record.setField(column.getName(), value);
                break;
        }
    }

    public void stringColumn(Column column) {
        if (reader.isNull(column)) {
            this.record.setField(column.getName(), null);
            return;
        }

        var icebergTypeId = this.getIcebergTypeId(column.getName());

        var value = reader.getString(column);

        switch (Objects.requireNonNull(icebergTypeId)) {
            case DECIMAL:
                this.record.setField(column.getName(), new BigDecimal(value));
                break;
            case TIME:
                this.record.setField(column.getName(), OffsetTime.parse(value));
                break;
            case UUID:
                this.record.setField(column.getName(), UUID.fromString(value));
                break;
            case TIMESTAMP:
                this.record.setField(column.getName(), OffsetDateTime.parse(value));
                break;
            case DATE:
                this.record.setField(column.getName(), OffsetDateTime.parse(value).toLocalDate());
                break;
            case BOOLEAN:
                this.record.setField(column.getName(), Boolean.valueOf(value));
                break;
            case LONG:
                this.record.setField(column.getName(), Long.valueOf(value));
                break;
            case INTEGER:
                this.record.setField(column.getName(), Integer.valueOf(value));
                break;
            case FLOAT:
                this.record.setField(column.getName(), Float.valueOf(value));
                break;
            case DOUBLE:
                this.record.setField(column.getName(), Double.valueOf(value));
                break;
            case STRING:
            default:
                this.record.setField(column.getName(), value);
                break;
        }
    }

    public void timestampColumn(Column column) {
        if (reader.isNull(column)) {
            this.record.setField(column.getName(), null);
            return;
        }

        var icebergTypeId = this.getIcebergTypeId(column.getName());
        var value = reader.getTimestampInstant(column);

        switch (Objects.requireNonNull(icebergTypeId)) {
            case DATE:
                this.record.setField(column.getName(), value.atZone(ZoneId.systemDefault()).toLocalDate());
                break;
            case TIMESTAMP:
                this.record.setField(column.getName(), value.atZone(ZoneId.systemDefault()).toLocalDateTime());
                break;
            default:
                this.record.setField(column.getName(), value);
                break;
        }
    }

    public void jsonColumn(Column column) {
        throw new NotImplementedException("JSON Type is not supported");
    }

    private Type.TypeID getIcebergTypeId(String columnName) {
        for (Types.NestedField col : this.icebergSchema.columns()) {
            if (Objects.equals(col.name(), columnName)) {
                return col.type().typeId();
            }
        }
        return null;
    }
}
