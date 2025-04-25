package io.github.shin1103.embulk.output.iceberg;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.util.List;
import java.util.Optional;

public interface IcebergFilterOption extends Task {
    @Config("type")
    @ConfigDefault("null")
    public String getFilterType();

    @Config("column")
    @ConfigDefault("null")
    public String getColumn();

    @Config("value")
    @ConfigDefault("null")
    public Optional<Object> getValue();

    @Config("in_values")
    @ConfigDefault("null")
    public Optional<List<Object>> getInValues();

}