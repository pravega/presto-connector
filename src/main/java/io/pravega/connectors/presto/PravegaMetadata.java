/*
 * Copyright (c) Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.presto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.pravega.connectors.presto.PravegaHandleResolver.convertColumnHandle;
import static io.pravega.connectors.presto.PravegaHandleResolver.convertTableHandle;
import static io.pravega.connectors.presto.util.PravegaNameUtils.groupId;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Pravega connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link PravegaInternalFieldDescription} for a list
 * of per-stream additional columns.
 */
public class PravegaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(PravegaMetadata.class);
    private final String connectorId;
    private final boolean hideInternalColumns;
    private final PravegaTableDescriptionSupplier tableDescSupplier;

    @Inject
    public PravegaMetadata(
            PravegaConnectorId connectorId,
            PravegaConnectorConfig pravegaConnectorConfig,
            PravegaTableDescriptionSupplier tableDescSupplier)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(pravegaConnectorConfig, "pravegaConnectorConfig is null");
        this.hideInternalColumns = pravegaConnectorConfig.isHideInternalColumns();

        requireNonNull(tableDescSupplier, "pravegaTableDescriptionSupplier is null");
        this.tableDescSupplier = tableDescSupplier;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("Listing schema names");
        return tableDescSupplier.listSchemas();
    }

    List<PravegaObjectSchema> extractSchema(PravegaStreamDescription table)
    {
        table.getEvent().orElseThrow(() ->
                new IllegalArgumentException("unable to extract schema from " + table));
        List<PravegaObjectSchema> list = new ArrayList<>(table.getEvent().get().size());
        table.getEvent().get().forEach(event -> {
            list.add(new PravegaObjectSchema(event.getDataFormat(), event.getDataSchema()));
        });
        return list;
    }

    @Override
    public PravegaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        log.debug("getTableHandle for %s", schemaTableName);
        PravegaStreamDescription table = tableDescSupplier.getTable(schemaTableName);
        if (table == null) {
            return null;
        }

        return new PravegaTableHandle(schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getObjectName(),
                table.getObjectType(),
                table.getObjectArgs(),
                extractSchema(table),
                groupId(schemaTableName.getSchemaName(), table.getObjectName()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return tableDescSupplier.listTables(schemaNameOrNull).stream()
                .filter(p -> !p.getHidden())
                .map(PravegaTableName::getSchemaTableName)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("Getting column handles");
        PravegaTableHandle pravegaTableHandle = convertTableHandle(tableHandle);

        PravegaStreamDescription pravegaStreamDescription =
                tableDescSupplier.getTable(pravegaTableHandle.toSchemaTableName());
        if (pravegaStreamDescription == null) {
            throw new TableNotFoundException(pravegaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        AtomicInteger index = new AtomicInteger(0);
        AtomicInteger schemaNum = new AtomicInteger(0);

        pravegaStreamDescription.getEvent().ifPresent(events -> {
            events.forEach(event -> {
                List<PravegaStreamFieldDescription> fields = event.getFields();
                if (fields != null) {
                    for (PravegaStreamFieldDescription pravegaStreamFieldDescription : fields) {
                        columnHandles.put(pravegaStreamFieldDescription.getName(),
                                pravegaStreamFieldDescription.getColumnHandle(connectorId,
                                        false,
                                        index.getAndIncrement(),
                                        schemaNum.get()));
                    }
                }
                schemaNum.incrementAndGet();
            });
        });

        for (PravegaInternalFieldDescription pravegaInternalFieldDescription : PravegaInternalFieldDescription.values()) {
            columnHandles.put(pravegaInternalFieldDescription.getColumnName(),
                    pravegaInternalFieldDescription.getColumnHandle(connectorId,
                            index.getAndIncrement(),
                            hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (TableNotFoundException e) {
                // Normally it would mean the table disappeared during listing operation
                throw new IllegalStateException(format("Table %s cannot be gone because tables are statically defined", tableName), e);
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns)
    {
        PravegaTableHandle handle = convertTableHandle(table);

        ConnectorTableLayout layout = new ConnectorTableLayout(new PravegaTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        log.debug("getTableMetadata %s", schemaTableName);
        PravegaStreamDescription table = tableDescSupplier.getTable(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        table.getEvent().ifPresent(events -> {
            events.forEach(event -> {
                List<PravegaStreamFieldDescription> fields = event.getFields();
                if (fields != null) {
                    for (PravegaStreamFieldDescription fieldDescription : fields) {
                        builder.add(fieldDescription.getColumnMetadata());
                    }
                }
            });
        });

        for (PravegaInternalFieldDescription fieldDescription : PravegaInternalFieldDescription.values()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
