package io.pravega.connectors.presto.util;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.admin.StreamManager;
import io.pravega.connectors.presto.schemamanagement.CompositeSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.LocalSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.PravegaSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.SchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.SchemaSupplier;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;

import java.util.ArrayList;
import java.util.List;

import static io.pravega.connectors.presto.util.PravegaTestUtils.avroSchema;
import static io.pravega.connectors.presto.util.TestSchemas.EMPLOYEE_AVSC;

public class SchemaRegistryUtil
{
    private final StreamManager streamManager;
    private final SchemaRegistryClient schemaRegistryClient;
    private final PravegaSchemaRegistry pravegaSchemaRegistry;

    private final List<LocalSchemaRegistry> localSchemaRegistryList;

    public SchemaRegistryUtil()
    {
        this.streamManager = new MockStreamManager();
        this.schemaRegistryClient = new MockSchemaRegistryClient();

        this.pravegaSchemaRegistry = new PravegaSchemaRegistry(schemaRegistryClient, streamManager);

        this.localSchemaRegistryList = new ArrayList<>();
    }

    public CompositeSchemaRegistry getSchemaRegistry()
    {
        List<SchemaSupplier> schemaSuppliers = new ArrayList<>();
        List<SchemaRegistry> schemaRegistries = new ArrayList<>();

        localSchemaRegistryList.forEach(lsr -> {
            schemaSuppliers.add(lsr);
            schemaRegistries.add(lsr);
        });

        schemaSuppliers.add(pravegaSchemaRegistry);
        schemaRegistries.add(pravegaSchemaRegistry);

        return new CompositeSchemaRegistry(schemaSuppliers, schemaRegistries);
    }

    public void addLocalSchema(String dir)
    {
        localSchemaRegistryList.add(PravegaTestUtils.localSchemaRegistry(dir));
    }

    public boolean addPravegaSchema(String schema)
    {
        return streamManager.createScope(schema);
    }

    public boolean addPravegaTable(SchemaTableName schemaTableName)
    {
        return streamManager.createStream(schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                null);
    }

    public boolean addPravegaTable(SchemaTableName schemaTableName, String schema)
    {
        addPravegaTable(schemaTableName);
        addAvroSchema(schemaTableName, schema);
        return true;
    }

    public void addAvroSchema(SchemaTableName schemaTableName, String schema)
    {
        schemaRegistryClient.addGroup(groupId(schemaTableName), groupProperties(false));
        schemaRegistryClient.addSchema(groupId(schemaTableName), AvroSchema.of(avroSchema(schema)).getSchemaInfo());
    }

    private static GroupProperties groupProperties(boolean inline)
    {
        return new GroupProperties(
                SerializationFormat.Avro,
                Compatibility.allowAny(),
                false,
                ImmutableMap.<String, String>builder().put(inline ? "inline" : "", "").build());
    }

    private static String groupId(SchemaTableName schemaTableName)
    {
        return PravegaNameUtils.groupId(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}
