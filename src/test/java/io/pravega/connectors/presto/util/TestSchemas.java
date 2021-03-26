package io.pravega.connectors.presto.util;

public class TestSchemas
{

    private TestSchemas() {}

    public static final String EMPLOYEE_AVSC =
            "{\"namespace\": \"io.pravega.avro\",\"type\": \"record\",\"name\": \"Employee\",\"fields\": [{\"name\": \"first\", \"type\": \"string\"},{\"name\": \"last\", \"type\": \"string\"}]}";
}
