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
package io.pravega.connectors.presto.util;

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * this class has a very limited use in unit testing, so many methods not implemented
 */
public class MockSchemaRegistryClient
        implements SchemaRegistryClient
{
    private final Map<String, GroupProperties> groups = new HashMap<>();

    private final Map<String, List<SchemaWithVersion>> schemas = new HashMap<>();

    @Override
    public boolean addGroup(String s, GroupProperties groupProperties) throws RegistryExceptions.BadArgumentException, RegistryExceptions.UnauthorizedException
    {
        return groups.putIfAbsent(s, groupProperties) == null;
    }

    @Override
    public void removeGroup(String s) throws RegistryExceptions.UnauthorizedException
    {
        groups.remove(s);
    }

    @Override
    public Iterator<Map.Entry<String, GroupProperties>> listGroups() throws RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public GroupProperties getGroupProperties(String s) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        GroupProperties groupProperties = groups.get(s);
        if (groupProperties == null) {
            throw new RegistryExceptions.ResourceNotFoundException(s);
        }
        return groupProperties;
    }

    @Override
    public boolean updateCompatibility(String s, Compatibility compatibility, @Nullable Compatibility compatibility1) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaWithVersion> getSchemas(String s) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        return schemas.get(s);
    }

    @Override
    public VersionInfo addSchema(String s, SchemaInfo schemaInfo) throws RegistryExceptions.SchemaValidationFailedException, RegistryExceptions.SerializationMismatchException, RegistryExceptions.MalformedSchemaException, RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        List<SchemaWithVersion> list = schemas.computeIfAbsent(s, k -> new ArrayList<>());
        VersionInfo versionInfo = new VersionInfo("type", "avro", list.size() + 1, list.size() + 1);
        list.add(new SchemaWithVersion(schemaInfo, versionInfo));
        return versionInfo;
    }

    @Override
    public void deleteSchemaVersion(String s, VersionInfo versionInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String s, VersionInfo versionInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public EncodingInfo getEncodingInfo(String s, EncodingId encodingId) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public EncodingId getEncodingId(String s, VersionInfo versionInfo, String s1) throws RegistryExceptions.CodecTypeNotRegisteredException, RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String s, @Nullable String s1) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionInfo getVersionForSchema(String s, SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String s, @Nullable String s1) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean validateSchema(String s, SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canReadUsing(String s, SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CodecType> getCodecTypes(String s) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCodecType(String s, CodecType codecType) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String s) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNamespace()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception
    {

    }
}
