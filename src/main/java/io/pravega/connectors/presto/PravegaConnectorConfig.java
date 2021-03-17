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

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

public class PravegaConnectorConfig
{
    /**
     * Pravega Controller URI
     */
    private URI controllerURI;

    /**
     * Pravega Schema Registry URI
     */
    private URI schemaRegistryURI;

    /**
     * how long to cache schema+table objects for before retrieving for pravega
     */
    private int tableCacheExpireSecs = Integer.MAX_VALUE;

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Folder holding the JSON description files for Pravega stream.
     */
    private File tableDescriptionDir = new File("etc/pravega/");

    @NotNull
    public URI getControllerURI()
    {
        return this.controllerURI;
    }

    @NotNull
    public URI getSchemaRegistryURI()
    {
        return this.schemaRegistryURI;
    }

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("pravega.table-description-dir")
    public PravegaConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    public int getTableCacheExpireSecs()
    {
        return this.tableCacheExpireSecs;
    }

    @Config("pravega.controller")
    public PravegaConnectorConfig setControllerURI(URI controllerURI)
    {
        this.controllerURI = controllerURI;
        return this;
    }

    @Config("pravega.schema-registry")
    public PravegaConnectorConfig setSchemaRegistryURI(URI schemaRegistryURI)
    {
        this.schemaRegistryURI = schemaRegistryURI;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("pravega.hide-internal-columns")
    public PravegaConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }
}
