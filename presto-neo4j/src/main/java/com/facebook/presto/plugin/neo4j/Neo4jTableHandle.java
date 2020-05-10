/*
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
package com.facebook.presto.plugin.neo4j;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Neo4jTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    private final List<String> nodes;
    private final List<String> relationships;
    private final boolean isPath;

    @JsonCreator
    public Neo4jTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("nodes") List<String> nodes,
            @JsonProperty("relationships") List<String> relationships,
            @JsonProperty("isPath") boolean isPath)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.nodes = requireNonNull(nodes, "nodesAndRelations is null");
        this.relationships = requireNonNull(relationships, "relationships is null");
        this.isPath = isPath;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public List<String> getRelationships()
    {
        return relationships;
    }

    @JsonProperty("isPath")
    public boolean isPath()
    {
        return isPath;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Neo4jTableHandle o = (Neo4jTableHandle) obj;
        return Objects.equals(connectorId, o.connectorId) &&
                Objects.equals(schemaTableName, o.schemaTableName) &&
                Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(nodes, o.nodes) &&
                Objects.equals(relationships, o.relationships);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName);
    }
}
