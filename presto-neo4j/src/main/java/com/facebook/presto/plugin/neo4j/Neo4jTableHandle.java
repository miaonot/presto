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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Neo4jTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    private final List<String> nodeTypes;
    private final List<String> relationshipTypes;
    private final List<String> nodeNames;
    private final List<String> relationshipNames;
    private final List<String> arguments;
    private final boolean isPath;

    private final Optional<Long> limitCount;
    private final Optional<List<String>> project;

    @JsonCreator
    public Neo4jTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("nodeTypes") List<String> nodeTypes,
            @JsonProperty("relationshipTypes") List<String> relationshipTypes,
            @JsonProperty("nodeNames") List<String> nodeNames,
            @JsonProperty("relationshipNames") List<String> relationshipNames,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("isPath") boolean isPath,
            @JsonProperty("limitCount")Optional<Long> limitCount,
            @JsonProperty("project")Optional<List<String>> project)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.nodeTypes = requireNonNull(nodeTypes, "nodeTypes is null");
        this.relationshipTypes = requireNonNull(relationshipTypes, "relationshipTypes is null");
        this.nodeNames = requireNonNull(nodeNames, "nodeNames is null");
        this.relationshipNames = requireNonNull(relationshipNames, "relationshipNames is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.isPath = isPath;
        this.limitCount = requireNonNull(limitCount, "limitCount is null");
        this.project = requireNonNull(project, "project is null");
    }

    public Neo4jTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("nodeTypes") List<String> nodeTypes,
            @JsonProperty("relationshipTypes") List<String> relationshipTypes,
            @JsonProperty("nodeNames") List<String> nodeNames,
            @JsonProperty("relationshipNames") List<String> relationshipNames,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("isPath") boolean isPath)
    {
        this(connectorId, schemaTableName, catalogName, schemaName, tableName, nodeTypes, relationshipTypes, nodeNames, relationshipNames, arguments, isPath, Optional.empty(), Optional.empty());
    }

    public static Neo4jTableHandle setLimitCount(Neo4jTableHandle old, long count)
    {
        return new Neo4jTableHandle(old.connectorId, old.schemaTableName, old.catalogName, old.schemaName, old.tableName, old.nodeTypes, old.relationshipTypes, old.nodeNames, old.relationshipNames, old.arguments, old.isPath, Optional.of(count), old.project);
    }

    public static Neo4jTableHandle setProject(Neo4jTableHandle old, ImmutableList<String> project)
    {
        return new Neo4jTableHandle(old.connectorId, old.schemaTableName, old.catalogName, old.schemaName, old.tableName, old.nodeTypes, old.relationshipTypes, old.nodeNames, old.relationshipNames, old.arguments, old.isPath, old.limitCount, Optional.of(project));
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
    public List<String> getNodeTypes()
    {
        return nodeTypes;
    }

    @JsonProperty
    public List<String> getRelationshipTypes()
    {
        return relationshipTypes;
    }

    @JsonProperty
    public List<String> getNodeNames()
    {
        return nodeNames;
    }

    @JsonProperty
    public List<String> getRelationshipNames()
    {
        return relationshipNames;
    }

    @JsonProperty
    public List<String> getArguments()
    {
        return arguments;
    }

    @JsonProperty("isPath")
    public boolean isPath()
    {
        return isPath;
    }

    @JsonProperty
    public Optional<Long> getLimitCount()
    {
        return limitCount;
    }

    @JsonProperty
    public Optional<List<String>> getProject()
    {
        return project;
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
                Objects.equals(nodeTypes, o.nodeTypes) &&
                Objects.equals(relationshipTypes, o.relationshipTypes) &&
                Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName, arguments);
    }
}
