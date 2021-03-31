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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class Neo4jSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<String> additionalPredicate;

    private final List<String> nodeTypes;
    private final List<String> relationshipTypes;
    private final List<String> nodeNames;
    private final List<String> relationshipNames;
    private final List<String> arguments;
    private final boolean isPath;
    private final Optional<Long> limitCount;
    private final Optional<List<String>> project;

    @JsonCreator
    public Neo4jSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("nodeTypes") List<String> nodeTypes,
            @JsonProperty("relationshipTypes") List<String> relationshipTypes,
            @JsonProperty("nodeNames") List<String> nodeNames,
            @JsonProperty("relationshipNames") List<String> relationshipNames,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("isPath") boolean isPath,
            @JsonProperty("limitCount") Optional<Long> limitCount,
            @JsonProperty("project") Optional<List<String>> project)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalogName = catalogName;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
        this.nodeTypes = requireNonNull(nodeTypes, "nodeTypes is null");
        this.relationshipTypes = requireNonNull(relationshipTypes, "relationshipTypes is null");
        this.nodeNames = requireNonNull(nodeNames, "nodeNames is null");
        this.relationshipNames = requireNonNull(relationshipTypes, "relationshipName is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.isPath = isPath;
        this.limitCount = requireNonNull(limitCount, "limitCount is null");
        this.project = requireNonNull(project, "project is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
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
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public Optional<String> getAdditionalPredicate()
    {
        return additionalPredicate;
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
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
