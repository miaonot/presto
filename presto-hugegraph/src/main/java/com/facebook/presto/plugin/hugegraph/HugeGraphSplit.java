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
package com.facebook.presto.plugin.hugegraph;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HugeGraphSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<String> additionalPredicate;
    private final String gremlinQuery;

    public HugeGraphSplit(
            String connectorId,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName,
            @Nullable TupleDomain<ColumnHandle> tupleDomain,
            @Nullable Optional<String> additionalPredicate,
            @Nullable String gremlinQuery)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tupleDomain = tupleDomain;
        this.additionalPredicate = additionalPredicate;
        this.gremlinQuery = gremlinQuery;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @Nullable
    public String getTableName()
    {
        return tableName;
    }

    @Nullable
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Nullable
    public Optional<String> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @Nullable
    public String getGremlinQuery()
    {
        return gremlinQuery;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
