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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Neo4jSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;

    @Inject
    public Neo4jSplitManager(Neo4jConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        Neo4jTableLayoutHandle tableLayoutHandle = (Neo4jTableLayoutHandle) layout;

        Neo4jTableHandle tableHandle = tableLayoutHandle.getTable();

        Neo4jSplit neo4jSplit = new Neo4jSplit(connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableLayoutHandle.getTupleDomain(),
                Optional.empty(),
                tableHandle.getNodeTypes(),
                tableHandle.getRelationshipTypes(),
                tableHandle.getNodeNames(),
                tableHandle.getRelationshipNames(),
                tableHandle.getArguments(),
                tableHandle.isPath(),
                tableHandle.getLimitCount(),
                tableHandle.getProject());
        return new FixedSplitSource(ImmutableList.of(neo4jSplit));
    }
}
