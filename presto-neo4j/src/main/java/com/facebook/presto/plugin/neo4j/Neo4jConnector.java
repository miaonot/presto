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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.neo4j.optimization.Neo4jPlanOptimizerProvider;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class Neo4jConnector
        implements Connector
{
    private static final Logger log = Logger.get(Neo4jConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final Neo4jMetadata neo4jMetadata;
    private final Neo4jSplitManager neo4jSplitManager;
    private final Neo4jRecordSetProvider neo4jRecordSetProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, Neo4jMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public Neo4jConnector(
            LifeCycleManager lifeCycleManager,
            Neo4jMetadata neo4jMetadata,
            Neo4jSplitManager neo4jSplitManager,
            Neo4jRecordSetProvider neo4jRecordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.neo4jMetadata = requireNonNull(neo4jMetadata, "neo4jMetadata is null");
        this.neo4jSplitManager = requireNonNull(neo4jSplitManager, "neo4jSplitManager is null");
        this.neo4jRecordSetProvider = requireNonNull(neo4jRecordSetProvider, "neo4jRecordSetProvider is null");
    }

    //TODO: Support transaction. Use factory to instantiate Neo4jMetadata.
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return new Neo4jTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return neo4jMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return neo4jSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return neo4jRecordSetProvider;
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return new Neo4jPlanOptimizerProvider();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
