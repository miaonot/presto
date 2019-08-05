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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class HugeGraphConnector
        implements Connector
{
    private static final Logger log = Logger.get(HugeGraphConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final HugeGraphMetadata hugeGraphMetadata;
    private final HugeGraphSplitManager hugeGraphSplitManager;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Set<Procedure> procedures;

    @Inject
    public HugeGraphConnector(
            LifeCycleManager lifeCycleManager,
            HugeGraphMetadata hugeGraphMetadata,
            HugeGraphSplitManager hugeGraphSplitManager,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.hugeGraphMetadata = requireNonNull(hugeGraphMetadata, "hugeGraphMetadata is null");
        this.hugeGraphSplitManager = requireNonNull(hugeGraphSplitManager, "hugeGraphSplitManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        HugeGraphTransactionHandle transaction = new HugeGraphTransactionHandle();
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return hugeGraphMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return hugeGraphSplitManager;
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
