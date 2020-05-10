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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Neo4jRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Neo4jClient neo4jClient;

    @Inject
    public Neo4jRecordSetProvider(Neo4jClient neo4jClient)
    {
        this.neo4jClient = requireNonNull(neo4jClient, "neo4jClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        Neo4jSplit neo4jSplit = (Neo4jSplit) split;

        ImmutableList.Builder<Neo4jColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((Neo4jColumnHandle) handle);
        }
        return new Neo4jRecordSet(neo4jClient, session, neo4jSplit, handles.build());
    }
}
