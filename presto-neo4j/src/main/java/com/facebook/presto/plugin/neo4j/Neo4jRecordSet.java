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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Neo4jRecordSet
        implements RecordSet
{
    private final Neo4jClient neo4jClient;
    private final List<Neo4jColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Neo4jSplit split;
    private final ConnectorSession session;

    public Neo4jRecordSet(Neo4jClient neo4jClient, ConnectorSession session, Neo4jSplit neo4jSplit, ImmutableList<Neo4jColumnHandle> build)
    {
        this.neo4jClient = requireNonNull(neo4jClient, "neo4jClient is null");
        this.split = requireNonNull(neo4jSplit, "split is null");
        this.columnHandles = requireNonNull(build, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (Neo4jColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new Neo4jRecordCursor(neo4jClient, split, columnHandles);
    }
}
