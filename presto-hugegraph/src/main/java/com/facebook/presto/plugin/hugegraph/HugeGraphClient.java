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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.plugin.hugegraph.GremlinErrorCode.GREMLIN_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class HugeGraphClient
{
    private static final Logger log = Logger.get(HugeGraphClient.class);

    protected final String connectorId;
    protected final GremlinClientFactory gremlinClientFactory;

    public HugeGraphClient(String connectorId, HugeGraphConfiguration config,
                           GremlinClientFactory gremlinClientFactory)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");
        this.gremlinClientFactory = requireNonNull(gremlinClientFactory, "connectionFactory is null");
    }

    //Gremlin无法获取图数据库
    public Set<String> getSchemaNames()
    {
        try {
            Client client = gremlinClientFactory.openClient();
            CompletableFuture<ResultSet> future = client.submitAsync("system.graphs()");
            ResultSet resultSet = future.get();
            Stream<String> stringStream = resultSet.stream().map(Result::getString);
            return stringStream.collect(Collectors.toSet());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<SchemaTableName> getTableNames(@Nullable String schema)
    {
        try {
            Client client = gremlinClientFactory.openClient();
            CompletableFuture<ResultSet> future = client.submitAsync("g=" + schema
                    + ".traversal(); g.V().label()");
            ResultSet resultSet = future.get();

            List<Result> results = resultSet.all().get();
            ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
            for (Result result : results) {
                list.add(new SchemaTableName(schema, result.getString()));
            }

            future = client.submitAsync("g=" + schema + ".traversal(); g.E().label()");
            resultSet = future.get();

            results = resultSet.all().get();
            for (Result result : results) {
                list.add(new SchemaTableName(schema, result.getString()));
            }
            return list.build();
        }
        catch (Exception e) {
            throw new PrestoException(GREMLIN_ERROR, e);
        }
    }

//    public ConnectorSplitSource getSplits(HugeGraphTableLayoutHandle layoutHandle)
//    {
//        HugeGraphTableHandle tableHandle = layoutHandle.getTable();
//        HugeGraphSplit jdbcSplit = new HugeGraphSplit
//                (connectorId,
//                tableHandle.getCatalogName(),
//                tableHandle.getSchemaName(),
//                tableHandle.getTableName(),
//                layoutHandle.getTupleDomain(),
//                Optional.empty());
//        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
//    }

    @Nullable
    public HugeGraphTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try {
            boolean isVertex = false;
            boolean isEdge = false;
            Client client = gremlinClientFactory.openClient();
            String schema = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();
            CompletableFuture<ResultSet> future = client.submitAsync("g=" + schema
                    + ".traversal(); g.V().hasLabel('" + tableName + "')");
            isVertex = !future.get().all().get().isEmpty();

            future = client.submitAsync("g=" + schema
                    + ".traversal(); g.E().hasLabel('" + tableName + "')");
            isEdge = !future.get().all().get().isEmpty();

            if (isVertex && isEdge) {
                throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
            }
            if (!isEdge && !isVertex) {
                return null;
            }

            return new HugeGraphTableHandle(connectorId,
                    schemaTableName,
                    schema,
                    schema,
                    tableName);
        }
        catch (Exception e) {
            throw new PrestoException(GREMLIN_ERROR, e);
        }
    }

    public List<HugeGraphColumnHandle> getColumns(ConnectorSession session, HugeGraphTableHandle tableHandle)
    {
        boolean isVertex = false;
        boolean isEdge = false;

        try {
            List<HugeGraphColumnHandle> columns = new ArrayList<>();

            Client client = gremlinClientFactory.openClient();
            String schema = tableHandle.getSchemaName();
            String tableName = tableHandle.getTableName();
            CompletableFuture<ResultSet> future = client.submitAsync("g=" + schema +
                    ".traversal(); g.V().hasLabel('" + tableName + "').properties().key()");
            isVertex = !future.get().all().get().isEmpty();
            if (isVertex) {
                List<Result> results = future.get().all().join();
                for (int i = 0; i < future.get().all().get().size(); i++) {
                    HugeGraphTypeHandle hugeGraphTypeHandle = new HugeGraphTypeHandle(0);
                    columns.add(new HugeGraphColumnHandle(connectorId, results.get(i).toString(), hugeGraphTypeHandle, toPrestoType(hugeGraphTypeHandle), false));
                }
            }

            future = client.submitAsync("g=" + schema
                    + ".traversal(); g.E().hasLabel('" + tableName + "').properties().key()");
            isEdge = !future.get().all().get().isEmpty();

            if (isVertex && isEdge) {
                throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + tableName);
            }
            if (!isEdge && !isVertex) {
                return null;
            }

            return columns;
        }
        catch (Exception e) {
            throw new PrestoException(GREMLIN_ERROR, e);
        }
    }

    private Type toPrestoType(HugeGraphTypeHandle hugeGraphTypeHandle)
    {
//        switch (hugeGraphTypeHandle.getHugeGraphType()) {
//
//        }
        return INTEGER;
    }

    @PreDestroy
    public void destroy()
            throws Exception
    {
        gremlinClientFactory.close();
    }

    public ConnectorSplitSource getSplits(HugeGraphTableLayoutHandle layoutHandle)
    {
        HugeGraphTableHandle tableHandle = layoutHandle.getTable();
        HugeGraphSplit hugeGraphSplit = new HugeGraphSplit(connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                layoutHandle.getTupleDomain(),
                Optional.empty(), null);
        return new FixedSplitSource(ImmutableList.of(hugeGraphSplit));
    }
}
