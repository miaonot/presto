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
import com.google.inject.Inject;
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
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class HugeGraphClient
{
    private static final Logger log = Logger.get(HugeGraphClient.class);

    protected final String connectorId;
    protected final GremlinClientFactory gremlinClientFactory;

    @Inject
    public HugeGraphClient(HugeGraphConnectorId hugeGraphConnectorId, HugeGraphConfig config,
            GremlinClientFactory gremlinClientFactory)
    {
        this.connectorId = requireNonNull(hugeGraphConnectorId.toString(), "connectorId is null");
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
        //Hard core
        String schema = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        return new HugeGraphTableHandle(connectorId, schemaTableName, "hugegraph", schema, tableName);
    }

    public List<HugeGraphColumnHandle> getColumns(ConnectorSession session, HugeGraphTableHandle tableHandle)
    {
        //Hard core
        List<HugeGraphColumnHandle> columns = new ArrayList<>();
        HugeGraphTypeHandle hugeGraphTypeHandle = new HugeGraphTypeHandle(0);
        columns.add(new HugeGraphColumnHandle(connectorId, "id", hugeGraphTypeHandle, toPrestoType(hugeGraphTypeHandle), false));
        return columns;
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
