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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.plugin.neo4j.Neo4jErrorCode.METADATA_ERROR;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.MAX_LENGTH;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Neo4jClient
{
    private final Neo4jConfig config;

    private final Driver driver;
    private final org.postgresql.Driver metadataDriver;
    private final Properties metadataProperties;

    private final String connectorId;

    @Inject
    public Neo4jClient(Neo4jConnectorId connectorId, Neo4jConfig neo4jConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(neo4jConfig, "neo4jConfig is null");
        this.driver = GraphDatabase.driver(neo4jConfig.getConnectionUrl(), AuthTokens.basic(neo4jConfig.getConnectionUser(), neo4jConfig.getConnectionPassword()));

        metadataDriver = new org.postgresql.Driver();
        metadataProperties = new Properties();
        metadataProperties.setProperty("user", neo4jConfig.getMetadataUser());
        metadataProperties.setProperty("password", neo4jConfig.getMetadataPassword());
    }

    public Neo4jTableHandle getTableHandle(SchemaTableName tableName)
    {
        String schema = tableName.getSchemaName();
        String table = tableName.getTableName();

        ImmutableList.Builder<String> nodesBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<String> relationshipsBuilder = new ImmutableList.Builder<>();

        String[] splitParts1 = schema.split("\\(");
        for (int i = 1; i < splitParts1.length; i++) {
            String[] splitParts2 = splitParts1[i].split("\\)");
            if (splitParts2.length != 0 && splitParts2[0].contains(":")) {
                nodesBuilder.add(splitParts2[0].substring(splitParts2[0].indexOf(":")));
            }
        }

        splitParts1 = schema.split("\\[");
        for (int i = 1; i < splitParts1.length; i++) {
            String[] splitParts2 = splitParts1[i].split("]");
            String s = splitParts2.length != 0 ? splitParts2[0] : "";
            if (s.contains(":")) {
                relationshipsBuilder.add(s.substring(s.indexOf(":"), (s.contains("*") ? s.indexOf("*") : s.length())));
            }
        }

        ImmutableList<String> nodes = nodesBuilder.build();
        ImmutableList<String> relationships = relationshipsBuilder.build();
        boolean isPath;
        if (schema.contains("*") || nodes.size() != splitParts1.length || relationships.size() != splitParts1.length - 1) {
            isPath = true;
        }
        else {
            isPath = false;
        }

        return new Neo4jTableHandle(connectorId,
                tableName,
                "neo4j",
                schema,
                table,
                nodes,
                relationships,
                isPath);
    }

    //TODO: add nullable support.
    //TODO: when more then one type for a node or a relationship, these types may not have any same column, so the record is zero, select should be ended here or rewrite.
    public List<Neo4jColumnHandle> getColumns(Neo4jTableHandle tableHandle)
    {
        ImmutableList.Builder<Neo4jColumnHandle> columns = new ImmutableList.Builder<>();

        List<String> nodes = tableHandle.getNodes();
        List<String> relationships = tableHandle.getRelationships();

        if (tableHandle.isPath()) {
            return columns.add(new Neo4jColumnHandle(connectorId, tableHandle.getTableName(), new Neo4jTypeHandle(Types.VARCHAR, MAX_LENGTH), toPrestoType(Types.VARCHAR, MAX_LENGTH), false)).build();
        }

        try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT column_name, column_type, column_size FROM graph WHERE table_name = ?;");

            for (int i = 0; i < nodes.size(); i++) {
                String[] parts = nodes.get(i).split(":");
                ArrayList<Neo4jColumnHandle>[] handleLists = new ArrayList[parts.length];
                for (int j = 1; j < parts.length; j++) {
                    handleLists[j] = new ArrayList<>();
                    preparedStatement.setString(1, parts[j]);
                    ResultSet rs = preparedStatement.executeQuery();
                    while (rs.next()) {
                        int type = rs.getInt("column_type");
                        int columnSize = rs.getInt("column_size");
                        handleLists[j].add(new Neo4jColumnHandle(connectorId, "node" + i + rs.getString("column_name").trim(), new Neo4jTypeHandle(type, columnSize), toPrestoType(type, columnSize), false));
                        handleLists[1].retainAll(handleLists[j]);
                    }
                }
                if (parts.length > 1) {
                    columns.addAll(handleLists[1]);
                }
                if (i != nodes.size() - 1) {
                    parts = relationships.get(i).split(":");
                    handleLists = new ArrayList[parts.length];
                    for (int j = 1; j < parts.length; j++) {
                        handleLists[j] = new ArrayList<>();
                        preparedStatement.setString(1, parts[j]);
                        ResultSet rs = preparedStatement.executeQuery();
                        while (rs.next()) {
                            int type = rs.getInt("column_type");
                            int columnSize = rs.getInt("column_size");
                            handleLists[j].add(new Neo4jColumnHandle(connectorId, "relationship" + i + rs.getString("column_name").trim(), new Neo4jTypeHandle(type, columnSize), toPrestoType(type, columnSize), false));
                            handleLists[1].retainAll(handleLists[j]);
                        }
                    }
                    if (parts.length > 1) {
                        columns.addAll(handleLists[1]);
                    }
                }
            }

            return columns.build();
        }
        catch (Exception e) {
            throw new PrestoException(METADATA_ERROR, e);
        }
    }

    private Type toPrestoType(int type, int columnSize)
    {
        switch (type) {
            case Types.INTEGER:
                return INTEGER;
            case Types.VARCHAR:
                return VarcharType.createVarcharType(columnSize);
            case Types.ARRAY:
            default:
                return VARCHAR;
        }
    }

    public Session openSession()
    {
        return driver.session();
    }
}
