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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.facebook.presto.plugin.neo4j.Neo4jErrorCode.METADATA_ERROR;
import static com.facebook.presto.plugin.neo4j.Neo4jErrorCode.NOT_SUPPORT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
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

        if (!schema.contains("|")) {
            try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT table_name FROM graph WHERE table_name = ?;");
                preparedStatement.setString(1, table);
                ResultSet rs = preparedStatement.executeQuery();

                if (!rs.next()) {
                    return null;
                }
                return new Neo4jTableHandle(connectorId,
                        tableName,
                        "neo4j",
                        schema,
                        table,
                        ImmutableList.of(table),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        false);
            }
            catch (Exception e) {
                throw new PrestoException(METADATA_ERROR, e);
            }
        }

        ImmutableList.Builder<String> nodeTypesBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<String> relationshipTypesBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<String> nodeNamesBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<String> relationshipNamesBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<String> arguments = new ImmutableList.Builder<>();

        String[] parts = schema.split("\\|");
        Arrays.stream(parts[0].substring(1, parts[0].length() - 1).split(",")).forEach(s -> nodeTypesBuilder.add(s.trim()));
        Arrays.stream(parts[2].substring(1, parts[2].length() - 1).split(",")).forEach(s -> nodeNamesBuilder.add(s.trim()));
        List<String> nodeTypes = nodeTypesBuilder.build();
        if (nodeTypes.size() > 1) {
            Arrays.stream(parts[1].substring(1, parts[1].length() - 1).split(",")).forEach(s -> relationshipTypesBuilder.add(s.trim()));
            Arrays.stream(parts[3].substring(1, parts[3].length() - 1).split(",")).forEach(s -> relationshipNamesBuilder.add(s.trim()));
        }
        if (parts[4].length() > 2) {
            Arrays.stream(parts[4].substring(1, parts[4].length() - 1).split(",")).forEach(s -> arguments.add(s.trim()));
        }

        boolean isPath;
//        if (schema.contains("*")) {
//            isPath = true;
//        }
//        else {
        isPath = false;
//        }

        //TODO: 加上如果在查询元数据中没有的Pattern时，不返回TableHandle的逻辑
        return new Neo4jTableHandle(connectorId,
                tableName,
                "neo4j",
                schema,
                table,
                nodeTypes,
                relationshipTypesBuilder.build(),
                nodeNamesBuilder.build(),
                relationshipNamesBuilder.build(),
                arguments.build(),
                isPath);
    }

    //TODO: add nullable support.
    //TODO: when more than one type for a node or a relationship, these types may not have any same column, so the record is zero, select should be ended here or rewrite.
    public List<Neo4jColumnHandle> getColumns(Neo4jTableHandle tableHandle)
    {
        ImmutableList.Builder<Neo4jColumnHandle> columns = new ImmutableList.Builder<>();

        List<String> nodes = tableHandle.getNodeTypes();
        List<String> relationships = tableHandle.getRelationshipTypes();

        if (tableHandle.isPath()) {
            return columns.add(new Neo4jColumnHandle(connectorId, tableHandle.getTableName(), new Neo4jTypeHandle(Types.VARCHAR, null), toPrestoType(Types.VARCHAR, null), false)).build();
        }

        List<String> arguments = tableHandle.getArguments();
        if (arguments.size() != 0) {
            switch (arguments.get(0).toLowerCase(Locale.ENGLISH)) {
                case "astar":
                    nodes.forEach(
                            node -> {
                                boolean flag = node.equals(nodes.get(0));
                                if (!flag) {
                                    throw new PrestoException(NOT_SUPPORT, "A* cannot have different type of node");
                                }
                            });

                    try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
                        PreparedStatement preparedStatement = connection.prepareStatement("SELECT column_name, column_type, parameters FROM graph WHERE table_name = ? AND column_name = ?;");
                        preparedStatement.setString(1, nodes.get(0).substring(1));
//                        preparedStatement.setString(2, arguments.get(5));
                        preparedStatement.setString(2, "id");

                        ResultSet rs = preparedStatement.executeQuery();

                        if (rs.next()) {
                            int type = rs.getInt("column_type");
                            String parameters = rs.getString("parameters");
                            columns.add(new Neo4jColumnHandle(connectorId, rs.getString("column_name").trim(), new Neo4jTypeHandle(type, parameters), toPrestoType(type, parameters), false));

                            columns.add(new Neo4jColumnHandle(connectorId, "cost", new Neo4jTypeHandle(Types.DOUBLE, null), toPrestoType(Types.DOUBLE, null), false));
                            return columns.build();
                        }
                        else {
                            throw new PrestoException(METADATA_ERROR, "The specified id column does not exist.");
                        }
                    }
                    catch (Exception e) {
                        throw new PrestoException(METADATA_ERROR, e);
                    }
                default:
                    throw new PrestoException(NOT_SUPPORT, "neo4j connector do not support this function.");
            }
        }

        try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT column_name, column_type, parameters FROM graph WHERE table_name = ?;");

            if (nodes.size() == 1 && !nodes.get(0).contains(":")) {
                preparedStatement.setString(1, nodes.get(0));
                ResultSet rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    int type = rs.getInt("column_type");
                    String parameters = rs.getString("parameters");
                    columns.add(new Neo4jColumnHandle(connectorId, rs.getString("column_name").trim(), new Neo4jTypeHandle(type, parameters), toPrestoType(type, parameters), false));
                }
                return columns.build();
            }

            for (int i = 0; i < nodes.size(); i++) {
                String[] parts = nodes.get(i).split(":");
                ArrayList<Neo4jColumnHandle>[] handleLists = new ArrayList[parts.length];
                for (int j = 1; j < parts.length; j++) {
                    handleLists[j] = new ArrayList<>();
                    preparedStatement.setString(1, parts[j]);
                    ResultSet rs = preparedStatement.executeQuery();
                    while (rs.next()) {
                        int type = rs.getInt("column_type");
                        String parameters = rs.getString("parameters");
                        handleLists[j].add(new Neo4jColumnHandle(connectorId, "node" + i + rs.getString("column_name").trim(), new Neo4jTypeHandle(type, parameters), toPrestoType(type, parameters), false));
                        handleLists[1].retainAll(handleLists[j]);
                    }
                }
                if (parts.length > 1) {
                    columns.addAll(handleLists[1]);
                }
                else {
                    columns.add(new Neo4jColumnHandle(connectorId, "node" + i, new Neo4jTypeHandle(Types.VARCHAR, null), toPrestoType(Types.VARCHAR, null), false));
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
                            String parameters = rs.getString("parameters");
                            handleLists[j].add(new Neo4jColumnHandle(connectorId, "relationship" + i + rs.getString("column_name").trim(), new Neo4jTypeHandle(type, parameters), toPrestoType(type, parameters), false));
                            handleLists[1].retainAll(handleLists[j]);
                        }
                    }
                    if (parts.length > 1) {
                        columns.addAll(handleLists[1]);
                    }
                    else {
                        columns.add(new Neo4jColumnHandle(connectorId, "relationship" + i, new Neo4jTypeHandle(Types.VARCHAR, null), toPrestoType(Types.VARCHAR, null), false));
                    }
                }
            }

            return columns.build();
        }
        catch (Exception e) {
            throw new PrestoException(METADATA_ERROR, e);
        }
    }

    // Types without parameters will ignore parameters during conversion
    private Type toPrestoType(int type, String parameters)
    {
        switch (type) {
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.DOUBLE:
                return DOUBLE;
            case Types.DECIMAL:
                if (parameters != null) {
                    try {
                        if (parameters.contains(",")) {
                            int[] p = Arrays.stream(parameters.split(",")).mapToInt(Integer::parseInt).toArray();
                            return DecimalType.createDecimalType(p[0], p[1]);
                        }
                        return DecimalType.createDecimalType(Integer.parseInt(parameters));
                    }
                    catch (Exception e) {
                        throw new PrestoException(METADATA_ERROR, format("The parameters %s do not apply to the type DECIMAL", parameters));
                    }
                }
                return DecimalType.createDecimalType();
            case Types.VARCHAR:
                if (parameters != null) {
                    try {
                        int p = Integer.parseInt(parameters);
                        if (p == UNBOUNDED_LENGTH) {
                            return VarcharType.createUnboundedVarcharType();
                        }
                        return VarcharType.createVarcharType(p);
                    }
                    catch (Exception e) {
                        throw new PrestoException(METADATA_ERROR, format("The parameters %s do not apply to the type VARCHAR", parameters));
                    }
                }
                return VARCHAR;
            case Types.CHAR:
                if (parameters != null) {
                    try {
                        return CharType.createCharType(Integer.parseInt(parameters));
                    }
                    catch (Exception e) {
                        throw new PrestoException(METADATA_ERROR, format("The parameters %s do not apply to the type CHAR", parameters));
                    }
                }
                throw new PrestoException(METADATA_ERROR, "The type CHAR must have parameter");
            case Types.VARBINARY:
                return VARBINARY;
            case Types.DATE:
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
                throw new PrestoException(METADATA_ERROR, format("%d is a presto supported, but neo4j connector non-supported type.", type));
            default:
                throw new PrestoException(METADATA_ERROR, format("The metadata manager returned a non-support type %d", type));
        }
    }

    private int toJdbcType(String base)
    {
        switch (base) {
            case StandardTypes.BOOLEAN:
                return Types.BOOLEAN;
            case StandardTypes.TINYINT:
                return Types.TINYINT;
            case StandardTypes.SMALLINT:
                return Types.SMALLINT;
            case StandardTypes.INTEGER:
                return Types.INTEGER;
            case StandardTypes.BIGINT:
                return Types.BIGINT;
            case StandardTypes.REAL:
                return Types.REAL;
            case StandardTypes.DOUBLE:
                return Types.DOUBLE;
            case StandardTypes.DECIMAL:
                return Types.DECIMAL;
            case StandardTypes.VARCHAR:
                return Types.VARCHAR;
            case StandardTypes.CHAR:
                return Types.CHAR;
            case StandardTypes.VARBINARY:
                return Types.VARBINARY;
            case StandardTypes.JSON:
            case StandardTypes.DATE:
            case StandardTypes.TIME:
            case StandardTypes.TIME_WITH_TIME_ZONE:
            case StandardTypes.TIMESTAMP:
            case StandardTypes.INTERVAL_YEAR_TO_MONTH:
            case StandardTypes.INTERVAL_DAY_TO_SECOND:
                throw new PrestoException(NOT_SUPPORT, format("The type %s is been supported by presto, but no by neo4j connector yet", base));
            default:
                throw new PrestoException(NOT_SUPPORT, format("The type %s is not supported by neo4j connector", base));
        }
    }

    //TODO: createTable和dropTable目前无法区分点和边，只能删除点
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        //目前解决不了确认Schema是否存在的问题
        String tableName = tableMetadata.getTable().getTableName();

        try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO graph(table_name, column_name, column_type, parameters) VALUES (?, ?, ?, ?)");
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, column.getName());
                TypeSignature signature = column.getType().getTypeSignature();
                preparedStatement.setInt(3, toJdbcType(signature.getBase()));
                preparedStatement.setString(4, Joiner.on(", ").join(signature.getParameters()));
                preparedStatement.execute();
            }
        }
        catch (Exception e) {
            throw new PrestoException(METADATA_ERROR, "failed to insert metadata into metadata manager", e);
        }
    }

    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Neo4jTableHandle neo4jTableHandle = (Neo4jTableHandle) tableHandle;
        String tableName = neo4jTableHandle.getTableName();

        try (Connection connection = metadataDriver.connect(config.getMetadataUrl(), metadataProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM graph WHERE table_name = ?");
            preparedStatement.setString(1, tableName);
            preparedStatement.execute();
        }
        catch (Exception e) {
            throw new PrestoException(METADATA_ERROR, "failed to delete metadata from metadata manager", e);
        }

        Session neo4jSession = openSession();
        neo4jSession.run("MATCH(n:" + tableName + ") DELETE n");
    }

    public Session openSession()
    {
        return driver.session();
    }
}
