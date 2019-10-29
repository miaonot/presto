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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;

import javax.annotation.Nullable;

import java.util.Objects;

public class HugeGraphTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public HugeGraphTableHandle(String connectorId,
            SchemaTableName schemaTableName,
            @Nullable String catalogName,
            @Nullable String schemaName,
            String tableName)
    {
        this.connectorId = connectorId;
        this.schemaTableName = schemaTableName;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        HugeGraphTableHandle o = (HugeGraphTableHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName);
    }
}
