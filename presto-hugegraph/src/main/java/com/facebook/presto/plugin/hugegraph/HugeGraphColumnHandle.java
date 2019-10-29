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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class HugeGraphColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final HugeGraphTypeHandle hugeGraphTypeHandle;
    private final Type columnType;
    private final boolean nullable;

    public HugeGraphColumnHandle(String connectorId,
            String columnName,
            HugeGraphTypeHandle hugeGraphTypeHandle,
            Type columnType,
            boolean nullable)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "cloumnName is null");
        this.hugeGraphTypeHandle = requireNonNull(hugeGraphTypeHandle, "hugeGraphTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public HugeGraphTypeHandle getJdbcTypeHandle()
    {
        return hugeGraphTypeHandle;
    }

    public Type getColumnType()
    {
        return columnType;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, nullable, null, null, false, emptyMap());
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
        HugeGraphColumnHandle o = (HugeGraphColumnHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("hugeGraphTypeHandle", hugeGraphTypeHandle)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .toString();
    }
}
