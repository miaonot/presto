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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Neo4jTypeHandle
{
    private final int type;
    private final String parameters;

    @JsonCreator
    public Neo4jTypeHandle(
            @JsonProperty("type") int type,
            @JsonProperty("parameters") String parameters)
    {
        this.type = type;
        this.parameters = parameters;
    }

    @JsonProperty
    public int getType()
    {
        return type;
    }

    @JsonProperty
    public String getParameters()
    {
        return parameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, parameters);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Neo4jTypeHandle that = (Neo4jTypeHandle) o;
        return type == that.type &&
                parameters == that.parameters;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("columnSize", parameters)
                .toString();
    }
}
