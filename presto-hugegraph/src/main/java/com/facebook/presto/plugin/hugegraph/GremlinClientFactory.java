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

import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import static com.facebook.presto.plugin.hugegraph.GremlinErrorCode.GREMLIN_ERROR;
import static java.util.Objects.requireNonNull;

public class GremlinClientFactory
        implements AutoCloseable
{
    private final Cluster cluster;

    @Inject
    public GremlinClientFactory(HugeGraphConfig hugeGraphConfig)
    {
        requireNonNull(hugeGraphConfig, "configuration is null");
        Cluster temp;
        try {
            temp = Cluster.open(hugeGraphConfig.getHugeGraphConfigurationPath());
        }
        catch (Exception e) {
            throw new PrestoException(GREMLIN_ERROR, e);
        }
        cluster = temp;
    }

    public Client openClient()
    {
        return cluster.connect();
    }

    @Override
    public void close()
            throws Exception
    {}
}
