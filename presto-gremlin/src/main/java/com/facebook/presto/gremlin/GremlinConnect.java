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
package com.facebook.presto.gremlin;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.concurrent.CompletableFuture;

public class GremlinConnect
{
    public ResultSet gremlinExecute(String input)
    {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty("hosts", "10.77.110.131");
        configuration.addProperty("port", "8182"); //TODO

        Cluster cluster = Cluster.open(configuration);
        Client client = cluster.connect();
        CompletableFuture<ResultSet> future = client.submitAsync(input);

        try {
            return future.get();
        }
        catch (Exception e) {
            //TODO
            return null;
            // throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
