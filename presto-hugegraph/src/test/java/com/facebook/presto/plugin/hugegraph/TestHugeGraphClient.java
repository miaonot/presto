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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class TestHugeGraphClient
{
    public static final String connectorId = "test";
    private HugeGraphConfig config;
    private GremlinClientFactory gremlinClientFactory;

    @BeforeMethod
    public void setup()
    {
        config.setHugeGraphConfigurationPath("../presto-main/etc/catalog/hugegraph.yaml");
        gremlinClientFactory = new GremlinClientFactory(config);
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown()
            throws Exception
    {
        gremlinClientFactory.close();
    }

    @Test
    public void testGetSchemaNames()
    {
        HugeGraphClient hugeGraphClient = new HugeGraphClient(new HugeGraphConnectorId(connectorId), config, gremlinClientFactory);
        assertNotNull(hugeGraphClient.getSchemaNames());
    }
}
