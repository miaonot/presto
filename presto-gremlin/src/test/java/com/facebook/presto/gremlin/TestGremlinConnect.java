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

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.junit.Test;

import java.util.List;

import static org.testng.Assert.assertNotNull;

public class TestGremlinConnect
{
    @Test
    public void testGremlinExecute()
    {
        try {
            GremlinConnect gremlinConnect = new GremlinConnect();
//            ResultSet resultSet = gremlinConnect.gremlinExecute("hugegraph.traversal().V().as('a').repeat(out().simplePath()).times(2).where(out().as('a')).path()");
            ResultSet resultSet = gremlinConnect.gremlinExecute("hugegraph.traversal().V().hasLabel('person').limit(1).properties().key()");
//            ResultSet resultSet = gremlinConnect.gremlinExecute("1+1");
            assertNotNull(resultSet);
            List<Result> results = resultSet.all().join();
            System.out.println(results.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
