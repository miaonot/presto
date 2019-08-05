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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class HugeGraphModule
        implements Module
{
    private final String connectorId;

    public HugeGraphModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HugeGraphConnectorId.class).toInstance(new HugeGraphConnectorId(connectorId));
        binder.bind(HugeGraphConnector.class).in(Scopes.SINGLETON);
        binder.bind(HugeGraphMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HugeGraphClient.class).in(Scopes.SINGLETON);
        binder.bind(HugeGraphSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HugeGraphRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HugeGraphHandleResolver.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HugeGraphConfiguration.class);
    }
}
