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

import com.sun.istack.NotNull;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import org.apache.commons.configuration.BaseConfiguration;

public class HugeGraphConfiguration
        extends BaseConfiguration
{
    @Config("connection-hosts")
    public HugeGraphConfiguration setConnectionHosts(String connectionHosts)
    {
        addProperty("hosts", connectionHosts);
        return this;
    }

    @Config("connection-port")
    public HugeGraphConfiguration setConnectionPort(String connectionPort)
    {
        addProperty("port", connectionPort);
        return this;
    }

    @Config("connection-user")
    public HugeGraphConfiguration setConnectionUser(String connectionUser)
    {
        addProperty("username", connectionUser);
        return this;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public HugeGraphConfiguration setConnectionPassword(String connectionPassword)
    {
        addProperty("password", connectionPassword);
        return this;
    }

    @Config("connectionPool.enableSsl")
    public HugeGraphConfiguration setConnectionPoolEnableSsl(boolean connectionPoolEnableSsl)
    {
        addProperty("connectionPool.enableSsl", connectionPoolEnableSsl);
        return this;
    }

    @Config("connectionPool.sslEnabledProtocols")
    public HugeGraphConfiguration setConnectionPoolSslEnabledProtocols(String connectionPoolSslEnabledProtocols)
    {
        addProperty("connectionPool.sslEnabledProtocals", connectionPoolSslEnabledProtocols);
        return this;
    }

    @Config("serializer.className")
    public HugeGraphConfiguration setSerializerClassName(String serializerClassName)
    {
        addProperty("serializer.className", serializerClassName);
        return this;
    }

    @Config("serializer.config.serializeResultToString")
    public HugeGraphConfiguration setSerializerConfigSerializeResultToString(
            String serializerConfigSerializeResultToString)
    {
        addProperty("serializer.config.serializeResultToString", serializerConfigSerializeResultToString);
        return this;
    }

    @NotNull
    public String getConnectionHosts()
    {
        return getString("hosts");
    }

    @NotNull
    public String getConnectionPort()
    {
        return getString("port");
    }

    public String getConnectionUser()
    {
        return getString("username");
    }

    public String getConnectionPassword()
    {
        return getString("password");
    }

    public boolean getConnectionPoolEnableSsl()
    {
        return getBoolean("connectionPool.enableSsl");
    }

    public String getConnectionPoolSslEnabledProtocols()
    {
        return getString("connectionPool.sslEnabledProtocols");
    }

    public String getSerializerClassName()
    {
        return getString("serializer.className");
    }

    public String getSerializerConfigSerializeResultToString()
    {
        return getString("serializer.config.serializeResultToString");
    }
}
