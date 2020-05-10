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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

public class Neo4jConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String metadataUrl;
    private String metadataUser;
    private String metadataPassword;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public Neo4jConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public Neo4jConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public Neo4jConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public String getMetadataUrl()
    {
        return metadataUrl;
    }

    @Config("metadata-url")
    public Neo4jConfig setMetadataUrl(String metadataUrl)
    {
        this.metadataUrl = metadataUrl;
        return this;
    }

    public String getMetadataUser()
    {
        return metadataUser;
    }

    @Config("metadata-user")
    public Neo4jConfig setMetadataUser(String metadataUser)
    {
        this.metadataUser = metadataUser;
        return this;
    }

    public String getMetadataPassword()
    {
        return metadataPassword;
    }

    @Config("metadata-password")
    @ConfigSecuritySensitive
    public Neo4jConfig setMetadataPassword(String metadataPassword)
    {
        this.metadataPassword = metadataPassword;
        return this;
    }
}
