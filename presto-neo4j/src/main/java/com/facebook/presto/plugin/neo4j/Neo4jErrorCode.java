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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum Neo4jErrorCode
        implements ErrorCodeSupplier
{
    NEO4J_ERROR(0, EXTERNAL),
    NEO4J_NOT_SUPPORT(1, EXTERNAL),
    METADATA_ERROR(2, EXTERNAL);

    private final ErrorCode errorCode;

    Neo4jErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0800_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return null;
    }
}
