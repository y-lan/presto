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
package com.facebook.presto.td;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class TdjobConfig
{
    private Map<String, String> metadataKeys;

    @NotNull
    public Map<String, String> getMetadataKeys()
    {
        return metadataKeys;
    }

    @Config("metadata-keys")
    public TdjobConfig setMetadataKeys(String keys)
    {
        if (keys == null) {
            this.metadataKeys = null;
        }
        else {
            this.metadataKeys = ImmutableMap.copyOf(Splitter.on(',')
                    .omitEmptyStrings().trimResults().withKeyValueSeparator(':').split(keys));
        }
        return this;
    }
}
