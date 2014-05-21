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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class TdjobListResult
{
    private final int count;
    private final int from;
    private final int to;
    private final List<TdjobTable> jobs;

    @JsonCreator
    public TdjobListResult(
            @JsonProperty("count") int count,
            @JsonProperty("from") int from,
            @JsonProperty("to") int to,
            @JsonProperty("jobs") List<TdjobTable> jobs)
    {
        this.count = count;
        this.from = from;
        this.to = to;
        this.jobs = ImmutableList.copyOf(checkNotNull(jobs, "jobs is null"));
    }

    @JsonProperty
    public int getCount()
    {
        return count;
    }

    @JsonProperty
    public int getFrom()
    {
        return from;
    }

    @JsonProperty
    public int getTo()
    {
        return to;
    }

    @JsonProperty
    public List<TdjobTable> getJobs()
    {
        return jobs;
    }
}
