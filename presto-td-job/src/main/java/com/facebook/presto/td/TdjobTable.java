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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TdjobTable
{
    public static final JsonCodec<List<List<String>>> COLUMNS_CODEC = JsonCodec.listJsonCodec(JsonCodec.listJsonCodec(String.class));
    public static final JsonCodec<List<Map<String, String>>> COLUMNS_CODEC_GPDATA = JsonCodec.listJsonCodec(JsonCodec.mapJsonCodec(String.class, String.class));

    private long cpuTime = 0;
    private String createAt;
    private String database;
    private int duration = 0;
    private String endAt;
    private String hiveResultSchema = "";
    private String resultSchema = "";
    private String name;
    private String organization = "default";
    private int priority = 0;
    private String query;
    private String result = "";
    private int resultSize = 0;
    private int retryLimit = 0;
    private String startAt;
    private String status;
    private String type;
    private String updatedAt = "";
    private String url = "";
    private String userName;

    private List<TdjobColumn> columns;
    private List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public TdjobTable(
            @JsonProperty("cpu_time") long cpuTime,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("database") String database,
            @JsonProperty("duration") int duration,
            @JsonProperty("end_at") String endAt,
            @JsonProperty("hive_result_schema") String hiveResultSchema,
            @JsonProperty("result_schema") String resultSchema,
            @JsonProperty("job_id") String jobId,
            @JsonProperty("organization") String organization,
            @JsonProperty("priority") int priority,
            @JsonProperty("query") String query,
            @JsonProperty("result") String result,
            @JsonProperty("result_size") int resultSize,
            @JsonProperty("retry_limit") int retryLimit,
            @JsonProperty("start_at") String startAt,
            @JsonProperty("status") String status,
            @JsonProperty("type") String type,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("url") String url,
            @JsonProperty("user_name") String userName)
    {
        checkArgument(!isNullOrEmpty(database), "database is null or is empty");
        checkArgument(!isNullOrEmpty(jobId), "jobId is null or is empty");

        // error jobs do not have schema
        // checkArgument(!isNullOrEmpty(hiveResultSchema), String.format("hiveResultSchema(job_id:%s) is null or is empty", jobId));

        this.cpuTime = cpuTime;
        this.createAt = createdAt;
        this.database = database;
        this.duration = duration;
        this.endAt = endAt;
        this.hiveResultSchema = hiveResultSchema;
        this.resultSchema = resultSchema;
        this.name = jobId;
        this.organization = organization;
        this.priority = priority;
        this.query = query;
        this.result = result;
        this.resultSize = resultSize;
        this.retryLimit = retryLimit;
        this.startAt = startAt;
        this.status = status;
        this.type = type;
        this.updatedAt = updatedAt;
        this.url = url;
        this.userName = userName;

        if (this.hiveResultSchema == null || this.hiveResultSchema.isEmpty()) {
            this.hiveResultSchema = this.resultSchema;
        }

        if (status.toLowerCase().equals("success") || status.toLowerCase().equals("finished")) {
            if (!Strings.isNullOrEmpty(this.resultSchema)) {
                List<Map<String, String>> rawColumns = COLUMNS_CODEC_GPDATA.fromJson(this.resultSchema);

                int index = 0;
                ImmutableList.Builder<TdjobColumn> columns = ImmutableList.builder();
                ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
                for (Map<String, String> column : rawColumns) {
                    checkArgument(column.size() == 2, "Column meta in hive result schema should consists of exactly 2 elements");
                    TdjobColumn tdjobColumn = new TdjobColumn(column.get("name"), column.get("type"));
                    columns.add(tdjobColumn);
                    columnsMetadata.add(new ColumnMetadata(tdjobColumn.getName(), tdjobColumn.getType(), index, false));
                    index++;
                }
                this.columns = columns.build();
                this.columnsMetadata = columnsMetadata.build();
            }
            else if (!Strings.isNullOrEmpty(this.hiveResultSchema)) {
                List<List<String>> rawColumns = COLUMNS_CODEC.fromJson(this.hiveResultSchema);

                int index = 0;
                ImmutableList.Builder<TdjobColumn> columns = ImmutableList.builder();
                ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
                for (List<String> column : rawColumns) {
                    checkArgument(column.size() == 2, "Column meta in hive result schema should consists of exactly 2 elements");
                    TdjobColumn tdjobColumn = new TdjobColumn(column.get(0), column.get(1));
                    columns.add(tdjobColumn);
                    columnsMetadata.add(new ColumnMetadata(tdjobColumn.getName(), tdjobColumn.getType(), index, false));
                    index++;
                }
                this.columns = columns.build();
                this.columnsMetadata = columnsMetadata.build();
            }
        }
        else {
            this.columns = null;
            this.columnsMetadata = null;
        }
    }

    @JsonProperty("cpu_time")
    public long getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty("created_at")
    public String getCreatedAt()
    {
        return createAt;
    }

    @JsonProperty
    public String getDatabase()
    {
        return database;
    }

    @JsonProperty
    public long getDuration()
    {
        return duration;
    }

    @JsonProperty("end_at")
    public String getEndAt()
    {
        return endAt;
    }

    @JsonProperty("hive_result_schema")
    public String getHiveResultSchema()
    {
        return hiveResultSchema;
    }

    @JsonProperty("result_schema")
    public String getResultSchema()
    {
        return resultSchema;
    }

    @JsonProperty("job_id")
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getOrganization()
    {
        return organization;
    }

    @JsonProperty
    public int getPriority()
    {
        return priority;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getResult()
    {
        return result;
    }

    @JsonProperty("result_size")
    public int getResultSize()
    {
        return resultSize;
    }

    @JsonProperty("retry_limit")
    public int getRetryLimit()
    {
        return retryLimit;
    }

    @JsonProperty("start_at")
    public String getStartAt()
    {
        return startAt;
    }

    @JsonProperty
    public String getStatus()
    {
        return status;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty("updated_at")
    public String getUpdatedAt()
    {
        return updatedAt;
    }

    @JsonProperty
    public String getUrl()
    {
        return url;
    }

    @JsonProperty("user_name")
    public String getUserName()
    {
        return userName;
    }

    public List<TdjobColumn> getColumns()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public static Function<TdjobTable, String> nameGetter()
    {
        return new Function<TdjobTable, String>()
        {
            @Override
            public String apply(TdjobTable table)
            {
                return table.getName();
            }
        };
    }
}
