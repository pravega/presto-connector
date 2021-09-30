/*
 * Copyright (c) Pravega Authors.
 *
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
package io.pravega.demo.objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Sensor {
    private final String name;
    private final long measurement;
    private final long timestamp;

    @JsonCreator
    public Sensor(@JsonProperty("name") String name,
                  @JsonProperty("measurement") long measurement,
                  @JsonProperty("timestamp") long timestamp) {
        this.name = name;
        this.measurement = measurement;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public long getMeasurement() {
        return measurement;
    }

    @JsonProperty
    public long getTimestamp() {
        return timestamp;
    }
}