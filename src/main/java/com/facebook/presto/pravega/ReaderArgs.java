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

package com.facebook.presto.pravega;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ReaderArgs
        implements Serializable
{
    private final String scope;
    private final String stream;
    private final StreamCutRange streamCutRange;
    private final String readerGroup;

    public ReaderArgs(@JsonProperty("scope") String scope,
                      @JsonProperty("stream") String stream,
                      @JsonProperty("streamCutRange") StreamCutRange streamCutRange,
                      @JsonProperty("readerGroup") String readerGroup)
    {
        this.scope = requireNonNull(scope, "scope is null");
        this.stream = requireNonNull(stream, "stream is null");
        this.streamCutRange = requireNonNull(streamCutRange, "streamCutRange is null");
        this.readerGroup = readerGroup; // may be null
    }

    @JsonProperty
    public String getScope()
    {
        return scope;
    }

    @JsonProperty
    public String getStream()
    {
        return stream;
    }

    @JsonProperty
    public StreamCutRange getStreamCutRange()
    {
        return streamCutRange;
    }

    @JsonProperty
    public String getReaderGroup()
    {
        return readerGroup;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("scope", scope)
                .add("stream", stream)
                .add("streamCutRange", streamCutRange)
                .add("readerGroup", readerGroup)
                .toString();
    }
}
