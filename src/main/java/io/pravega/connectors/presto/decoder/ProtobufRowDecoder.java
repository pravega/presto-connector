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

package io.pravega.connectors.presto.decoder;

import com.facebook.presto.decoder.DecoderColumnHandle;
import io.pravega.connectors.presto.PravegaRecordValue;
import io.pravega.connectors.presto.TypedRecordValue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ProtobufRowDecoder
        implements EventDecoder
{
    private final Map<DecoderColumnHandle, ProtobufColumnDecoder> columnDecoders;

    public ProtobufRowDecoder(Set<DecoderColumnHandle> columns)
    {
        columnDecoders = columns.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private ProtobufColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new ProtobufColumnDecoder(columnHandle);
    }

    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        ((TypedRecordValue) record).setDecodedValue(
                Optional.of(columnDecoders.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry ->
                                entry.getValue().decodeField(event.asProtobuf())))));
        return true;
    }
}
