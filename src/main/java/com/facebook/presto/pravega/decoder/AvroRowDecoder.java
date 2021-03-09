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
 * limitations under the License.Copyright (c) Pravega Authors.
 */

package com.facebook.presto.pravega.decoder;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.avro.AvroColumnDecoder;
import com.facebook.presto.pravega.PravegaRecordValue;
import com.facebook.presto.pravega.TypedRecordValue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AvroRowDecoder
        implements EventDecoder
{
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;

    public AvroRowDecoder(Set<DecoderColumnHandle> columns)
    {
        columnDecoders = columns.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        ((TypedRecordValue) record).setDecodedValue(
                Optional.of(columnDecoders.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry ->
                                entry.getValue().decodeField(event.asAvro())))));
        return true;
    }
}
