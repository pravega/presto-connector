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
import com.facebook.presto.decoder.FieldValueProvider;
import io.pravega.connectors.presto.PravegaRecordValue;
import io.pravega.connectors.presto.TypedRecordValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// row comprised of N sources
// (kv table, where we have a key + value which are dealt with separately)
public class MultiSourceRowDecoder
        implements EventDecoder
{
    private final List<EventDecoder> rowDecoders;

    public MultiSourceRowDecoder(List<EventDecoder> rowDecoders)
    {
        this.rowDecoders = rowDecoders;
    }

    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        // rowDecoder[0] <-> event
        // rowDecoder[1] <-> event.next()
        // rowDecoder[N] <-> ..

        TypedRecordValue legacyRecord = (TypedRecordValue) record;
        Map<DecoderColumnHandle, FieldValueProvider> result = new HashMap<>();

        for (EventDecoder rowDecoder : rowDecoders) {
            if (event == null) {
                throw new IllegalArgumentException("no more events, decoder set of " + rowDecoders.size());
            }
            rowDecoder.decodeEvent(event, legacyRecord);
            legacyRecord.getDecodedValue().ifPresent(result::putAll);
            event = event.next();
        }

        legacyRecord.setDecodedValue(Optional.of(result));
        return true;
    }
}
