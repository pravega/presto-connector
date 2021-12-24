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

package io.pravega.connectors.presto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import io.pravega.connectors.presto.decoder.DecodableEvent;
import io.pravega.connectors.presto.decoder.EventDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import io.pravega.client.batch.SegmentRange;

import java.util.Iterator;
import java.util.List;

import static io.pravega.connectors.presto.util.PravegaSerializationUtils.deserialize;
import static java.util.Objects.requireNonNull;

public class PravegaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(PravegaRecordSet.class);

    private final PravegaProperties properties;

    private final PravegaSplit split;
    private final PravegaSegmentManager segmentManager;

    private final EventDecoder eventDecoder;

    private final List<PravegaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    PravegaRecordSet(PravegaProperties properties,
                     PravegaSplit split,
                     PravegaSegmentManager segmentManager,
                     List<PravegaColumnHandle> columnHandles,
                     EventDecoder eventDecoder)
    {
        this.properties = requireNonNull(properties, "properties is null");

        this.split = requireNonNull(split, "split is null");

        this.segmentManager = requireNonNull(segmentManager, "segmentManager is null");

        this.eventDecoder = requireNonNull(eventDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        Iterator<DecodableEvent> eventIterator =
                new SegmentEventIterator(segmentManager, deserialize(split.getReaderArgs(), SegmentRange.class));

        return new PravegaRecordCursor(eventIterator, columnHandles, eventDecoder, properties, split.getSchema().get(0).getFormat());
    }
}
