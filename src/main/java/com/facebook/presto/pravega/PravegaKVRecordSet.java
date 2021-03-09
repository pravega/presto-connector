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

package com.facebook.presto.pravega;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.pravega.decoder.DecodableEvent;
import com.facebook.presto.pravega.decoder.KVSerializer;
import com.facebook.presto.pravega.decoder.MultiSourceRowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.TableEntry;
import io.pravega.common.util.AsyncIterator;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.pravega.util.PravegaSerializationUtils.deserialize;
import static java.util.Objects.requireNonNull;

public class PravegaKVRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(PravegaKVRecordSet.class);

    private final PravegaProperties properties;

    private final PravegaSplit split;
    private final PravegaSegmentManager segmentManager;

    private final MultiSourceRowDecoder eventDecoder;

    private final List<PravegaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final List<KVSerializer<?>> serializers;

    PravegaKVRecordSet(PravegaProperties properties,
                       PravegaSplit split,
                       PravegaSegmentManager segmentManager,
                       List<PravegaColumnHandle> columnHandles,
                       MultiSourceRowDecoder eventDecoder,
                       List<KVSerializer<?>> serializers)
    {
        this.properties = requireNonNull(properties, "properties is null");

        this.split = requireNonNull(split, "split is null");

        this.segmentManager = requireNonNull(segmentManager, "segmentManager is null");

        this.eventDecoder = requireNonNull(eventDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        this.serializers = requireNonNull(serializers, "serializers is null");

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
        // listing from a kv table.keyfamily
        //
        // iterator we get will fetch N items at a time - this is batchIter
        // from there we keep a local iterator to iterate over those N items - this is itemIter
        // when itemIter is done, fetch more from batchIter
        // batchIter returns null on EOF

        PravegaKVTable kvTable = deserialize(split.getReaderArgs(), PravegaKVTable.class);
        AsyncIterator<IteratorItem<TableEntry<?, ?>>> batchIter = constructIterator(kvTable);

        final Iterator<DecodableEvent> eventIterator = new Iterator<DecodableEvent>()
        {
            boolean eof;
            DecodableEvent nextEvent;
            Iterator<TableEntry<?, ?>> itemIter;

            @Override
            public boolean hasNext()
            {
                return nextEvent != null || _next() != null;
            }

            @Override
            public DecodableEvent next()
            {
                try {
                    if (nextEvent != null) {
                        return nextEvent;
                    }
                    return _next();
                }
                finally {
                    nextEvent = null;
                }
            }

            Iterator<TableEntry<?, ?>> iter()
            {
                // return next batch of key/values
                IteratorItem<TableEntry<?, ?>> item = batchIter.getNext().join();
                if (item == null) {
                    return null;
                }

                return item.getItems().iterator();
            }

            private DecodableEvent _next()
            {
                if (eof) {
                    return null;
                }

                if (itemIter == null || !itemIter.hasNext()) {
                    // no more items, get next batch
                    itemIter = iter();
                    if (itemIter == null) {
                        eof = true;
                        return null;
                    }
                }

                TableEntry<?, ?> tableEntry = itemIter.next();
                // 1 serializer for key, 1 for value
                DecodableEvent value = serializers.get(1).toEvent(tableEntry.getValue());
                nextEvent = serializers.get(0).toEvent(tableEntry.getKey().getKey());
                nextEvent.setNext(value); // key, followed by value
                return nextEvent;
            }
        };

        return new PravegaRecordCursor(eventIterator, columnHandles, eventDecoder, properties, "kv");
    }

    AsyncIterator<IteratorItem<TableEntry<?, ?>>> constructIterator(final PravegaKVTable kvTable)
    {
        KeyValueTable table = segmentManager.getKeyValueTableFactory(kvTable.getScope())
                .forKeyValueTable(kvTable.getTable(),
                        serializers.get(0),
                        serializers.get(1),
                        KeyValueTableClientConfiguration.builder().build());

        return table.entryIterator(kvTable.getKeyFamily(), 1024, null);
    }
}
