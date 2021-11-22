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
 *
 * Note: This file contains changes from Trinodb.  Specifically the decodeTree and locateNode methods
 * (rev 03d8e5abc686c5c4a9f96ca14db07e2aed880174)
 * https://github.com/trinodb/trino/blob/359/lib/trino-record-decoder/src/main/java/io/trino/decoder/json/JsonRowDecoder.java
 */

package io.trino.plugin.pravega.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.JsonFieldDecoder;
import io.trino.plugin.pravega.PravegaRecordValue;
import io.trino.plugin.pravega.TypedRecordValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class JsonRowDecoder
        implements EventDecoder
{
    private final ObjectMapper objectMapper;

    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    public JsonRowDecoder(ObjectMapper objectMapper, Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders)
    {
        this.objectMapper = objectMapper;
        this.fieldDecoders = fieldDecoders;
    }

    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        ((TypedRecordValue) record).setDecodedValue(decodeTree(event.asJson()));
        return true;
    }

    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeTree(JsonNode tree)
    {
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();

        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }

        return Optional.of(decodedRow);
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}
