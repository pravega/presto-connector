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
 * Note: This file contains changes from PrestoDb.  Specifically the decodeTree and locateNode methods
 * (rev cb21100597c1aec4c6f29e4a82117886aaa95d10)
 * https://github.com/prestodb/presto/blob/0.235/presto-record-decoder/src/main/java/com/facebook/presto/decoder/json/JsonRowDecoder.java
 */

package io.pravega.connectors.presto.decoder;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.json.JsonFieldDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import io.pravega.connectors.presto.PravegaRecordValue;
import io.pravega.connectors.presto.TypedRecordValue;

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
