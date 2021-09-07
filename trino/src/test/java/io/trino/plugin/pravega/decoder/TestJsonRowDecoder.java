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

package io.trino.plugin.pravega.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pravega.PravegaColumnHandle;
import io.trino.plugin.pravega.PravegaRecordValue;
import io.trino.plugin.pravega.TypedRecordValue;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonRowDecoder
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final JsonRowDecoderFactory decoderFactory = new JsonRowDecoderFactory(objectMapper);

    private static final String FIELD1 = "field1";
    private static final String NESTED_FIELD1_MAPPING = "nested/field1";

    @Test
    public void testSimpleField()
    {
        /*
         *  column display name: "field1" mapping: "field1""
         *
         *  {
         *    "field1" : 10
         *  }
         */

        PravegaRecordValue recordValue = recordValue(FIELD1, FIELD1, BIGINT, 10);
        assertEquals(recordValue.getLong(0, 0), 10);
    }

    @Test
    public void testNestedField()
    {
        /*
         *  column display name: "field1" mapping: "nested/field1"
         *
         *  {
         *    "nested" : {
         *      "field1" : "john"
         *    }
         *  }
         */

        PravegaRecordValue recordValue = recordValue(FIELD1, NESTED_FIELD1_MAPPING, VARCHAR, "john");
        assertEquals(recordValue.getSlice(0, 0).toStringUtf8(), "john");
    }

    private PravegaRecordValue recordValue(String name, String mapping, Type type, Object value)
    {
        List<DecoderColumnHandle> columns = new ArrayList<>();
        columns.add(columnHandle(name, mapping, type));

        JsonRowDecoder decoder = decoderFactory.create(columns.stream().collect(toImmutableSet()));

        TypedRecordValue recordValue = new TypedRecordValue(columns.stream()
                .map(TestJsonRowDecoder::columnHandle)
                .collect(Collectors.toList()));

        assertTrue(decoder.decodeEvent(new JsonEvent(toJson(mapping, type, value)), recordValue));
        recordValue.decode();

        return recordValue;
    }

    private JsonNode toJson(String mapping, Type type, Object value)
    {
        ObjectNode objectNode = new ObjectNode(objectMapper.getNodeFactory());
        ObjectNode root = objectNode;

        Iterator<String> path = Arrays.stream(mapping.split("/")).iterator();

        String name = path.next();
        while (path.hasNext()) {
            ObjectNode tmp = new ObjectNode(objectMapper.getNodeFactory());
            objectNode.set(name, tmp);
            objectNode = tmp;
            name = path.next();
        }

        if (BIGINT.equals(type)) {
            objectNode.put(name, (int) value);
        }
        else if (VARCHAR.equals(type)) {
            objectNode.put(name, (String) value);
        }
        else {
            throw new IllegalArgumentException();
        }
        return root;
    }

    private static ColumnHandle columnHandle(DecoderColumnHandle handle)
    {
        return handle;
    }

    private static DecoderColumnHandle columnHandle(String name, String mapping, Type type)
    {
        return new PravegaColumnHandle("pravega",
                0 /* not used in json */,
                name,
                type,
                mapping,
                "",
                null,
                false /* keyDecoder */,
                false /* hidden  */,
                false /* internal */,
                0 /* schemaNum */);
    }
}
