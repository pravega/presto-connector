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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.pravega.connectors.presto.PravegaTestUtils.getKvStreamDesc;
import static org.testng.Assert.assertEquals;

@Test
public class TestPravegaKeyValue
{
    private final EmbeddedPravega pravega;

    private final DistributedQueryRunner queryRunner;

    public TestPravegaKeyValue()
            throws Exception
    {
        this.pravega = new EmbeddedPravega();
        this.queryRunner = PravegaQueryRunner.createQueryRunner(pravega.getController(), java.util.Collections.emptyList(), KeyValueTable.getTables());
    }

    @Test
    public void testLoadSimpleSchema()
    {
        PravegaStreamDescription streamDescription = getKvStreamDesc("employee");
        streamDescription.getEvent().orElseThrow(IllegalStateException::new);

        List<PravegaStreamFieldDescription> keyFields = streamDescription.getEvent().get().get(0).getFields();
        assertEquals(1, keyFields.size());
        assertEquals("key/id", keyFields.get(0).getName());

        List<PravegaStreamFieldDescription> valueFields = streamDescription.getEvent().get().get(1).getFields();
        assertEquals(2, valueFields.size());
        assertEquals("value/first", valueFields.get(0).getName());
        assertEquals("value/last", valueFields.get(1).getName());
    }

    @Test
    public void testSelectCount()
    {
        MaterializedResult result = queryRunner.execute(kvSession(), "select count(*) from employee");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3L);
    }

    @Test
    public void testSelectConstraint()
    {
        MaterializedResult result = queryRunner.execute(kvSession(), "select \"value/last\" from employee where \"key/id\" = 2");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "doe");
    }

    public static Session kvSession()
    {
        return testSessionBuilder().setCatalog("pravega").setSchema("kv").build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        pravega.close();
    }
}
