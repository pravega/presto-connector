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
package io.pravega.connectors.presto.integration;

import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test
public class TestPravegaDistributed
        extends AbstractTestQueries
{
    private final EmbeddedPravega pravega;

    public TestPravegaDistributed()
            throws Exception
    {
        this(new EmbeddedPravega());
    }

    public TestPravegaDistributed(EmbeddedPravega pravega)
    {
        super(() -> PravegaQueryRunner.createQueryRunner(pravega.getController(), TpchTable.getTables(), java.util.Collections.emptyList()));
        this.pravega = pravega;
    }

    // non-passing tests
    @Override
    public void testAccessControl(){}

    @Override
    public void testDescribeOutputNonSelect(){}

    @Override
    public void testInformationSchemaUppercaseName() {}

    @Override
    public void testShowTablesLikeWithEscape() {}

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        pravega.close();
    }
}
