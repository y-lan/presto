/*
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
package com.facebook.presto.operator;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestUnnestOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<bigint>"));
        Type mapType = metadata.getType(parseTypeSignature("map<bigint,bigint>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2, arrayBlockOf(BIGINT, 99), null)
                .row(3, null, null)
                .pageBreak()
                .row(6, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1, 2, 4, 5)
                .row(1, 3, null, null)
                .row(2, 99, null, null)
                .row(6, 7, 9, 10)
                .row(6, 8, 11, 12)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testUnnestWithArray()
            throws Exception
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<array<bigint>>"));
        Type mapType = metadata.getType(parseTypeSignature("map<array<bigint>,array<bigint>>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(
                        1,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(2, 4), ImmutableList.of(3, 6)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(4, 8), ImmutableList.of(5, 10))))
                .row(2, arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(99, 198)), null)
                .row(3, null, null)
                .pageBreak()
                .row(
                        6,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(7, 14), ImmutableList.of(8, 16)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(9, 18), ImmutableList.of(10, 20), ImmutableList.of(11, 22), ImmutableList.of(12, 24))))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, new ArrayType(BIGINT), new ArrayType(BIGINT), new ArrayType(BIGINT))
                .row(1, ImmutableList.of(2L, 4L), ImmutableList.of(4L, 8L), ImmutableList.of(5L, 10L))
                .row(1, ImmutableList.of(3L, 6L), null, null)
                .row(2, ImmutableList.of(99L, 198L), null, null)
                .row(6, ImmutableList.of(7L, 14L), ImmutableList.of(9L, 18L), ImmutableList.of(10L, 20L))
                .row(6, ImmutableList.of(8L, 16L), ImmutableList.of(11L, 22L), ImmutableList.of(12L, 24L))
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testUnnestWithOrdinality()
            throws Exception
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<bigint>"));
        Type mapType = metadata.getType(parseTypeSignature("map<bigint,bigint>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2, arrayBlockOf(BIGINT, 99), null)
                .row(3, null, null)
                .pageBreak()
                .row(6, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), true);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1, 2, 4, 5, 1)
                .row(1, 3, null, null, 2)
                .row(2, 99, null, null, 1)
                .row(6, 7, 9, 10, 1)
                .row(6, 8, 11, 12, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testUnnestNonNumericDoubles()
            throws Exception
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<double>"));
        Type mapType = metadata.getType(parseTypeSignature("map<bigint,double>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1, arrayBlockOf(DOUBLE, NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN),
                        mapBlockOf(BIGINT, DOUBLE, ImmutableMap.of(1, NEGATIVE_INFINITY, 2, POSITIVE_INFINITY, 3, NaN)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, DOUBLE)
                .row(1, NEGATIVE_INFINITY, 1, NEGATIVE_INFINITY)
                .row(1, POSITIVE_INFINITY, 2, POSITIVE_INFINITY)
                .row(1, NaN, 3, NaN)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
