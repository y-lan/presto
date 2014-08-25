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
package com.facebook.presto.operator.scalar;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class BenchmarkJsonFunctions
{
    private FunctionAssertions functionAssertions;
    public static final int TEST_TIMES = 100000;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testJsonSize()
    {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < TEST_TIMES; i++) {
            assertFunction(format("JSON_EXTRACT('%s', '%s')",
                    "{\"awakening\":\"0\",\"request_id\":\"20140822001849-421875-0011511697\"," +
                            "\"device\":\"android\",\"fr\":\"board\",\"api\":\"request\",\"app_id\":\"96\"," +
                            "\"env\":\"spnative\",\"act\":\"imp\"}",
                    "$.env"
            ), "\"spnative\"");
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("json_extract totalTime:" + totalTime);

        startTime = System.currentTimeMillis();
        for (int i = 0; i < TEST_TIMES; i++) {
            assertFunction(format("JSON_GET('%s', '%s')",
                    "{\"awakening\":\"0\",\"request_id\":\"20140822001849-421875-0011511697\"," +
                            "\"device\":\"android\",\"fr\":\"board\",\"api\":\"request\",\"app_id\":\"96\"," +
                            "\"env\":\"spnative\",\"act\":\"imp\"}",
                    "env"
            ), "spnative");
        }
        endTime = System.currentTimeMillis();
        long totalTime2 = endTime - startTime;
        System.out.println("json_get totalTime:" + totalTime2);
        System.out.println("comparison:" + 100.0 * totalTime2 / totalTime);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }
}
