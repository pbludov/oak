/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.Assert;
import org.junit.Test;

public class DirectUtilsTest {
    static final int[] TEST_NUMBERS = new int[]{0, 1, 2, Integer.MAX_VALUE, -1, -2, Integer.MIN_VALUE};

    int[] longToInts(long l) {
        return new int[]{
                (int) (l & DirectUtils.LONG_INT_MASK),
                (int) ((l >>> Integer.SIZE) & DirectUtils.LONG_INT_MASK)
        };
    }

    public void singleIntLongTest(int i1, int i2) {
        long combine = DirectUtils.intsToLong(i1, i2);
        int[] res = longToInts(combine);
        Assert.assertEquals(i1, res[0]);
        Assert.assertEquals(i2, res[1]);
    }

    @Test
    public void testIntsToLong() {
        for (int i1 : TEST_NUMBERS) {
            for (int i2 : TEST_NUMBERS) {
                singleIntLongTest(i1, i2);
            }
        }
    }

    @Test
    public void testCopyToArray() {
        final int sz = 10;
        final long allocSize = sz;
        final byte[] expected = new byte[sz];

        long address = DirectUtils.allocateMemory(128 * BlocksPool.MB + sz);

        try {
            long start = address + 128 * BlocksPool.MB - 2;
            for (byte i = 0; i < sz; i++) {
                expected[i] = i;
                DirectUtils.put(start + i, i);
            }

            final byte[] result = new byte[sz];
            long copySize = DirectUtils.copyToArray(start, result, sz);
            Assert.assertEquals(allocSize, copySize);
            Assert.assertArrayEquals(expected, result);
        } finally {
            DirectUtils.freeMemory(address);
        }
    }

    @Test
    public void testLongOnBufferEdge() {
        final int sz = Long.BYTES;

        long address = DirectUtils.allocateMemory(128 * BlocksPool.MB + sz);

        try {
            long start = address + 128 * BlocksPool.MB - 2;
            DirectUtils.putLong(start, 0x12345678L);
            long value = DirectUtils.getLong(start);

            Assert.assertEquals(0x12345678L, value);
        } finally {
            DirectUtils.freeMemory(address);
        }
    }

    @Test
    public void testCopyFromArray() {
        final int sz = 10;
        final long allocSize = sz;
        final byte[] expected = new byte[sz];

        for (byte i = 0; i < sz; i++) {
            expected[i] = i;
        }

        long address = DirectUtils.allocateMemory(sz);
        try {
            long copySize = DirectUtils.copyFromArray(expected, address, sz);
            Assert.assertEquals(allocSize, copySize);

            for (int i = 0; i < sz; i++) {
                Assert.assertEquals(expected[i], DirectUtils.get(address + i));
            }
        } finally {
            DirectUtils.freeMemory(address);
        }
    }
}
