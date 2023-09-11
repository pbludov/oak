/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OakUnsafeDirectBufferTest {

    private MemoryManager memoryManager;

    @Before
    public void setUp() throws Exception {
        int size = 8 * 1024 * 1024;
        long capacity = size * 3;
        BlocksPool.setBlockSize(size);
        memoryManager = new SyncRecycleMemoryManager(new NativeMemoryAllocator(capacity));
    }

    @After
    public void tearDown() throws Exception {
        memoryManager.close();
        BlocksPool.setBlockSize(BlocksPool.DEFAULT_BLOCK_SIZE_BYTES);
    }

    @Test
    public void byteBufferFromKeyBufferShouldBeReadOnly() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        Assert.assertTrue(new KeyBuffer(s).isAssociated());
    }

    @Test
    public void byteBufferFromValueBufferShouldBeReadOnly() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        Assert.assertTrue(new ValueBuffer(s).isAssociated());
    }

    @Test
    public void byteBufferFromScopedReadBufferShouldBeReadOnly() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        Assert.assertTrue(new ScopedReadBuffer(s).isAssociated());
    }

    @Test
    public void byteBufferFromScopedWriteBufferShouldBeWritable() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        ScopedWriteBuffer.compute(s, buf -> Assert.assertEquals(100,
                ((OakUnsafeDirectBuffer) buf).getLength()));
    }

    @Test
    public void byteBufferFromScopedBufferShouldBeReadOnlyWhenWrappingReadOnlyBuffer() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        ScopedReadBuffer readBuffer = new ScopedReadBuffer(s);
        Assert.assertEquals(new UnscopedBuffer<>(readBuffer).getLength(), 100);
    }

    @Test
    public void byteBufferFromScopedBufferShouldBeWritableWhenWrappingWritableBuffer() {
        Slice s = memoryManager.getEmptySlice();
        s.allocate(100, false);
        ScopedWriteBuffer.compute(s, buf -> Assert.assertEquals(100,
                new UnscopedBuffer<>((ScopedWriteBuffer) buf).getLength()));
    }

    @Test
    public void byteBufferFromUnscopedValueBufferSyncedShouldBeReadOnly() {
        Slice keySlice = memoryManager.getEmptySlice();
        keySlice.allocate(100, false);
        Slice valueSlice = memoryManager.getEmptySlice();
        valueSlice.allocate(100, false);

        KeyBuffer keyBuffer = new KeyBuffer(keySlice);
        ValueBuffer valueBuffer = new ValueBuffer(valueSlice);
        Assert.assertEquals(100,
                new UnscopedValueBufferSynced(keyBuffer, valueBuffer, null).getLength());
    }
}
