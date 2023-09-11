/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Support read/write API to direct memory address.
 * It uses {@link sun.misc.Unsafe} API (Java 8).
 * As long as Oak is compiled for Java 8, this API should work as intended even if Oak is used by projects that
 * compiled in newer Java versions.
 */
public final class DirectUtils {
    static int MAX_BUFFER_SIZE = 1024 * 1024 * 1024;
    static List<ByteBuffer> BUFFERS = new ArrayList<>();
    static Map<Long, Long> REGIONS = new HashMap<>();

    static final long LONG_INT_MASK = (1L << Integer.SIZE) - 1L;

    static {
        // Just to make sure our virtual address space does not start with Zero.
        BUFFERS.add(null);
    }
    private DirectUtils() {
    }

    /**
     * Allocate memory. The returned address should be freed explicitly using {@link #freeMemory(long)}.
     * @param capacity the memory capacity to allocate
     * @return an address to the newly allocated memory.
     */
    public static synchronized long allocateMemory(long capacity) {
        long address = (long) MAX_BUFFER_SIZE * BUFFERS.size();
        var numBuffers = capacity / MAX_BUFFER_SIZE;
        var tail = capacity % MAX_BUFFER_SIZE;

        // Temporary list that will be freed if the OOM error occurs.
        List<ByteBuffer> newBuffers = new ArrayList<>();
        for (int i = 0; i < numBuffers; ++i) {
            newBuffers.add(ByteBuffer.allocateDirect(MAX_BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN));
        }
        if (tail > 0L) {
            newBuffers.add(ByteBuffer.allocateDirect((int) tail).order(ByteOrder.LITTLE_ENDIAN));
        }

        BUFFERS.addAll(newBuffers);
        REGIONS.put(address, capacity);
        return address;
    }

    /**
     * Releases memory that was allocated via {@link com.yahoo.oak.DirectUtils#allocateMemory(long)}.
     * @param address the memory address to release.
     */
    public static synchronized void freeMemory(long address) {
        Long size = REGIONS.get(address);
        if (size == null) {
            // already freed
            return;
        }
        var numBuffers = size / MAX_BUFFER_SIZE;
        if (size % MAX_BUFFER_SIZE > 0) {
            numBuffers++;
        }

        int bufferIndex = getBufferIndex(address);
        for (int i = 0; i < numBuffers; ++i) {
            // Never remove items. Instead, set buffer to null to avoid re-using address space.
            BUFFERS.set(bufferIndex + i, null);
        }
        REGIONS.remove(address);
    }

    /**
     * Initialize a memory block.
     * @param address the memory address of the block
     * @param bytes the number of bytes to write
     * @param value the value to write
     */
    public static void setMemory(long address, long bytes, byte value) {
        int bufferIndex = getBufferIndex(address);
        int start = getStartInBuffer(address);
        ByteBuffer buffer = ensureBufferAllocated(bufferIndex);
        for (int i = 0; i < bytes; i++) {
            buffer.put(start, value);
            if (++start == MAX_BUFFER_SIZE) {
                start = 0;
                buffer = ensureBufferAllocated(++bufferIndex);
            }
        }
    }

    /**
     * Read a byte value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static byte get(long address) {
        return getByteBufferForAddress(address).get(getStartInBuffer(address));
    }

    /**
     * Read a char value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static char getChar(long address) {
        int start = getStartInBuffer(address);
        if (start + Character.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getChar(start);
        }
        return getBytes(address, Character.BYTES).getChar();
    }

    /**
     * Read a short value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static short getShort(long address) {
        int start = getStartInBuffer(address);
        if (start + Short.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getShort(start);
        }
        return getBytes(address, Short.BYTES).getShort();
    }

    /**
     * Read an int value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static int getInt(long address) {
        int start = getStartInBuffer(address);
        if (start + Integer.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getInt(start);
        }
        return getBytes(address, Integer.BYTES).getInt();
    }

    /**
     * Read a long value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static long getLong(long address) {
        int start = getStartInBuffer(address);
        if (start + Long.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getLong(start);
        }
        return getBytes(address, Long.BYTES).getLong();
    }

    /**
     * Read a float value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static float getFloat(long address) {
        int start = getStartInBuffer(address);
        if (start + Float.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getFloat(start);
        }
        return getBytes(address, Float.BYTES).getFloat();
    }

    /**
     * Read a double value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static double getDouble(long address) {
        int start = getStartInBuffer(address);
        if (start + Double.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getDouble(start);
        }
        return getBytes(address, Double.BYTES).getDouble();
    }

    /**
     * Puts a byte value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void put(long address, byte value) {
        getByteBufferForAddress(address).put(getStartInBuffer(address), value);
    }

    /**
     * Puts a char value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putChar(long address, char value) {
        int start = getStartInBuffer(address);
        if (start + Character.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putChar(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES).putChar(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a short value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putShort(long address, short value) {
        int start = getStartInBuffer(address);
        if (start + Short.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putShort(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).putShort(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts an int value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putInt(long address, int value) {
        int start = getStartInBuffer(address);
        if (start + Integer.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putInt(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a long value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putLong(long address, long value) {
        int start = getStartInBuffer(address);
        if (start + Long.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putLong(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a float value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putFloat(long address, float value) {
        int start = getStartInBuffer(address);
        if (start + Float.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putFloat(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES).putFloat(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a double value in a given address.
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putDouble(long address, double value) {
        int start = getStartInBuffer(address);
        if (start + Double.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putDouble(start, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES).putDouble(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Copy data from a direct memory address to a int array.
     * @param address the source memory address
     * @param array the target array
     * @param size the number of integers to copy
     * @return the number of bytes that were copied
     */
    public static long copyToArray(long address, int[] array, int size) {
        long offset = address;
        for (int i = 0; i < size; ++i) {
            array[i] = getInt(offset);
            offset += Integer.BYTES;
        }

        return (long) size * Integer.BYTES;
    }

    /**
     * Copies int array to a direct memory block byte wise.
     * @param array the source int array
     * @param address the target memory address
     * @param size the number of integers to copy
     * @return the number of bytes that were copied
     */
    public static long copyFromArray(int[] array, long address, int size) {
        long offset = address;
        for (int i = 0; i < size; ++i) {
            putInt(offset, array[i]);
            offset += Integer.BYTES;
        }

        return (long) size * Integer.BYTES;
    }

    /**
     * Combines two integers into one long where the first argument is placed in the lower four bytes.
     * Each integer is written in its native endianness.
     * Uses OR so the sign of the integers should not matter.
     * @param i1 the number stored in the lower part of the output
     * @param i2 the number stored in the upper part of the output
     * @return combination of the two integers as long
     */
    public static long intsToLong(int i1, int i2) {
        return (i1 & LONG_INT_MASK) | (((long) i2) << Integer.SIZE);
    }

    private static int getBufferIndex(long address) {
        return (int) (address / MAX_BUFFER_SIZE);
    }

    private static int getStartInBuffer(long address) {
        return (int) (address % MAX_BUFFER_SIZE);
    }

    private static ByteBuffer ensureBufferAllocated(int bufferIndex) {
        final ByteBuffer buffer = BUFFERS.get(bufferIndex);
        if (buffer == null) {
            long address = (long) bufferIndex * MAX_BUFFER_SIZE;
            throw new ArrayIndexOutOfBoundsException("Address " + address + " is freed or not allocated");
        }
        return buffer;
    }

    private static ByteBuffer getByteBufferForAddress(long address) {
        return ensureBufferAllocated(getBufferIndex(address));
    }

    public static void copyMemory(long srcAddress, long dstAddress, int size) {
        byte[] local = new byte[size];
        int srcBufferIndex = getBufferIndex(srcAddress);
        var srcBuffer = ensureBufferAllocated(srcBufferIndex);
        var srcStart = getStartInBuffer(srcAddress);
        if (size + srcStart <= MAX_BUFFER_SIZE) {
            srcBuffer.get(srcStart, local);
        } else {

            // Read head
            int remaining = size;
            int read = MAX_BUFFER_SIZE - srcStart;
            srcBuffer.get(srcStart, local, 0, read);
            remaining -= read;

            // Read whole buffers
            while (remaining > MAX_BUFFER_SIZE) {
                srcBuffer = ensureBufferAllocated(++srcBufferIndex);
                srcBuffer.get(0, local, read, MAX_BUFFER_SIZE);
                read += MAX_BUFFER_SIZE;
                remaining -= MAX_BUFFER_SIZE;
            }

            // Read tail
            if (remaining > 0) {
                srcBuffer = ensureBufferAllocated(++srcBufferIndex);
                srcBuffer.get(0, local, read, remaining);
            }
        }

        int dstBufferIndex = getBufferIndex(dstAddress);
        var dstBuffer = ensureBufferAllocated(dstBufferIndex);
        var dstStart = getStartInBuffer(dstAddress);
        if (size + dstStart <= MAX_BUFFER_SIZE) {
            dstBuffer.put(dstStart, local);
        } else {
            // Write head
            int remaining = size;
            int written = MAX_BUFFER_SIZE - dstStart;
            dstBuffer.put(dstStart, local, 0, written);
            remaining -= written;

            // Write whole buffers
            while (remaining > MAX_BUFFER_SIZE) {
                dstBuffer = ensureBufferAllocated(++dstBufferIndex);
                dstBuffer.put(0, local, written, MAX_BUFFER_SIZE);
                written += MAX_BUFFER_SIZE;
                remaining -= MAX_BUFFER_SIZE;
            }

            // Write tail
            if (remaining > 0) {
                dstBuffer = ensureBufferAllocated(++dstBufferIndex);
                dstBuffer.put(0, local, written, remaining);
            }
        }
    }

    /**
     * Unoptimized version that reads bytes one-by one for reading data split into multiple buffers.
     *
     * @param address starting address
     * @param length the length to read
     * @return on-heap ByteBuffer
     */
    private static ByteBuffer getBytes(
            long address,
            int length
    ) {
        byte[] bytes = new byte[length];
        long offset = address;
        for (int i = 0; i < length; ++i) {
            bytes[i] = get(offset++);
        }
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Unoptimized version that writes bytes one-by one for writing data split into multiple buffers.
     *
     * @param address starting address
     * @param buffer the on-heap ByteBuffer to write
     */
    private static void putBytes(
            long address,
            ByteBuffer buffer
    ) {
        for (int i = 0; i < buffer.limit(); ++i) {
            put(address + i, buffer.get(i));
        }
    }

    /**
     * Ensures that loads and stores before the fence will not be reordered with
     * stores after the fence; a "StoreStore plus LoadStore barrier".
     */
    public static void storeFence() {
        VarHandle.storeStoreFence();
    }

    /**
     * Ensures that loads before the fence will not be reordered with loads and
     * stores after the fence; a "LoadLoad plus LoadStore barrier".
     */
    public static void loadFence() {
        VarHandle.loadLoadFence();
    }

    /**
     * Ensures that loads and stores before the fence will not be reordered
     * with loads and stores after the fence.  Implies the effects of both
     * loadFence() and storeFence(), and in addition, the effect of a StoreLoad
     * barrier.
     */
    public static void fullFence() {
        VarHandle.fullFence();
    }

}
