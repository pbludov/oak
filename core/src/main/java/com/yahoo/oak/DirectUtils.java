/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Support read/write API to direct memory address. It uses {@link ByteBuffer#allocateDirect} API.
 */
public final class DirectUtils {

    private static final Logger LOGGER = LogManager.getLogger(DirectUtils.class);
    private static final int CHUNK_SIZE = 4096;

    private static final int MAX_BUFFER_SIZE = 128 * 1024;
    private static final List<ByteBuffer> BUFFERS = new ArrayList<>();
    private static final Map<Long, Long> REGIONS = new HashMap<>();

    static final long LONG_INT_MASK = (1L << Integer.SIZE) - 1L;

    static {
        // Just to make sure our virtual address space does not start with Zero.
        BUFFERS.add(null);
    }

    private DirectUtils() {
    }

    /**
     * Allocates memory. The returned address should be freed explicitly using {@link #freeMemory(long)}.
     * All addresses are unique and never come again even after releasing memory.
     *
     * @param capacity the memory capacity to allocate
     * @return an address to the newly allocated memory
     */
    public static synchronized long allocateMemory(long capacity) {
        LOGGER.trace("Allocating buffer of {} bytes", capacity);
        long address = (long) MAX_BUFFER_SIZE * BUFFERS.size();
        var numBuffers = capacity / MAX_BUFFER_SIZE;
        var tail = capacity % MAX_BUFFER_SIZE;

        // Temporary list that will be freed if the OOM error occurs.
        List<ByteBuffer> newBuffers = new ArrayList<>();
        for (int i = 0; i < numBuffers; ++i) {
            newBuffers.add(ByteBuffer.allocateDirect(MAX_BUFFER_SIZE).order(ByteOrder.nativeOrder()));
        }
        if (tail > 0L) {
            newBuffers.add(ByteBuffer.allocateDirect((int) tail).order(ByteOrder.nativeOrder()));
        }

        BUFFERS.addAll(newBuffers);
        REGIONS.put(address, capacity);
        LOGGER.info("Allocated buffer with size of {} bytes as {}", capacity, address);
        return address;
    }

    /**
     * Releases memory that was allocated via {@link com.yahoo.oak.DirectUtils#allocateMemory(long)}.
     *
     * @param address the memory address to release
     */
    public static synchronized void freeMemory(long address) {
        LOGGER.trace("Freeing memory at address {}", address);

        Long size = REGIONS.get(address);
        if (size == null) {
            // already freed
            LOGGER.info("Buffer at address {} not allocated or already freed", address);
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
        LOGGER.info("Buffer with size of {} bytes at address {} freed", size, address);
    }

    /**
     * Initializes a memory block.
     *
     * @param address the memory address of the block
     * @param bytes the number of bytes to write
     * @param value the value to write
     */
    public static void setMemory(long address, long bytes, byte value) {
        LOGGER.trace("Memset at {} for {} bytes to {}", address, bytes, value);
        byte[] local = new byte[CHUNK_SIZE];
        Arrays.fill(local, value);
        long pending = bytes;
        long writingAddress = address;
        while (pending > CHUNK_SIZE) {
            copyFromArray(local, writingAddress, CHUNK_SIZE);
            pending -= CHUNK_SIZE;
            writingAddress += CHUNK_SIZE;
        }

        if (pending > 0) {
            copyFromArray(local, writingAddress, (int) pending);
        }
    }

    /**
     * Copies a memory block.
     *
     * @param srcAddress the memory address of the source block
     * @param dstAddress the memory address of the destination block
     * @param size the number of bytes to write
     */
    public static void copyMemory(long srcAddress, long dstAddress, int size) {
        LOGGER.trace("Copying {} bytes from {} to {}", size, srcAddress, dstAddress);
        byte[] local = new byte[CHUNK_SIZE];
        long pending = size;
        long readingAddress = srcAddress;
        long writingAddress = dstAddress;
        while (pending > CHUNK_SIZE) {
            copyToArray(readingAddress, local, CHUNK_SIZE);
            copyFromArray(local, writingAddress, CHUNK_SIZE);
            pending -= CHUNK_SIZE;
            readingAddress += CHUNK_SIZE;
            writingAddress += CHUNK_SIZE;
        }

        if (pending > 0) {
            copyToArray(readingAddress, local, (int) pending);
            copyFromArray(local, writingAddress, (int) pending);
        }
    }

    /**
     * Reads a byte value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static byte get(long address) {
        return getByteBufferForAddress(address).get(getOffsetInBuffer(address));
    }

    /**
     * Reads a char value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static char getChar(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Character.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getChar(offset);
        }
        return getBytes(address, Character.BYTES).getChar();
    }

    /**
     * Reads a short value from a given address.
     * @param address the source memory address
     * @return the read value
     */
    public static short getShort(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Short.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getShort(offset);
        }
        return getBytes(address, Short.BYTES).getShort();
    }

    /**
     * Reads an int value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static int getInt(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Integer.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getInt(offset);
        }
        return getBytes(address, Integer.BYTES).getInt();
    }

    /**
     * Reads a long value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static long getLong(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Long.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getLong(offset);
        }
        return getBytes(address, Long.BYTES).getLong();
    }

    /**
     * Reads a float value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static float getFloat(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Float.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getFloat(offset);
        }
        return getBytes(address, Float.BYTES).getFloat();
    }

    /**
     * Reads a double value from a given address.
     *
     * @param address the source memory address
     * @return the read value
     */
    public static double getDouble(long address) {
        int offset = getOffsetInBuffer(address);
        if (offset + Double.BYTES <= MAX_BUFFER_SIZE) {
            return getByteBufferForAddress(address).getDouble(offset);
        }
        return getBytes(address, Double.BYTES).getDouble();
    }

    /**
     * Puts a byte value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void put(long address, byte value) {
        getByteBufferForAddress(address).put(getOffsetInBuffer(address), value);
    }

    /**
     * Puts a char value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putChar(long address, char value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Character.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putChar(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES).order(ByteOrder.nativeOrder()).putChar(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a short value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putShort(long address, short value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Short.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putShort(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.nativeOrder()).putShort(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts an int value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putInt(long address, int value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Integer.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putInt(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder()).putInt(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a long value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putLong(long address, long value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Long.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putLong(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder()).putLong(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a float value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putFloat(long address, float value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Float.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putFloat(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.nativeOrder()).putFloat(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Puts a double value in a given address.
     *
     * @param address the target memory address
     * @param value the value to write
     */
    public static void putDouble(long address, double value) {
        int offset = getOffsetInBuffer(address);
        if (offset + Double.BYTES <= MAX_BUFFER_SIZE) {
            getByteBufferForAddress(address).putDouble(offset, value);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.nativeOrder()).putDouble(value);
            putBytes(address, buffer);
        }
    }

    /**
     * Copies data from a direct memory address to a byte array.
     *
     * @param address the source memory address
     * @param array the target array
     * @param size the number of bytes to copy
     * @return the number of bytes that were copied
     */
    public static long copyToArray(long address, byte[] array, int size) {
        int bufferIndex = getBufferIndex(address);
        var buffer = ensureBufferAllocated(bufferIndex);
        var readingAddress = getOffsetInBuffer(address);
        if (size + readingAddress <= MAX_BUFFER_SIZE) {
            buffer.get(readingAddress, array, 0, size);
        } else {
            // Read head
            int remaining = size;
            int read = MAX_BUFFER_SIZE - readingAddress;
            buffer.get(readingAddress, array, 0, read);
            remaining -= read;
            // Read whole buffers
            while (remaining > MAX_BUFFER_SIZE) {
                buffer = ensureBufferAllocated(++bufferIndex);
                buffer.get(0, array, read, MAX_BUFFER_SIZE);
                read += MAX_BUFFER_SIZE;
                remaining -= MAX_BUFFER_SIZE;
            }
            // Read tail
            if (remaining > 0) {
                buffer = ensureBufferAllocated(++bufferIndex);
                buffer.get(0, array, read, remaining);
            }
        }
        return size;
    }

    /**
     * Copies byte array to a direct memory block byte wise.
     *
     * @param array the source int array
     * @param address the target memory address
     * @param size the number of bytes to copy
     * @return the number of bytes that were copied
     */
    public static long copyFromArray(byte[] array, long address, int size) {
        int bufferIndex = getBufferIndex(address);
        var buffer = ensureBufferAllocated(bufferIndex);
        var writingAddress = getOffsetInBuffer(address);
        if (size + writingAddress <= MAX_BUFFER_SIZE) {
            buffer.put(writingAddress, array, 0, size);
        } else {
            // Write head
            int remaining = size;
            int written = MAX_BUFFER_SIZE - writingAddress;
            buffer.put(writingAddress, array, 0, written);
            remaining -= written;
            // Write whole buffers
            while (remaining > MAX_BUFFER_SIZE) {
                buffer = ensureBufferAllocated(++bufferIndex);
                buffer.put(0, array, written, MAX_BUFFER_SIZE);
                written += MAX_BUFFER_SIZE;
                remaining -= MAX_BUFFER_SIZE;
            }
            // Write tail
            if (remaining > 0) {
                buffer = ensureBufferAllocated(++bufferIndex);
                buffer.put(0, array, written, remaining);
            }
        }
        return size;
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

    /* ============== */
    /* Helper methods */
    /* ============== */

    private static int getBufferIndex(long address) {
        return (int) (address / MAX_BUFFER_SIZE);
    }

    private static int getOffsetInBuffer(long address) {
        return (int) (address % MAX_BUFFER_SIZE);
    }

    private static ByteBuffer ensureBufferAllocated(int bufferIndex) {
        if (bufferIndex < BUFFERS.size()) {
            ByteBuffer buffer = BUFFERS.get(bufferIndex);
            if (buffer != null) {
                return buffer;
            }
        }
        long address = (long) bufferIndex * MAX_BUFFER_SIZE;
        throw new ArrayIndexOutOfBoundsException("Address " + address + " is freed or not allocated");
    }

    private static ByteBuffer getByteBufferForAddress(long address) {
        return ensureBufferAllocated(getBufferIndex(address));
    }

    private static ByteBuffer getBytes(long address, int length) {
        byte[] bytes = new byte[length];
        copyToArray(address, bytes, length);
        return ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
    }

    private static void putBytes(long address, ByteBuffer buffer) {
        copyFromArray(buffer.array(), address, buffer.limit());
    }

}
