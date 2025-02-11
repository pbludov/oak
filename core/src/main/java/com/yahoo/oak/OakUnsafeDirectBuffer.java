/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * This interface allows high performance access to the underlying data of Oak.
 * To achieve that, it sacrifices safety, so it should be used only if you know what you are doing.
 * Misuse of this interface might result in corrupted data, a crash or a deadlock.
 * <p>
 * Specifically, the developer should be concerned by two issues:
 * 1. Concurrency: using this interface inside the context of serialize, compute, compare and transform is thread safe.
 *    In other contexts (e.g., get output), the developer should ensure that there is no concurrent access to this
 *    data. Failing to ensure that might result in corrupted data.
 * 2. Data boundaries: when using this interface, Oak will not alert the developer regarding any out of boundary access.
 *    Thus, the developer should use getOffset() and getLength() to obtain the data boundaries and carefully access
 *    the data. Writing data out of these boundaries might result in corrupted data, a crash or a deadlock.
 * <p>
 * To use this interface, the developer should cast Oak's buffer (OakScopedReadBuffer or OakScopedWriteBuffer) to this
 * interface, similarly to how Java's internal DirectBuffer is used. For example:
 * <pre>
 * {@code
 * int foo(OakScopedReadBuffer b) {
 *     OakUnsafeDirectBuffer ub = (OakUnsafeDirectBuffer) b;
 *     ByteBuffer bb = ub.getByteBuffer();
 *     return bb.getInt(ub.getOffset());
 * }
 * }
 * </pre>
 */
public interface OakUnsafeDirectBuffer {

    /**
     * @return the data length.
     */
    int getLength();

    /**
     * Allows access to the memory address of the OakUnsafeDirectBuffer of Oak.
     * The address will point to the beginning of the user data, but avoiding overflow is the developer responsibility.
     * Thus, the developer should use getLength() and access data only in this boundary.
     *
     * @return the exact memory address of the Buffer in the position of the data.
     */
    long getAddress();

}
