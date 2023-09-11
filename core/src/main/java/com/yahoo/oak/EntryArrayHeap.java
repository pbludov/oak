/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Stores the entry array as Java long array (on-heap).
 */
public class EntryArrayHeap implements EntryArrayInternal {

    // The array is initialized to 0 - this is important!
    private final AtomicLongArray entries;

    // Number of primitive fields in each entry
    private final int fieldCount;

    // number of entries to be maximally held
    private final int entryCount;

    /**
     * @param entryCount how many entries should this instance keep at maximum
     * @param fieldCount the number of fields (64bit) in each entry
     */
    EntryArrayHeap(int entryCount, int fieldCount) {
        this.fieldCount = fieldCount;
        this.entryCount = entryCount;
        this.entries = new AtomicLongArray(entryCount * fieldCount);
    }

    /**
     * Converts external entry-index and field-index to the field's direct memory address.
     * <p>
     * @param entryIndex the index of the entry
     * @param fieldIndex the field (long) index inside the entry
     * @return the field's offset in the array
     */
    private int entryOffset(int entryIndex, int fieldIndex) {
        return entryIndex * fieldCount + fieldIndex;
    }

    /** {@inheritDoc} */
    @Override
    public int entryCount() {
        return entryCount;
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return entryCount;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        for (int i = 0; i < entries.length(); i++) {
            entries.lazySet(i, 0L);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getEntryFieldLong(int entryIndex, int fieldIndex) {
        return entries.get(entryOffset(entryIndex, fieldIndex));
    }

    /** {@inheritDoc} */
    @Override
    public void setEntryFieldLong(int entryIndex, int fieldIndex, long value) {
        entries.set(entryOffset(entryIndex, fieldIndex), value);
    }

    /** {@inheritDoc} */
    @Override
    public boolean casEntryFieldLong(int entryIndex, int fieldIndex, long expectedValue, long newValue) {
        return entries.compareAndSet(entryOffset(entryIndex, fieldIndex), expectedValue, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyEntryFrom(EntryArrayInternal other, int srcEntryIndex, int destEntryIndex, int fieldCount) {
        assert fieldCount <= this.fieldCount;
        EntryArrayHeap o = (EntryArrayHeap) other;
        int srcOffset = o.entryOffset(srcEntryIndex, 0);
        int dstOffset = entryOffset(destEntryIndex, 0);
        for (int i = 0; i < fieldCount; i++) {
            entries.set(dstOffset++, o.entries.get(srcOffset++));
        }
    }
}
