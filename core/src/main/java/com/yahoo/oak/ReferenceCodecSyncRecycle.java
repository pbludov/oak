/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

/**
 * A reference that is involved in synchronized and recycling memory management is composed of
 * 3 parameters:block ID, offset and version.
 *
 * 0...            ...41 | 42... ...63
 * block ID   |  offset  | version
 * first         second      third
 *
 * All these parameters may be squashed together into one long for easy representation.
 * Using different number of bits for each parameter may incur different limitations on their sizes.
 */
class ReferenceCodecSyncRecycle extends ReferenceCodec {

    static final int INVALID_VERSION = 0;

    // All Oak instances on one machine are expected to reference to no more than 4TB of RAM.
    // 4TB = 2^42 bytes
    // blockIDBitSize + offsetBitSize = BITS_FOR_MAXIMUM_RAM
    // The number of bits required to represent such memory:
    private static final int BITS_FOR_MAXIMUM_RAM = 42;
    private long versionDeleteBitMASK = (1 << (Long.SIZE - BITS_FOR_MAXIMUM_RAM - 1));
    private long referenceDeleteBitMASK
           = (INVALID_REFERENCE | (versionDeleteBitMASK << BITS_FOR_MAXIMUM_RAM));

    // number of allowed bits for version (-1 for delete bit) set to one
    static final int LAST_VALID_VERSION = (int) mask(Long.SIZE - BITS_FOR_MAXIMUM_RAM - 1);

    /**
     * Initialize the codec with offset in the size of block.
     * This will inflict a limit on the maximal number of blocks - size of block ID.
     * The remaining bits out of maximum RAM (BITS_FOR_MAXIMUM_RAM)
     * @param blockSize an upper limit on the size of a block (exclusive)
     * @param allocator
     *
     */
    ReferenceCodecSyncRecycle(long blockSize, BlockMemoryAllocator allocator) {
        super(BITS_FOR_MAXIMUM_RAM - ReferenceCodec.requiredBits(blockSize),
            ReferenceCodec.requiredBits(blockSize), AUTO_CALCULATE_BIT_SIZE);
        // and the rest goes for version (currently 22 bits)
    }
    
    ReferenceCodecSyncRecycle(long blockSize, BlockMemoryAllocator allocator, long versionDeleteBitMASK) {
        super(BITS_FOR_MAXIMUM_RAM - ReferenceCodec.requiredBits(blockSize),
            ReferenceCodec.requiredBits(blockSize), AUTO_CALCULATE_BIT_SIZE);
        // and the rest goes for version (currently 22 bits)
        this.versionDeleteBitMASK = versionDeleteBitMASK;
        this.referenceDeleteBitMASK
               = (INVALID_REFERENCE | (versionDeleteBitMASK << BITS_FOR_MAXIMUM_RAM));
    }

    @Override
    protected long getFirstForDelete(long reference) {
        return INVALID_REFERENCE;
    }

    @Override
    protected long getSecondForDelete(long reference) {
        return INVALID_REFERENCE;
    }

    @Override
    protected long getThirdForDelete(long reference) {
        long v = getThird(reference);
        // The set the MSB (the left-most bit out of 22 is delete bit)
        v |= versionDeleteBitMASK;
        return (INVALID_REFERENCE | v);
    }

    // invoked only within assert
    boolean isReferenceConsistent(long reference) {
        if (reference == INVALID_REFERENCE) {
            return true;
        }
        if (isReferenceDeleted(reference)) {
            return true;
        }
        int v = getThird(reference);
        return (v != INVALID_VERSION);
    }

    @Override
    boolean isReferenceDeleted(long reference) {
        return ((reference & referenceDeleteBitMASK) != INVALID_REFERENCE);
    }

    boolean isReferenceValidAndNotDeleted(long reference) {
        return (reference != INVALID_REFERENCE &&
            (reference & referenceDeleteBitMASK) == INVALID_REFERENCE);
    }
}
