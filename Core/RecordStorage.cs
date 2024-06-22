using System;
using System.Collections.Generic;
using System.IO;

namespace database_from_scratch.Core
{
    /// <summary>
    /// Record storage service that store data in form of records,
    /// each record ip from one or several blocks
    /// </summary>
    public class RecordStorage: IRecordStorage
    {
        private readonly IBlockStorage _storage;

        private const int _maxRecordSize = 4194304;
        private const int _nextBlockId = 0;
        private const int _recordLength = 1;
        private const int _blockContentLength = 2;
        private const int _prevoiusBlockId = 3;
        private const int _isDeleted = 4;
        
        // Constructor
        public RecordStorage(IBlockStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException();

            if (_storage.BlockHeaderSize < 48)
            {
                throw new ArgumentException("Record storage needs at least 48 header bytes");
            }
        }
        
        // Public methods
        
        public virtual void Update(uint recordId, byte[] data)
        {
            var written = 0;
            var total = data.Length;
            var blocks = GetBlocks(recordId);
            var blocksUsed = 0;
            var previousBlock = (IBlock)null;

            try
            {
                // Start writing block by block
                while (written < total)
                {
                    // Bytes to be written in this block
                    var bytesToWrite = Math.Min(total - written, _storage.BlockContentSize);

                    // Get the block where the first byte of remaining data will be written to
                    var blockIndex = (int)Math.Floor((double)written / _storage.BlockContentSize);

                    // Find the block to write to:
                    // If blockIndex exists in blocks, then write into it,
                    // otherwise allocate a new one for writing
                    var target = (IBlock)null;
                    if (blockIndex < blocks.Count)
                    {
                        target = blocks[blockIndex];
                    }
                    else
                    {
                        target = AllocateBlock();
                        if (target == null)
                        {
                            throw new Exception("Failed to allocate new block");
                        }

                        blocks.Add(target);
                    }

                    // Link with previous block
                    if (previousBlock != null)
                    {
                        previousBlock.SetHeader(_nextBlockId, target.Id);
                        target.SetHeader(_prevoiusBlockId, previousBlock.Id);
                    }

                    // Write data
                    target.Write(src: data, srcOffset: written, dstOffset: 0, count: bytesToWrite);
                    target.SetHeader(_blockContentLength, bytesToWrite);
                    target.SetHeader(_nextBlockId, 0);
                    if (written == 0)
                    {
                        target.SetHeader(_recordLength, total);
                    }

                    // Get ready for next loop
                    blocksUsed++;
                    written += bytesToWrite;
                    previousBlock = target;
                }

                // After writing, delete off any unused blocks
                if (blocksUsed < blocks.Count)
                {
                    for (var i = blocksUsed; i < blocks.Count; i++)
                    {
                        MarkASFree(blocks[i].Id);
                    }
                }
            }
            finally
            {
                // Always dispose all fetched blocks after using them
                foreach (var block in blocks)
                {
                    block.Dispose();
                }
            }
        }

        public virtual byte[] Get(uint recordId)
        {
            // First grab the block
            using (var block = _storage.Get(recordId))
            {
                if (block == null) return null;
                
                // If it's a deleted block, ignore it
                if (block.GetHeader(_isDeleted) == 1L) return null;
                
                // If it's a child block, ignore it
                if (block.GetHeader(_prevoiusBlockId) != 0L) return null;
                
                // Grab total record size and allocate corresponded memory
                var totalRecordSize = block.GetHeader(_recordLength);
                if (totalRecordSize > _maxRecordSize)
                {
                    throw new NotSupportedException($"Unexpected record length: {totalRecordSize}");
                }

                var data = new byte[totalRecordSize];
                var bytesRead = 0;
                
                // Now start filling in data
                var currentBlock = block;
                while (true)
                {
                    uint nextBlockId;

                    using (currentBlock)
                    {
                        var thisBlockContentLength = currentBlock.GetHeader(_blockContentLength);
                        if (thisBlockContentLength > _storage.BlockContentSize)
                        {
                            throw new InvalidDataException(
                                $"Unexpected block content length: {thisBlockContentLength}");
                        }
                        
                        // Read all available content of current block
                        currentBlock.Read(dest: data, destOffset: bytesRead, srcOffset: 0, count: (int)thisBlockContentLength);
                        
                        // Update number of bytes read
                        bytesRead += (int)thisBlockContentLength;
                        
                        // Move to the next block if there is any
                        nextBlockId = (uint)currentBlock.GetHeader(_nextBlockId);
                        if (nextBlockId == 0)
                        {
                            return data;
                        }
                    } // Using currentBlock

                    currentBlock = _storage.Get(nextBlockId);
                    if (currentBlock == null)
                    {
                        throw new InvalidDataException($"Block not found by id: {nextBlockId}");
                    }
                }
            }
            
        }

        public virtual uint Create()
        {
            using (var firstBlock = AllocateBlock())
            {
                return firstBlock.Id;
            }
        }

        public virtual uint Create(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentException();
            }
            return Create(recordId => data);
        }

        public virtual uint Create(Func<uint, byte[]> dataGenerator)
        {
            if (dataGenerator == null)
            {
                throw new ArgumentException();
            }
            
            using (var firstBlock = AllocateBlock())
            {
                var returnId = firstBlock.Id;
                
                // Begin writing data
                var data = dataGenerator(returnId);
                var dataWritten = 0;
                var dataToBeWritten = data.Length;
                firstBlock.SetHeader(_recordLength, dataToBeWritten);
                
                // If no data to be written
                // return this block straight away
                if (dataToBeWritten == 0)
                {
                    return returnId;
                }
                
                // Otherwise continue to write data until completion
                var currentBlock = firstBlock;
                while (dataWritten < dataToBeWritten)
                {
                    IBlock nextBlock;

                    using (currentBlock)
                    {
                        // Write as much data as possible to this block
                        var thisWrite = Math.Min(_storage.BlockContentSize, dataToBeWritten - dataWritten);
                        currentBlock.Write(data, dataWritten, 0, thisWrite);
                        currentBlock.SetHeader(_blockContentLength, thisWrite);
                        dataWritten += thisWrite;
                        
                        // If there is still data to be written
                        // move to the next block
                        if (dataWritten < dataToBeWritten)
                        {
                            nextBlock = AllocateBlock();
                            var success = false;

                            try
                            {
                                nextBlock.SetHeader(_prevoiusBlockId, currentBlock.Id);
                                currentBlock.SetHeader(_nextBlockId, nextBlock.Id);
                                success = true;
                            }
                            finally
                            {
                                if (!success && nextBlock != null)
                                {
                                    nextBlock.Dispose();
                                    nextBlock = null;
                                }
                            }
                        }
                        else
                        {
                            break;
                        }
                    } // Using currentBlock
                    
                    // MOve to the next block if possible
                    if (nextBlock != null)
                    {
                        currentBlock = nextBlock;
                    }
                }
                
                // Return id of the first block that got queued
                return returnId;
            }
        }

        public virtual void Delete(uint recordId)
        {
            using (var block = _storage.Get(recordId))
            {
                var currentBlock = block;
                while (true)
                {
                    IBlock nextBlock;

                    using (currentBlock)
                    {
                        MarkASFree(currentBlock.Id);
                        currentBlock.SetHeader(_isDeleted, 1L);

                        var nextBlockId = (uint)currentBlock.GetHeader(_nextBlockId);
                        if (nextBlockId == 0)
                        {
                            break;
                        }
                        
                        nextBlock = _storage.Get(nextBlockId);
                        if (currentBlock == null)
                        {
                            throw new InvalidDataException($"Block not found by id: {nextBlockId}");
                        }
                    } // Using currentBlock
                    
                    // Move to the next block
                    if (nextBlock != null)
                    {
                        currentBlock = nextBlock;
                    }
                }
            }
        }
        
        // Private Methods

        /// <summary>
        /// Find all block of given record, return these blocks in order.
        /// </summary>
        private List<IBlock> GetBlocks(uint recordId)
        {
            var blocks = new List<IBlock>();
            var success = false;

            try
            {
                var currentBlockId = recordId;

                do
                {
                    // Grab next block
                    var block = _storage.Get(currentBlockId);
                    if (block == null)
                    {
                        // Special case: if block #0 is never created, then attempt to create it
                        if (currentBlockId == 0)
                        {
                            block = _storage.CreateNew();
                        }
                        else
                        {
                            throw new Exception($"Block not found by id: {currentBlockId}");
                        }
                    }

                    blocks.Add(block);

                    // If this is a deleted block then ignore it
                    if (block.GetHeader(_isDeleted) == 1L)
                    {
                        throw new InvalidDataException($"Block not found: {currentBlockId}");
                    }

                    // Move next
                    currentBlockId = (uint)block.GetHeader(_nextBlockId);
                } while (currentBlockId != 0);

                success = true;
                return blocks;
            }
            finally
            {
                // In case shit happens, dispose all fetched blocks
                if (!success)
                {
                    foreach (var block in blocks)
                    {
                        block.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Allocate new block for use, either by dequeueing an existing non-used block
        /// or creating a new one
        /// </summary>
        /// <returns>Newly allocated block ready to use </returns>
        private IBlock AllocateBlock()
        {
            IBlock newBlock;

            if (!TryFindFreeBlock(out var reusableBlockId))
            {
                newBlock = _storage.CreateNew();
                if (newBlock == null)
                {
                    throw new Exception("Failed to create new block");
                }
            }
            else
            {
                newBlock = _storage.Get(reusableBlockId);
                if (newBlock == null)
                {
                    throw new InvalidDataException($"Block not found by id: {reusableBlockId}");
                }
                newBlock.SetHeader(_blockContentLength, 0L);
                newBlock.SetHeader(_nextBlockId, 0L);
                newBlock.SetHeader(_prevoiusBlockId, 0L);
                newBlock.SetHeader(_recordLength, 0L);
                newBlock.SetHeader(_isDeleted, 0L);
            }

            return newBlock;
        }

        private bool TryFindFreeBlock(out uint blockId)
        {
            blockId = 0;
            GetSpaceTrackingBlock(out var lastBlock, out var secondLastBlock);
            
            using (lastBlock)
            using (secondLastBlock)
            {
                // If this block is empty then goto previous block
                var currentBlockContentLength = lastBlock.GetHeader(_blockContentLength);
                if (currentBlockContentLength == 0)
                {
                    // IF there is no previous block, return false to indicate we can't dequeue
                    if (secondLastBlock == null) return false;
                    
                    // Dequeue an uint from previous block, then mark current block as free
                    blockId = ReadUInt32FromTrailingContent(secondLastBlock);
                    
                    // Back off 4 bytes before calling AppendUInt32ToContent
                    secondLastBlock.SetHeader(_blockContentLength, secondLastBlock.GetHeader(_blockContentLength) - 4);
                    AppendUInt32ToContent(secondLastBlock, lastBlock.Id);
                    
                    // Forward 4 bytes, as an uint32 has been written
                    secondLastBlock.SetHeader(_blockContentLength, secondLastBlock.GetHeader(_blockContentLength) + 4);
                    secondLastBlock.SetHeader(_nextBlockId, 0);
                    lastBlock.SetHeader(_prevoiusBlockId, 0);
                    
                    // Indicate success
                    return true;
                }
                // If this block is not empty then dequeue an UInt32 from it
                else
                {
                    blockId = ReadUInt32FromTrailingContent(lastBlock);
                    lastBlock.SetHeader(_blockContentLength, currentBlockContentLength - 4);
                    
                    // Indicate success
                    return true;
                }
            }
        }

        private void AppendUInt32ToContent(IBlock block, uint value)
        {
            var contentLength = block.GetHeader(_blockContentLength);

            if (contentLength % 4 != 0)
            {
                throw new DataMisalignedException($"Block content length not %4: {contentLength}");
            }
            
            block.Write(src: LittleEndianByteOrder.GetBytes(value), srcOffset: 0, dstOffset: (int)contentLength, count: 4);
        }

        private uint ReadUInt32FromTrailingContent(IBlock block)
        {
            var buffer = new byte[4];
            var contentLength = block.GetHeader(_blockContentLength);

            if (contentLength % 4 != 0)
            {
                throw new DataMisalignedException($"Block content length not %4: {contentLength}");
            }

            if (contentLength == 0)
            {
                throw new InvalidDataException("Trying to dequeue UInt32 from an empty block");
            }

            block.Read(dest: buffer, destOffset: 0, srcOffset: (int)contentLength - 4, count: 4);
            return LittleEndianByteOrder.GetUInt32(buffer);
        }

        private void MarkASFree(uint blockId)
        {
            IBlock lastBlock, secondLastBlock, targetBlock = null;
            GetSpaceTrackingBlock(out lastBlock, out secondLastBlock);
            
            using (lastBlock)
            using (secondLastBlock)
            {
                try
                {
                    // Just append a number, if there is some space left
                    var contentLength = lastBlock.GetHeader(_blockContentLength);
                    if (contentLength + 4 <= _storage.BlockContentSize)
                    {
                        targetBlock = lastBlock;
                    }
                    // No more space left, allocate new block for writing
                    // Note that we allocate fresh new block, reusing it fucks stuff up
                    else
                    {
                        targetBlock = _storage.CreateNew();
                        targetBlock.SetHeader(_prevoiusBlockId, lastBlock.Id);

                        lastBlock.SetHeader(_nextBlockId, targetBlock.Id);

                        contentLength = 0;
                    }

                    // Write!
                    AppendUInt32ToContent(targetBlock, blockId);

                    // Extend the block length to 4, as we wrote a number
                    targetBlock.SetHeader(_blockContentLength, contentLength + 4);
                }
                finally
                {
                    // Always dispose targetBlock
                    targetBlock?.Dispose();
                }
            }
        }
        
        /// <summary>
        /// Get the last 2 blocks from the free space tracking record
        /// </summary>
        private void GetSpaceTrackingBlock(out IBlock lastBlock, out IBlock secondLastBlock)
        {
            lastBlock = null;
            secondLastBlock = null;
            
            // Grab all record 0's block
            var blocks = GetBlocks(0);

            try
            {
                if (blocks == null || blocks.Count == 0)
                {
                    throw new Exception("Failed to find blocks of record 0");
                }

                // Assign
                lastBlock = blocks[blocks.Count - 1];
                if (blocks.Count > 1)
                {
                    secondLastBlock = blocks[blocks.Count - 2];
                }
            }
            finally
            {
                // Always dispose unused blocks
                if (blocks != null)
                {
                    foreach (var block in blocks)
                    {
                        if ((lastBlock == null || block != lastBlock) &&
                            (secondLastBlock == null || block != secondLastBlock))
                        {
                            block.Dispose();
                        }
                    }
                }
            }
        }
    }
}