using System;

namespace database_from_scratch.Core
{
    public interface IRecordStorage
    {
        /// <summary>
        /// Update a record
        /// </summary>
        void Update(uint recordId, byte[] data);

        /// <summary>
        /// Grab a record's data
        /// </summary>
        byte[] Get(uint recordId);

        /// <summary>
        /// Creates a new empty record
        /// </summary>
        uint Create();

        /// <summary>
        /// Creates a new record with the given data
        /// </summary>
        uint Create(byte[] data);

        /// <summary>
        /// Creates a new record with a data generator that generates the data
        /// </summary>
        uint Create(Func<uint, byte[]> dataGenerator);

        /// <summary>
        /// Deletes a record by its id
        /// </summary>
        void Delete(uint recordId);
    }
}