// src/mappings/passthrough-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';

/**
 * A simple passthrough transformer that takes an array of IngestionData items
 * and returns them unchanged.
 *
 * This transformer is useful when the data source already provides the data
 * in the desired format (e.g., a Buffer for binary files) and no further
 * transformation (like text extraction or parsing) is needed before sending
 * to the destination.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to the same array of IngestionData objects.
 */
const passthroughTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => {
    logger.info(`PassthroughTransformer: Processing ${data.length} items. Passing them through unchanged.`);
    // The map is used here to create a new array reference, but the items themselves
    // are passed without modification.
    return data.map(item => ({ ...item })); 
};

export default passthroughTransformer;
