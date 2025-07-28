// src\functions\Transformers\generic-ingestion-preprocessor.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';

/**
 * A generic preprocessor transformer for IngestionData items.
 * It performs universal tasks such as:
 * - Ensuring content is a string (converting Buffers to UTF-8).
 * - Normalizing whitespace in text content.
 * - Adding/updating metadata fields like 'ingestionTimestamp' and 'contentLength'.
 * - Adding a 'sourcePluginType' if available in metadata.
 *
 * @param data An array of IngestionData objects.
 * @returns An array of transformed IngestionData objects.
 */
const genericIngestionPreprocessor = (data: IngestionData[]): IngestionData[] => {
    logger.info(`GenericIngestionPreprocessor: Processing ${data.length} items.`);
    const transformedData: IngestionData[] = [];
    const ingestionTimestamp = new Date().toISOString();

    for (const item of data) {
        let processedContent: string | undefined;
        let contentLength: number | undefined;

        // 1. Ensure content is a string (convert Buffer to UTF-8)
        if (item.content instanceof Buffer) {
            try {
                processedContent = item.content.toString('utf8');
                logger.debug(`GenericIngestionPreprocessor: Converted Buffer content to string for ID: ${item.id}`);
            } catch (bufferError: any) {
                logger.warn(`GenericIngestionPreprocessor: Could not convert Buffer to UTF-8 for ID ${item.id}. Content will be empty string.`, { bufferError });
                processedContent = ''; // Default to empty string if conversion fails
            }
        } else if (item.content !== undefined && item.content !== null) {
            processedContent = String(item.content);
        } else {
            processedContent = ''; // Default to empty string if content is null/undefined
        }

        // 2. Normalize whitespace in text content
        if (typeof processedContent === 'string') {
            processedContent = processedContent.replace(/\s+/g, ' ').trim();
            contentLength = processedContent.length;
        }

        // 3. Add/update metadata fields
        const newMetadata = {
            ...item.metadata,
            ingestionTimestamp: ingestionTimestamp, // When this item was processed by the pipeline
            contentLength: contentLength, // Length of the processed string content
            processedByGenericPreprocessor: true,
            // If the original source already added 'sourcePluginType', it will be preserved.
            // Otherwise, you could add a default here if desired.
            // sourcePluginType: item.metadata?.sourcePluginType || 'unknown_source', 
        };

        transformedData.push({
            ...item,
            content: processedContent,
            metadata: newMetadata,
        });
        logger.debug(`GenericIngestionPreprocessor: Processed item ID: ${item.id}`);
    }

    logger.info(`GenericIngestionPreprocessor: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default genericIngestionPreprocessor;
