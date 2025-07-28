// \src\functions\Transformers\blob-content-processor-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';
import pdf from 'pdf-parse'; // FIX: Changed import to default import for pdf-parse

/**
 * Processes content from Azure Blob Storage items, normalizing it to plain text where possible.
 * Handles PDF content extraction and ensures other text content is a string.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to an array of transformed IngestionData objects.
 */
const blobContentProcessorTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => {
    logger.info(`BlobContentProcessorTransformer: Processing ${data.length} items.`);
    const transformedData: IngestionData[] = [];

    for (const item of data) {
        // FIX: Initialize newContent to a type that can accommodate string, object, or Buffer
        // Assuming IngestionData.content can be string | object | Buffer
        let newContent: string | object | Buffer | undefined = item.content; 
        let newMimeType: string | undefined = item.metadata?.mimeType;

        try {
            if (item.content instanceof Buffer) {
                if (item.metadata?.mimeType === 'application/pdf') {
                    logger.debug(`BlobContentProcessorTransformer: Parsing PDF content for ID: ${item.id}`);
                    const pdfData = await pdf(item.content); // FIX: pdf is now callable
                    newContent = pdfData.text;
                    newMimeType = 'text/plain';
                } else {
                    try {
                        // Attempt to convert other binary content to UTF-8 string
                        newContent = item.content.toString('utf8');
                        if (!newMimeType || newMimeType === 'application/octet-stream') {
                            newMimeType = 'text/plain'; // Assume plain text if successfully converted
                        }
                    } catch (bufferError: any) {
                        logger.warn(`BlobContentProcessorTransformer: Could not convert Buffer to UTF-8 for ID ${item.id}. Keeping as original Buffer.`, { bufferError });
                        newContent = item.content; // Keep original Buffer if conversion fails
                    }
                }
            } else if (typeof item.content === 'object' && item.content !== null) {
                // FIX: If content is an object, stringify it for text-based processing
                try {
                    newContent = JSON.stringify(item.content);
                    if (!newMimeType || newMimeType === 'application/octet-stream') {
                        newMimeType = 'application/json'; // Update MIME type to JSON
                    }
                    logger.debug(`BlobContentProcessorTransformer: Stringified object content for ID: ${item.id}`);
                } catch (jsonError: any) {
                    logger.warn(`BlobContentProcessorTransformer: Could not stringify object content for ID ${item.id}: ${jsonError.message}. Content will be empty string.`, { jsonError });
                    newContent = ''; // Default to empty string if stringify fails
                }
            } else if (item.content === undefined || item.content === null) {
                // FIX: Ensure content is at least an empty string if it was null/undefined
                newContent = '';
                if (!newMimeType) {
                    newMimeType = 'text/plain';
                }
            } else {
                // Content is already a string or other primitive, ensure it's a string
                newContent = String(item.content);
                if (!newMimeType) {
                    newMimeType = 'text/plain';
                }
            }

            transformedData.push({
                ...item,
                // FIX: Cast newContent to ensure compatibility with IngestionData.content type
                // This assertion tells TypeScript that newContent will conform to the expected type.
                content: newContent as (string | object | Buffer), 
                metadata: {
                    ...item.metadata,
                    mimeType: newMimeType,
                    transformedBy: 'blob-content-processor-transformer'
                }
            });
            logger.debug(`BlobContentProcessorTransformer: Normalized content for ID: ${item.id}`);
        } catch (error: any) {
            logger.warn(`BlobContentProcessorTransformer: Failed to normalize content for ID ${item.id}: ${error.message}. Keeping original item with error.`, { error });
            transformedData.push({
                ...item,
                statusCode: 500,
                metadata: {
                    ...item.metadata,
                    transformationError: error.message,
                    transformedBy: 'blob-content-processor-transformer-failed'
                }
            });
        }
    }
    logger.info(`BlobContentProcessorTransformer: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default blobContentProcessorTransformer;
