// src\functions\Transformers\gdrive-content-normalizer-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';
import pdf from 'pdf-parse'; // Changed import to default import for pdf-parse

/**
 * Processes content from Google Drive items, normalizing it to plain text where possible.
 * Handles PDF content extraction and ensures other text content is a string.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to an array of transformed IngestionData objects.
 */
const gdriveContentNormalizerTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => { // FIX: Renamed constant
    logger.info(`GdriveContentNormalizerTransformer: Processing ${data.length} items.`); // FIX: Updated log name
    const transformedData: IngestionData[] = [];

    for (const item of data) {
        let newContent: string | object | Buffer | undefined = item.content; 
        let newMimeType: string | undefined = item.metadata?.mimeType;

        try {
            if (item.content instanceof Buffer) {
                if (item.metadata?.mimeType === 'application/pdf') {
                    logger.debug(`GdriveContentNormalizerTransformer: Parsing PDF content for ID: ${item.id}`); // FIX: Updated log name
                    const pdfData = await pdf(item.content); 
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
                        logger.warn(`GdriveContentNormalizerTransformer: Could not convert Buffer to UTF-8 for ID ${item.id}. Keeping as original Buffer.`, { bufferError }); // FIX: Updated log name
                        newContent = item.content; // Keep original Buffer if conversion fails
                    }
                }
            } else if (typeof item.content === 'object' && item.content !== null) {
                // If content is an object, stringify it for text-based processing
                try {
                    newContent = JSON.stringify(item.content);
                    if (!newMimeType || newMimeType === 'application/octet-stream') {
                        newMimeType = 'application/json'; // Update MIME type to JSON
                    }
                    logger.debug(`GdriveContentNormalizerTransformer: Stringified object content for ID: ${item.id}`); // FIX: Updated log name
                } catch (jsonError: any) {
                    logger.warn(`GdriveContentNormalizerTransformer: Could not stringify object content for ID ${item.id}: ${jsonError.message}. Content will be empty string.`, { jsonError }); // FIX: Updated log name
                    newContent = ''; // Default to empty string if stringify fails
                }
            } else if (item.content === undefined || item.content === null) {
                // Ensure content is at least an empty string if it was null/undefined
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
                content: newContent as (string | object | Buffer), 
                metadata: {
                    ...item.metadata,
                    mimeType: newMimeType,
                    transformedBy: 'gdrive-content-normalizer-transformer' // FIX: Updated transformer name
                }
            });
            logger.debug(`GdriveContentNormalizerTransformer: Normalized content for ID: ${item.id}`); // FIX: Updated log name
        } catch (error: any) {
            logger.warn(`GdriveContentNormalizerTransformer: Failed to normalize content for ID ${item.id}: ${error.message}. Keeping original item with error.`, { error }); // FIX: Updated log name
            transformedData.push({
                ...item,
                statusCode: 500,
                metadata: {
                    ...item.metadata,
                    transformationError: error.message,
                    transformedBy: 'gdrive-content-normalizer-transformer-failed' // FIX: Updated transformer name
                }
            });
        }
    }
    logger.info(`GdriveContentNormalizerTransformer: Finished processing. Returned ${transformedData.length} items.`); // FIX: Updated log name
    return transformedData;
};

export default gdriveContentNormalizerTransformer; // FIX: Export renamed constant
