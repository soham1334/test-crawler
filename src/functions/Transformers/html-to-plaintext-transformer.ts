// src/mappings/html-to-plaintext-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';
import * as cheerio from 'cheerio'; // For HTML parsing

/**
 * Transforms HTML content within IngestionData items into clean plain text,
 * preserving line breaks for block-level elements.
 * It removes script and style tags and normalizes whitespace within lines.
 * This version is robust in detecting HTML content.
 * Items that are not HTML or have no content are passed through unchanged.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to an array of transformed IngestionData objects.
 */
const htmlToPlaintextTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => {
    logger.info(`HtmlToPlaintextTransformer: Processing ${data.length} items.`);
    const transformedData: IngestionData[] = [];

    for (const item of data) {
        // Ensure content is a string for HTML parsing
        const contentString = item.content instanceof Buffer ? item.content.toString('utf8') : String(item.content || '');
        const itemMimeType = item.metadata?.mimeType;

        logger.debug(`HtmlToPlaintextTransformer: Checking item ID: ${item.id}`);
        logger.debug(`  Content Type: ${typeof item.content}`);
        logger.debug(`  Content is Buffer: ${item.content instanceof Buffer}`);
        logger.debug(`  Content String Length: ${contentString.length}`);
        logger.debug(`  MimeType in metadata: ${itemMimeType}`);
        logger.debug(`  Content snippet (first 100 chars): ${contentString.substring(0, 100)}`);

        // Determine if the content should be treated as HTML
        const isHtmlContent = 
            (itemMimeType && (itemMimeType.includes('text/html') || itemMimeType.includes('application/xhtml+xml'))) ||
            (contentString.startsWith('<') && (contentString.includes('<html') || contentString.includes('<body')));

        logger.debug(`  Is HTML Content (heuristic): ${isHtmlContent}`);

        if (contentString && isHtmlContent) {
            try {
                const $ = cheerio.load(contentString);
                
                // Remove script and style tags first
                $('script, style').remove();

                // Add newlines after common block-level elements before extracting text
                // This helps preserve paragraph breaks and list item separation
                $('p, h1, h2, h3, h4, h5, h6, li, div, br').each((_, element) => {
                    $(element).append('\n'); // Append a newline after the element's content
                });

                // Extract text from the body and normalize whitespace.
                // The `\s+` will still collapse multiple spaces/newlines within a single block,
                // but the appended `\n` will ensure breaks between blocks.
                const plainTextContent = $('body').text()
                                            .replace(/\s*\n\s*/g, '\n') // Normalize multiple newlines/spaces around newlines
                                            .replace(/[ \t]+/g, ' ')   // Normalize multiple spaces/tabs within a line
                                            .trim();

                transformedData.push({
                    ...item, // Keep all original properties
                    content: plainTextContent, // Update content to plain text
                    metadata: {
                        ...item.metadata,
                        originalMimeType: itemMimeType, // Keep track of original type
                        mimeType: 'text/plain', // Update mimeType to plain text
                        transformedBy: 'html-to-plaintext-transformer'
                    }
                });
                logger.debug(`HtmlToPlaintextTransformer: Successfully transformed HTML to plain text for ID: ${item.id}`);
            } catch (error: any) {
                logger.warn(`HtmlToPlaintextTransformer: Failed to process HTML for ID ${item.id}: ${error.message}. Keeping original content.`, { error });
                // If transformation fails, push the original item with an error status/metadata
                transformedData.push({
                    ...item,
                    statusCode: 500, // Indicate an issue with this specific item
                    metadata: {
                        ...item.metadata,
                        transformationError: error.message,
                        transformedBy: 'html-to-plaintext-transformer-failed'
                    }
                });
            }
        } else {
            // If not HTML or no content, pass through unchanged
            logger.debug(`HtmlToPlaintextTransformer: Skipping non-HTML content for ID: ${item.id}.`);
            transformedData.push(item);
        }
    }
    logger.info(`HtmlToPlaintextTransformer: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default htmlToPlaintextTransformer;
