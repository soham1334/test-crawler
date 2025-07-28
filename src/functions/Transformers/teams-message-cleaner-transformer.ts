// src\functions\Transformers\teams-message-cleaner-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';
import * as cheerio from 'cheerio'; // For HTML parsing

/**
 * Cleans up Teams chat message content, typically by stripping HTML tags.
 * Can be extended to extract mentions, links, or other entities.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to an array of transformed IngestionData objects.
 */
const teamsMessageCleanerTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => {
    logger.info(`TeamsMessageCleanerTransformer: Processing ${data.length} items.`);
    const transformedData: IngestionData[] = [];

    for (const item of data) {
        let cleanedContent: string = item.content instanceof Buffer ? item.content.toString('utf8') : String(item.content || '');
        
        // Teams chat messages often contain HTML. Strip it to get plain text.
        if (cleanedContent.includes('<') && cleanedContent.includes('>')) {
            try {
                const $ = cheerio.load(cleanedContent);
                // Remove specific elements that are not part of the message content (e.g., hidden formatting)
                $('span.mention').each((i, el) => {
                    // Replace mentions with their plain text equivalent, e.g., "@User Name"
                    const userName = $(el).attr('alt');
                    if (userName) {
                        $(el).replaceWith(`@${userName}`);
                    } else {
                        $(el).replaceWith($(el).text()); // Fallback to inner text
                    }
                });
                // Remove other unwanted tags or attributes
                $('a').each((i, el) => {
                    const href = $(el).attr('href');
                    if (href) {
                        $(el).replaceWith(`${$(el).text()} (${href})`); // Keep link text and URL
                    } else {
                        $(el).replaceWith($(el).text());
                    }
                });
                
                cleanedContent = $.text().replace(/\s+/g, ' ').trim(); // Get plain text and normalize whitespace
                logger.debug(`TeamsMessageCleanerTransformer: Stripped HTML from message ID: ${item.id}`);
            } catch (error: any) {
                logger.warn(`TeamsMessageCleanerTransformer: Failed to strip HTML from message ID ${item.id}: ${error.message}. Keeping original content.`, { error });
                // Fallback: keep original content if HTML stripping fails
            }
        }

        transformedData.push({
            ...item,
            content: cleanedContent,
            metadata: {
                ...item.metadata,
                cleanedContent: true, // Add a flag that content has been cleaned
                transformedBy: 'teams-message-cleaner-transformer'
            }
        });
    }
    logger.info(`TeamsMessageCleanerTransformer: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default teamsMessageCleanerTransformer;
