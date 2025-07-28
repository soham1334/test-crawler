// src\functions\Transformers\code-metadata-extractor-transformer.ts

import { IngestionData } from '../ingestion/interfaces'; // Adjust path if necessary
import { logger } from '@godspeedsystems/core';
import * as path from 'path'; // For path manipulation

// A simple mapping for common code file extensions to languages
const LANGUAGE_MAP: { [key: string]: string } = {
    '.js': 'JavaScript',
    '.ts': 'TypeScript',
    '.py': 'Python',
    '.java': 'Java',
    '.c': 'C',
    '.cpp': 'C++',
    '.cs': 'C#',
    '.go': 'Go',
    '.rb': 'Ruby',
    '.php': 'PHP',
    '.html': 'HTML',
    '.css': 'CSS',
    '.json': 'JSON',
    '.xml': 'XML',
    '.md': 'Markdown',
    '.sh': 'Shell',
    '.yml': 'YAML',
    '.yaml': 'YAML',
};

/**
 * Extracts language and a code preview from code files within IngestionData items.
 * Adds 'language' and 'codePreview' to the metadata.
 * Non-code files are passed through unchanged.
 *
 * @param data An array of IngestionData objects.
 * @returns A Promise resolving to an array of transformed IngestionData objects.
 */
const gitcodeMetadataExtractorTransformer = async (data: IngestionData[]): Promise<IngestionData[]> => {
    logger.info(`CodeMetadataExtractorTransformer: Processing ${data.length} items.`);
    const transformedData: IngestionData[] = [];

    for (const item of data) {
        // Ensure content is a string for text processing
        const contentString = item.content instanceof Buffer ? item.content.toString('utf8') : String(item.content || '');

        // Attempt to determine language from file extension
        const fileExtension = item.metadata?.filePath ? path.extname(item.metadata.filePath).toLowerCase() : '';
        const language = LANGUAGE_MAP[fileExtension] || 'Unknown';

        // Check if it's likely a code or text file (not binary, and has content)
        if (contentString && contentString.length > 0 && !item.metadata?.mimeType?.startsWith('image/') && !item.metadata?.mimeType?.startsWith('application/octet-stream')) {
            const lines = contentString.split('\n');
            const codePreview = lines.slice(0, 10).join('\n'); // First 10 lines as preview

            transformedData.push({
                ...item,
                content: contentString, // Ensure content is string if it was Buffer
                metadata: {
                    ...item.metadata,
                    language: language,
                    codePreview: codePreview,
                    transformedBy: 'code-metadata-extractor-transformer'
                }
            });
            logger.debug(`CodeMetadataExtractorTransformer: Extracted metadata for ID: ${item.id}, Language: ${language}`);
        } else {
            // Pass through unchanged if not a text/code file or no content
            transformedData.push(item);
        }
    }
    logger.info(`CodeMetadataExtractorTransformer: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default gitcodeMetadataExtractorTransformer;
