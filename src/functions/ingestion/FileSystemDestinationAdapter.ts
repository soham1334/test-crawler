// src/functions/ingestion/FileSystemDestinationAdapter.ts

import { IDestinationPlugin, IngestionData } from './interfaces';
import { GSStatus, logger } from '@godspeedsystems/core';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as mime from 'mime-types'; // NEW: Import mime-types for better extension detection

export interface FileSystemDestinationConfig {
    outputPath?: string;
}

// Define an interface for the metadata object as it will be serialized to JSON
interface SerializedIngestionMetadata extends Omit<Partial<IngestionData>, 'fetchedAt' | 'content'> {
    fetchedAt?: string; // Override fetchedAt to be a string for serialization
    // Ensure 'content' is explicitly omitted or handled separately if it was ever part of metadata
}

export class FileSystemDestinationAdapter implements IDestinationPlugin {
    private config: FileSystemDestinationConfig | undefined;
    private isInitializedForSaving: boolean = false;

    constructor() {
        logger.info("FileSystemDestinationAdapter instance created.");
    }

    async init(config: FileSystemDestinationConfig): Promise<void> {
        this.config = config;

        if (!this.config?.outputPath) {
            logger.warn("FileSystemDestinationAdapter: 'outputPath' is not provided in configuration. Files will NOT be saved to disk.");
            this.isInitializedForSaving = false;
            return;
        }

        try {
            await fs.mkdir(this.config.outputPath, { recursive: true });
            logger.info(`FileSystemDestinationAdapter initialized. Output path: ${this.config.outputPath}`);
            this.isInitializedForSaving = true;
        } catch (error: any) {
            logger.error(`FileSystemDestinationAdapter: Failed to create output directory ${this.config.outputPath}: ${error.message}. Files will NOT be saved to disk.`, { error });
            this.isInitializedForSaving = false;
        }
    }

    // Helper to generate a consistent and safe base filename from the IngestionData ID
    private getSafeBaseFilename(item: IngestionData): string {
        if (!this.config?.outputPath) {
            throw new Error("outputPath is not configured for FileSystemDestinationAdapter.");
        }
        // Use the item.id as the base for the filename, making it filesystem-safe.
        // Replace characters that are invalid or problematic in filenames.
        return item.id.replace(/[^a-zA-Z0-9\-\._]/g, '_');
    }

    // NEW: Helper to determine file extension based on MIME type or original file name
    private getFileExtension(item: IngestionData): string {
        // 1. Try to get extension from original file name (if available)
        if (item.metadata?.fileName) {
            const ext = path.extname(item.metadata.fileName);
            if (ext) return ext;
        }
        // 2. Try to get extension from MIME type using mime-types library
        if (item.metadata?.mimeType) {
            const ext = mime.extension(item.metadata.mimeType);
            if (ext) return `.${ext}`;
        }
        // 3. Fallback based on content type if mimeType is generic or missing
        if (item.content instanceof Buffer) {
            return '.bin'; // Default for unknown binary
        } else if (typeof item.content === 'string') {
            if (item.content.startsWith('{') && item.content.endsWith('}')) return '.json';
            if (item.content.includes('<html') || item.content.includes('<body')) return '.html';
            if (item.content.includes('<svg')) return '.svg';
            return '.txt'; // Default for generic text
        }
        return '.dat'; // Last resort
    }

    async processData(data: IngestionData[]): Promise<GSStatus> {
        if (!this.isInitializedForSaving || !this.config?.outputPath) {
            return new GSStatus(false, 400, `FileSystemDestinationAdapter: Skipping save of ${data.length} files because outputPath was not provided or directory could not be created.`);
        }

        logger.info(`FileSystemDestinationAdapter: Attempting to process batch of ${data.length} items to ${this.config.outputPath}`);
        let successCount = 0;
        let errorCount = 0;
        let deletedCount = 0;
        const errors: string[] = [];

        const results = await Promise.allSettled(data.map(async (item) => {
            const baseFilename = this.getSafeBaseFilename(item);
            const fileExtension = this.getFileExtension(item); // Use the new helper
            
            // Construct paths for the content file and the metadata file
            const contentFilePath = path.join(this.config!.outputPath!, `${baseFilename}${fileExtension}`);
            const metadataFilePath = path.join(this.config!.outputPath!, `${baseFilename}.metadata.json`);

            try {
                if (item.metadata?.changeType === 'removed') {
                    // Handle deletion: Try to delete both the content file and its metadata file
                    logger.debug(`FileSystemDestinationAdapter: Attempting to delete files for removed item: ${contentFilePath} and ${metadataFilePath}`);
                    let contentDeleted = false;
                    let metadataDeleted = false;

                    try {
                        await fs.unlink(contentFilePath);
                        logger.info(`FileSystemDestinationAdapter: Successfully deleted content file: ${contentFilePath}`);
                        contentDeleted = true;
                    } catch (deleteError: any) {
                        if (deleteError.code === 'ENOENT') {
                            logger.warn(`FileSystemDestinationAdapter: Content file to delete not found: ${contentFilePath}.`);
                            contentDeleted = true; 
                        } else {
                            logger.error(`FileSystemDestinationAdapter: Failed to delete content file ${contentFilePath}: ${deleteError.message}`, { error: deleteError });
                        }
                    }

                    try {
                        await fs.unlink(metadataFilePath);
                        logger.info(`FileSystemDestinationAdapter: Successfully deleted metadata file: ${metadataFilePath}`);
                        metadataDeleted = true;
                    } catch (deleteError: any) {
                        if (deleteError.code === 'ENOENT') {
                            logger.warn(`FileSystemDestinationAdapter: Metadata file to delete not found: ${metadataFilePath}.`);
                            metadataDeleted = true; 
                        } else {
                            logger.error(`FileSystemDestinationAdapter: Failed to delete metadata file ${metadataFilePath}: ${deleteError.message}`, { error: deleteError });
                        }
                    }

                    if (contentDeleted && metadataDeleted) {
                        deletedCount++;
                        return new GSStatus(true, 200, `Successfully deleted item ${item.id} (content & metadata)`);
                    } else {
                        throw new Error(`Partial deletion for item ${item.id}. Content deleted: ${contentDeleted}, Metadata deleted: ${metadataDeleted}`);
                    }
                } else {
                    // Handle addition/modification/full_scan
                    logger.debug(`FileSystemDestinationAdapter: Saving item content to: ${contentFilePath}`);
                    logger.debug(`FileSystemDestinationAdapter: Saving item metadata to: ${metadataFilePath}`);
                    
                    // Ensure the directory exists before writing files
                    await fs.mkdir(path.dirname(contentFilePath), { recursive: true });

                    // 1. Prepare content for writing: Directly use Buffer or convert string content
                    let contentToWrite: string | Buffer;
                    if (item.content instanceof Buffer) {
                        contentToWrite = item.content; // Write Buffer directly
                    } else if (typeof item.content === 'string') {
                        contentToWrite = item.content; // Write string directly
                    } else if (item.content !== null && item.content !== undefined) {
                        // Fallback for object content (e.g., JSON payloads)
                        contentToWrite = JSON.stringify(item.content, null, 2); 
                    } else {
                        contentToWrite = ''; // Handle null/undefined content
                    }
                    
                    await fs.writeFile(contentFilePath, contentToWrite); // Write content

                    // 2. Save the metadata to a separate JSON file
const metadataToSave: SerializedIngestionMetadata = {
    ...item,
    // Explicitly convert fetchedAt to ISO string here
    fetchedAt: item.fetchedAt ? item.fetchedAt.toISOString() : undefined,
    // Explicitly remove content, as it's not part of SerializedIngestionMetadata
    content: undefined // Ensure 'content' property is not copied or is undefined
};
                    await fs.writeFile(metadataFilePath, JSON.stringify(metadataToSave, null, 2), 'utf8');

                    logger.debug(`FileSystemDestinationAdapter: Saved content and metadata for item: ${baseFilename}${fileExtension}`); // FIX: Log correct filename
                    successCount++;
                    return new GSStatus(true, 200, `Successfully saved item ${item.id} to ${contentFilePath} and ${metadataFilePath}`);
                }
            } catch (error: any) {
                logger.error(`FileSystemDestinationAdapter: Error processing item ${item.id}: ${error.message}`, { error });
                throw new Error(`Failed to process item ${item.id}: ${error.message}`); 
            }
        }));

        results.forEach(result => {
            if (result.status === 'rejected') { 
                errorCount++;
                errors.push(`Item processing threw an error: ${result.reason?.message || result.reason}`);
            }
        });

        if (errorCount > 0) {
            logger.error(`FileSystemDestinationAdapter: Batch processing completed with ${errorCount} errors.`, { errors });
            return new GSStatus(false, 500, `Batch processing failed for ${errorCount} items.`, { data: { errors } });
        }
        logger.info(`FileSystemDestinationAdapter: Successfully processed batch. ${successCount} items saved/updated, ${deletedCount} items deleted.`);
        return new GSStatus(true, 200, `Successfully processed ${successCount + deletedCount} items in batch.`);
    }
}
