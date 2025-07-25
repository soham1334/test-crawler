// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\functions\ingestion\FileSystemDestinationAdapter.ts

import { IDestinationPlugin, IngestionData } from './interfaces';
import { GSStatus, logger } from '@godspeedsystems/core';
import * as fs from 'fs/promises';
import * as path from 'path';

export interface FileSystemDestinationConfig {
    outputPath?: string;
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

    async processData(data: IngestionData[]): Promise<GSStatus> {
        if (!this.isInitializedForSaving || !this.config?.outputPath) {
            return new GSStatus(false, 400, `FileSystemDestinationAdapter: Skipping save of ${data.length} files because outputPath was not provided or directory could not be created.`);
        }

        logger.info(`FileSystemDestinationAdapter: Attempting to save batch of ${data.length} items to ${this.config.outputPath}`);
        let successCount = 0;
        let errorCount = 0;
        const errors: string[] = [];

        const results = await Promise.allSettled(data.map(async (item) => {
            const saveSingleItem = async (singleItem: IngestionData): Promise<GSStatus> => {
                const filename = singleItem.metadata?.filename || path.basename(singleItem.id) || `data-${Date.now()}`;
                const relativePath = singleItem.metadata?.relativePath || '';
                
                // Extract fetchedAt, URL, and StatusCode from the IngestionData item
                // FIX: Add debug log for singleItem.fetchedAt
                logger.debug(`[FileSystemDestinationAdapter DEBUG] singleItem.fetchedAt:`, singleItem.fetchedAt);
                const fetchedAt = singleItem.fetchedAt ? singleItem.fetchedAt.toISOString() : 'N/A';
                const url = singleItem.url || singleItem.metadata?.url || 'N/A';
                const statusCode = singleItem.statusCode || singleItem.metadata?.statusCode || 'N/A';

                const targetDir = path.join(this.config!.outputPath!, relativePath ? path.dirname(relativePath) : '');
                const targetFilePath = path.join(targetDir, filename);

                try {
                    if (singleItem.content instanceof Buffer || typeof singleItem.content === 'string') {
                        await fs.mkdir(targetDir, { recursive: true });
                        
                        const metadataHeader = `
--- Metadata ---
ID: ${singleItem.id}
URL: ${url}
Status Code: ${statusCode}
Fetched At: ${new Date()}
----------------
\n`;

                        const contentToWrite = typeof singleItem.content === 'string'
                            ? metadataHeader + singleItem.content
                            : Buffer.concat([Buffer.from(metadataHeader), singleItem.content]);

                        await fs.writeFile(targetFilePath, contentToWrite);
                        logger.debug(`FileSystemDestinationAdapter: Saved single item: ${targetFilePath}`);
                        return new GSStatus(true, 200, `Successfully saved item ${singleItem.id} to ${targetFilePath}`);
                    } else {
                        logger.warn(`FileSystemDestinationAdapter: Item ID ${singleItem.id} has unsupported content type for file save.`);
                        return new GSStatus(false, 400, `Unsupported content type for item ${singleItem.id}`);
                    }
                } catch (error: any) {
                    logger.error(`FileSystemDestinationAdapter: Failed to save item ${singleItem.id} to ${targetFilePath}: ${error.message}`, { error });
                    return new GSStatus(false, 500, `Failed to save item ${singleItem.id}: ${error.message}`);
                }
            };

            return saveSingleItem(item);
        }));

        results.forEach(result => {
            if (result.status === 'fulfilled' && result.value.success) {
                successCount++;
            } else {
                errorCount++;
                if (result.status === 'fulfilled') {
                    errors.push(`Item failed: ${result.value.message}`);
                } else { // result.status === 'rejected'
                    errors.push(`Item processing threw an error: ${result.reason?.message || result.reason}`);
                }
            }
        });

        if (errorCount > 0) {
            logger.error(`FileSystemDestinationAdapter: Batch save completed with ${errorCount} errors.`, { errors });
            return new GSStatus(false, 500, `Batch save failed for ${errorCount} items.`, { data: { errors } });
        }
        logger.info(`FileSystemDestinationAdapter: Successfully saved ${successCount} items in batch.`);
        return new GSStatus(true, 200, `Successfully saved ${successCount} items in batch.`);
    }
}
