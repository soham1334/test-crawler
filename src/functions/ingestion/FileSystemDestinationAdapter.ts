// C:\Users\SOHAM\Desktop\test godspeed\test-project\src\functions\ingestion\FileSystemDestinationAdapter.ts
// Assuming this file is now in src/functions/ingestion/ and not src/test/

import { IDestinationPlugin, IngestionData } from './interfaces'; // Corrected path to interfaces
import { GSStatus, logger } from '@godspeedsystems/core';
import * as fs from 'fs/promises';
import * as path from 'path';

export interface FileSystemDestinationConfig {
    outputPath?: string; // Made optional in interfaces.ts
}

export class FileSystemDestinationAdapter implements IDestinationPlugin {
    private config: FileSystemDestinationConfig | undefined;
    private isInitializedForSaving: boolean = false; // Flag to track if saving is possible

    constructor() {
        logger.info("FileSystemDestinationAdapter instance created.");
    }

    /**
     * Initializes the FileSystemDestinationAdapter.
     * @param config The configuration, including the base outputPath.
     * @returns A Promise<void> as per IDestinationPlugin.
     */
    async init(config: FileSystemDestinationConfig): Promise<void> { // Return type changed to void as per IDestinationPlugin
        this.config = config;

        if (!this.config?.outputPath) {
            logger.warn("FileSystemDestinationAdapter: 'outputPath' is not provided in configuration. Files will NOT be saved to disk.");
            this.isInitializedForSaving = false;
            return; // Exit init gracefully
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

    /**
     * Processes (writes) a batch of IngestionData items to the file system.
     * This method implements the 'processData' from IDestinationPlugin.
     * @param data The array of IngestionData items to save.
     * @returns A GSStatus indicating overall success or failure for the batch.
     */
    async processData(data: IngestionData[]): Promise<GSStatus> {
        if (!this.isInitializedForSaving || !this.config?.outputPath) {
            return new GSStatus(false, 400, `FileSystemDestinationAdapter: Skipping save of ${data.length} files because outputPath was not provided or directory could not be created.`);
        }

        logger.info(`FileSystemDestinationAdapter: Attempting to save batch of ${data.length} items to ${this.config.outputPath}`);
        let successCount = 0;
        let errorCount = 0;
        const errors: string[] = [];

        // Use Promise.allSettled to process all items and collect results
        const results = await Promise.allSettled(data.map(async (item) => {
            // Internal helper to save a single item
            const saveSingleItem = async (singleItem: IngestionData): Promise<GSStatus> => {
                const filename = singleItem.metadata?.filename || path.basename(singleItem.id) || `data-${Date.now()}`;
                const relativePath = singleItem.metadata?.relativePath || '';
                
                const targetDir = path.join(this.config!.outputPath!, relativePath ? path.dirname(relativePath) : ''); // Use ! as we checked isInitializedForSaving
                const targetFilePath = path.join(targetDir, filename);

                try {
                    if (singleItem.content instanceof Buffer || typeof singleItem.content === 'string') {
                        await fs.mkdir(targetDir, { recursive: true });
                        await fs.writeFile(targetFilePath, singleItem.content);
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
            return new GSStatus(false, 500, `Batch save failed for ${errorCount} items.`, { data: { errors } }); // Use 'data' for errors
        }
        logger.info(`FileSystemDestinationAdapter: Successfully saved ${successCount} items in batch.`);
        return new GSStatus(true, 200, `Successfully saved ${successCount} items in batch.`);
    }

    // Removed the individual 'send' and 'sendBatch' methods as 'processData' now handles the batch.
    // The internal 'saveSingleItem' function within processData replaces the old 'send' logic.
}