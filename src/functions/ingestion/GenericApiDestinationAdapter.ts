//C:\Users\SOHAM\Desktop\crawler\test-crawler\src\functions\ingestion\GenericApiDestinationAdapter.ts

import { IDestinationPlugin, IngestionData } from './interfaces';
import { GSStatus, logger } from '@godspeedsystems/core';

export class GenericApiDestinationAdapter implements IDestinationPlugin {
    private config: any;

    constructor() {
        logger.info("GenericApiDestinationAdapter instance created.");
    }

    async init(config: any): Promise<void> {
        this.config = config;
        logger.info(`GenericApiDestinationAdapter initialized with config: ${JSON.stringify(config)}`);
    }

    /**
     * Processes (sends) a batch of IngestionData items to the generic API endpoint.
     * This method implements the 'processData' from IDestinationPlugin.
     * @param data The array of IngestionData items to send.
     * @returns A GSStatus indicating overall success or failure for the batch.
     */
    async processData(data: IngestionData[]): Promise<GSStatus> { // <--- FIX: Renamed sendBatch to processData
        if (!this.config || !this.config.endpoint) { // Example check for endpoint config
            const errorMessage = "GenericApiDestinationAdapter: Endpoint configuration missing.";
            logger.error(errorMessage);
            return new GSStatus(false, 400, errorMessage);
        }

        logger.info(`GenericApiDestinationAdapter: Simulating sending batch of ${data.length} items to ${this.config.endpoint}.`);

        // In a real implementation, you would:
        // 1. Construct the API request (e.g., POST request body with 'data' array)
        // 2. Make the HTTP call using a library like axios or node-fetch
        // 3. Handle success/failure responses from the API

        // For simulation, we'll just log and return success
        // Simulating some success and potential failure for demonstration
        const successfulItems = data.filter(item => item.id.includes('success')).length;
        const failedItems = data.length - successfulItems;

        if (failedItems > 0) {
            const errorMessage = `Simulated API batch send failed for ${failedItems} items.`;
            logger.error(errorMessage);
            return new GSStatus(false, 500, errorMessage, { successful: successfulItems, failed: failedItems });
        } else {
            const successMessage = `Successfully simulated sending batch of ${data.length} items to API.`;
            logger.info(successMessage);
            return new GSStatus(true, 200, successMessage, { itemsSent: data.length });
        }
    }

    // Removed the individual 'send' method as 'processData' handles the batch.
}