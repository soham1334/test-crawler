// src/datasources/types/azure-blob-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
import { BlobServiceClient, ContainerClient, BlobItem, StorageSharedKeyCredential } from "@azure/storage-blob"; // Added StorageSharedKeyCredential
import { IngestionData } from '../../functions/ingestion/interfaces'; 

export interface AzureBlobCrawlerConfig {
    connectionString?: string; 
    accountName?: string; 
    accountKey?: string; 
    containerName?: string; // Made optional for SDK flexibility
    prefix?: string; 
    webhookMode?: boolean; 
    webhookBlobUrlPath?: string; 
    webhookEventTypePath?: string; 
}

export default class DataSource extends GSDataSource {
    private blobServiceClient: BlobServiceClient | undefined;
    private containerClient: ContainerClient | undefined;
    public config: AzureBlobCrawlerConfig;
    private isConfiguredAndReady: boolean = false; // NEW: Internal flag for readiness

    constructor(configWrapper: { config: AzureBlobCrawlerConfig } | AzureBlobCrawlerConfig) {
        super(configWrapper);

        const initialConfig = (configWrapper as { config: AzureBlobCrawlerConfig }).config || (configWrapper as AzureBlobCrawlerConfig);

        this.config = {
            webhookMode: false, 
            ...initialConfig,
        } as AzureBlobCrawlerConfig;

        // --- Constructor-level validation (non-fatal for optional configs) ---
        // containerName is essential for any operation, but we'll handle its absence gracefully
        // by setting isConfiguredAndReady to false.
        if (!this.config.containerName) {
            logger.warn("AzureBlobCrawler: 'containerName' is not provided. This crawler will be disabled until configured.");
            // Do NOT throw here. Set isConfiguredAndReady to false in initClient.
        }

        logger.info(`AzureBlobCrawler initialized for container: ${this.config.containerName || 'N/A'} (Webhook Mode: ${this.config.webhookMode})`);
    }

    /**
     * Initializes the Azure Blob Storage client.
     * This method is designed to fail gracefully without throwing fatal errors.
     */
    public async initClient(): Promise<object> {
        logger.info("AzureBlobCrawler: Initializing Azure Blob Storage client...");

        // Check for containerName first, as it's fundamental
        if (!this.config.containerName) {
            logger.error("AzureBlobCrawler: Cannot initialize client. 'containerName' is missing.");
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: "Missing containerName." };
        }

        try {
            if (this.config.connectionString) {
                this.blobServiceClient = BlobServiceClient.fromConnectionString(this.config.connectionString);
                logger.info("AzureBlobCrawler: Initializing BlobServiceClient from connection string.");
            } else if (this.config.accountName && this.config.accountKey) {
                const sharedKeyCredential = new StorageSharedKeyCredential(this.config.accountName, this.config.accountKey);
                const blobServiceUrl = `https://${this.config.accountName}.blob.core.windows.net`;
                this.blobServiceClient = new BlobServiceClient(blobServiceUrl, sharedKeyCredential);
                logger.info(`AzureBlobCrawler: Initializing BlobServiceClient from account name/key for ${this.config.accountName}.`);
            } else {
                // No valid authentication configuration provided
                logger.warn("AzureBlobCrawler: No valid authentication method provided (connectionString or accountName/accountKey). This crawler will be disabled.");
                this.isConfiguredAndReady = false;
                return { status: "disabled", reason: "Missing authentication configuration." };
            }

            this.containerClient = this.blobServiceClient.getContainerClient(this.config.containerName);

            // Optional: Perform a small operation to verify connectivity, but catch errors gracefully
            try {
                await this.containerClient.exists();
                this.isConfiguredAndReady = true; // Mark as ready only if authentication and connectivity succeed
                logger.info(`AzureBlobCrawler client initialized and connected to container '${this.config.containerName}'.`);
                return { status: "connected", container: this.config.containerName };
            } catch (connectivityError: any) {
                logger.error(`AzureBlobCrawler: Failed to verify connectivity to container '${this.config.containerName}': ${connectivityError.message}. This crawler will be disabled.`, { connectivityError });
                this.isConfiguredAndReady = false;
                return { status: "disabled", reason: `Connectivity failed: ${connectivityError.message}` };
            }
        } catch (error: any) {
            logger.error(`AzureBlobCrawler: Failed to initialize Azure Blob Storage client: ${error.message}. This crawler will be disabled.`, { error });
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: `Client initialization failed: ${error.message}` };
        }
    }

    /**
     * Executes the Azure Blob Crawler.
     * This method handles both standard listing/fetching and webhook-triggered ingestion.
     * It checks the internal readiness flag first.
     *
     * @param ctx The Godspeed context.
     * @param initialPayload Optional payload, used for webhook data.
     * @returns A GSStatus containing the ingested data.
     */
    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        // FIX: Check readiness flag first. If not ready, return graceful error immediately.
        if (!this.isConfiguredAndReady || !this.containerClient) {
            logger.warn("AzureBlobCrawler: Attempted to execute but it is not configured or ready. Skipping operation.");
            return new GSStatus(false, 400, "Azure Blob Storage crawler is disabled due to missing or invalid configuration/authentication.");
        }

        const fetchedAt = new Date();
        const ingestionData: IngestionData[] = [];

        // --- Webhook Mode Logic ---
        if (this.config.webhookMode && initialPayload?.webhookPayload) {
            logger.info(`AzureBlobCrawler: Operating in webhook mode.`);
            const rawWebhookPayload = initialPayload.webhookPayload;

            let blobUrl: string | undefined;
            let eventType: string | undefined; 

            try {
                if (this.config.webhookBlobUrlPath) {
                    blobUrl = this.getNestedProperty(rawWebhookPayload, this.config.webhookBlobUrlPath);
                }
                if (this.config.webhookEventTypePath) {
                    eventType = this.getNestedProperty(rawWebhookPayload, this.config.webhookEventTypePath);
                }

                if (!blobUrl) {
                    logger.warn(`AzureBlobCrawler: Webhook payload missing blob URL at path '${this.config.webhookBlobUrlPath}'. Ingesting raw payload.`);
                    ingestionData.push({
                        id: `azure-webhook-raw-${fetchedAt.getTime()}`,
                        content: JSON.stringify(rawWebhookPayload),
                        url: 'N/A',
                        statusCode: 200,
                        fetchedAt: fetchedAt,
                        metadata: {
                            sourceWebhookPayload: rawWebhookPayload,
                            ingestionType: 'raw_webhook_payload_azure',
                            eventType: eventType || 'unknown'
                        }
                    });
                } else {
                    const urlParts = blobUrl.split('/');
                    // Ensure the URL matches the configured account/container if possible
                    // This is a basic check; full validation might involve parsing account name from URL.
                    // Assuming the URL format is https://<accountName>.blob.core.windows.net/<containerName>/<blobName>
                    // The container name is usually the third segment after the protocol and hostname.
                    const containerNameFromUrl = urlParts[3]; // e.g., "https://account.blob.core.windows.net/container/blob.txt" -> "container"
                    const blobName = urlParts.slice(4).join('/'); // The rest is the blob name

                    if (containerNameFromUrl !== this.config.containerName) {
                         logger.warn(`AzureBlobCrawler: Webhook blob URL's container '${containerNameFromUrl}' does not match configured container '${this.config.containerName}'. Skipping.`);
                         return new GSStatus(true, 200, `Webhook blob URL's container '${containerNameFromUrl}' does not match configured container '${this.config.containerName}'. Skipping.`);
                    }

                    logger.info(`AzureBlobCrawler: Processing Azure event '${eventType || 'unknown'}' for blob: '${blobName}'`);

                    if (eventType === 'Microsoft.Storage.BlobCreated' || eventType === 'Microsoft.Storage.BlobRenamed') {
                        try {
                            const blobContent = await this._getBlobContent(this.containerClient, blobName);
                            ingestionData.push({
                                id: blobUrl, 
                                content: blobContent || '',
                                url: blobUrl,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    container: this.config.containerName,
                                    blobName: blobName,
                                    eventType: eventType,
                                    ingestionType: 'azure_blob_created_modified',
                                    sourceWebhookPayload: rawWebhookPayload,
                                }
                            });
                            logger.info(`AzureBlobCrawler: Ingested content for blob '${blobName}'.`);
                        } catch (fetchError: any) {
                            logger.error(`AzureBlobCrawler: Failed to fetch blob '${blobName}': ${fetchError.message}`);
                            ingestionData.push({
                                id: `${blobUrl}-error`,
                                content: `Error fetching blob: ${fetchError.message}`,
                                url: blobUrl,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    container: this.config.containerName,
                                    blobName: blobName,
                                    eventType: eventType,
                                    ingestionType: 'azure_blob_fetch_failed',
                                    error: fetchError.message,
                                    sourceWebhookPayload: rawWebhookPayload
                                }
                            });
                        }
                    } else if (eventType === 'Microsoft.Storage.BlobDeleted') {
                        ingestionData.push({
                            id: blobUrl,
                            content: '', 
                            url: blobUrl,
                            statusCode: 200, 
                            fetchedAt: fetchedAt,
                            metadata: {
                                container: this.config.containerName,
                                blobName: blobName,
                                eventType: eventType,
                                ingestionType: 'azure_blob_removed',
                                sourceWebhookPayload: rawWebhookPayload
                            }
                        });
                        logger.info(`AzureBlobCrawler: Processed deletion event for blob '${blobName}'.`);
                    } else {
                        logger.warn(`AzureBlobCrawler: Unhandled Azure event type '${eventType}' for blob '${blobName}'. Ingesting raw payload.`);
                        ingestionData.push({
                            id: `azure-webhook-raw-${blobName}-${fetchedAt.getTime()}`,
                            content: JSON.stringify(rawWebhookPayload),
                            url: blobUrl,
                            statusCode: 200,
                            fetchedAt: fetchedAt,
                            metadata: {
                                sourceWebhookPayload: rawWebhookPayload,
                                ingestionType: 'raw_webhook_payload_azure_unhandled_event',
                                eventType: eventType || 'unknown',
                                blobName: blobName
                            }
                        });
                    }
                }
            } catch (parseError: any) {
                logger.error(`AzureBlobCrawler: Error parsing Azure webhook payload: ${parseError.message}`, { error: parseError, rawPayload: rawWebhookPayload });
                return new GSStatus(false, 400, `Invalid Azure webhook payload: ${parseError.message}`);
            }

            return new GSStatus(true, 200, "Azure webhook processed successfully.", {
                crawledCount: ingestionData.length,
                data: ingestionData,
            });

        } else {
            // --- Standard Mode Logic (List and Fetch All/Filtered Blobs) ---
            // FIX: Check containerName here for standard mode
            if (!this.config.containerName) {
                logger.error("AzureBlobCrawler: 'containerName' is required for standard (non-webhook) crawling mode.");
                return new GSStatus(false, 400, "Missing required configuration: 'containerName' for standard crawling mode.");
            }

            logger.info(`AzureBlobCrawler: Operating in standard (full scan) mode for container: ${this.config.containerName}.`);
            let totalBlobsListed = 0;

            try {
                for await (const blobItem of this._listBlobs(this.containerClient, this.config.prefix)) { // containerClient is guaranteed to be defined here
                    totalBlobsListed++;
                    if (blobItem.name) {
                        try {
                            const blobContent = await this._getBlobContent(this.containerClient, blobItem.name);
                            ingestionData.push({
                                id: `${this.config.containerName}/${blobItem.name}`,
                                content: blobContent || '',
                                url: this.containerClient.getBlobClient(blobItem.name).url, 
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    container: this.config.containerName,
                                    blobName: blobItem.name,
                                    size: blobItem.properties.contentLength,
                                    eTag: blobItem.properties.etag,
                                    lastModified: blobItem.properties.lastModified?.toISOString(),
                                    ingestionType: 'azure_blob_full_scan'
                                }
                            });
                        } catch (fetchError: any) {
                            logger.warn(`AzureBlobCrawler: Failed to fetch content for blob '${blobItem.name}': ${fetchError.message}`);
                            ingestionData.push({
                                id: `${this.config.containerName}/${blobItem.name}-error`,
                                content: `Error fetching blob: ${fetchError.message}`,
                                url: this.containerClient.getBlobClient(blobItem.name).url,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    container: this.config.containerName,
                                    blobName: blobItem.name,
                                    size: blobItem.properties.contentLength,
                                    eTag: blobItem.properties.etag,
                                    lastModified: blobItem.properties.lastModified?.toISOString(),
                                    ingestionType: 'azure_blob_full_scan_fetch_failed',
                                    error: fetchError.message
                                }
                            });
                        }
                    }
                }

                logger.info(`AzureBlobCrawler: Completed full scan. Listed ${totalBlobsListed} blobs and ingested ${ingestionData.length} items.`);
                return new GSStatus(true, 200, "Azure Blob Storage container scan successful.", {
                    crawledCount: ingestionData.length,
                    data: ingestionData,
                });

            } catch (error: any) {
                logger.error(`AzureBlobCrawler: Failed during standard Azure Blob scan: ${error.message}`, { error });
                return new GSStatus(false, 500, `Azure Blob Storage scan failed: ${error.message}`);
            }
        }
    }

    private getNestedProperty(obj: any, path: string): any | undefined {
        return path.split('.').reduce((acc, part) => {
            if (acc === undefined || acc === null) return undefined;

            const arrayMatch = part.match(/(\w+)\[(\d+)\]/);
            if (arrayMatch) {
                const arrayName = arrayMatch[1];
                const index = parseInt(arrayMatch[2], 10);
                return acc[arrayName]?.[index];
            }
            return acc[part];
        }, obj);
    }

    private async *_listBlobs(containerClient: ContainerClient, prefix?: string): AsyncIterableIterator<BlobItem> {
        for await (const blob of containerClient.listBlobsFlat({ prefix })) {
            yield blob;
        }
    }

    private async _getBlobContent(containerClient: ContainerClient, blobName: string): Promise<string | Buffer | undefined> {
        const blobClient = containerClient.getBlobClient(blobName);
        try {
            const downloadBlockBlobResponse = await blobClient.download();
            if (downloadBlockBlobResponse.readableStreamBody) {
                const stream = downloadBlockBlobResponse.readableStreamBody;
                return new Promise((resolve, reject) => {
                    const chunks: Buffer[] = [];
                    stream.on('data', (chunk) => chunks.push(chunk));
                    stream.on('error', reject);
                    stream.on('end', () => {
                        const buffer = Buffer.concat(chunks);
                        // Attempt to convert to string if it's likely text, otherwise keep as Buffer
                        const contentType = downloadBlockBlobResponse.contentType || '';
                        if (contentType.startsWith('text/') || contentType.includes('json') || contentType.includes('xml')) {
                            resolve(buffer.toString('utf8'));
                        } else {
                            resolve(buffer);
                        }
                    });
                });
            }
            return undefined;
        } catch (error: any) {
            logger.error(`AzureBlobCrawler: Error fetching blob content for '${blobName}': ${error.message}`);
            throw error;
        }
    }
}

const SourceType = 'DS';
const Type = "azure-blob-crawler";
const CONFIG_FILE_NAME = "azure-blob-crawler";
const DEFAULT_CONFIG = {
    containerName: "", // Default to empty, as it's conditionally required
    webhookMode: false,
    webhookBlobUrlPath: "data.url", 
    webhookEventTypePath: "eventType", 
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};
