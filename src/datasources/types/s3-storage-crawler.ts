// src/datasources/types/s3-storage-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
// FIX: Import S3 commands from the AWS SDK directly, as the plugin's client will use them
import { GetObjectCommand, ListObjectsV2Command, ListObjectsV2CommandOutput, GetObjectCommandOutput } from "@aws-sdk/client-s3";
import { Readable } from 'stream';
import { IngestionData } from '../../functions/ingestion/interfaces';

// Define a type for the S3 client provided by Godspeed's AWS plugin
// This is a simplified type, the actual client might have more methods.
interface GodspeedS3Client {
    send: (command: any) => Promise<any>;
    // Add other methods if you need them (e.g., for direct client access)
}

export interface S3StorageCrawlerConfig {
    bucketName: string;
    // FIX: Removed region, accessKeyId, secretAccessKey - these are configured in aws.yaml
    awsServiceInstanceName: string; // NEW: Name of the S3 service instance in aws.yaml (e.g., 's3', 's3_1')
    prefix?: string;
    webhookMode?: boolean;
    webhookObjectKeyPath?: string;
    webhookEventNamePath?: string;
}

export default class DataSource extends GSDataSource {
    // FIX: s3Client will now be retrieved from ctx.datasources, not stored here
    public config: S3StorageCrawlerConfig;

    constructor(configWrapper: { config: S3StorageCrawlerConfig } | S3StorageCrawlerConfig) {
        super(configWrapper);

        const initialConfig = (configWrapper as { config: S3StorageCrawlerConfig }).config || (configWrapper as S3StorageCrawlerConfig);

        this.config = {
            webhookMode: false,
            ...initialConfig,
        } as S3StorageCrawlerConfig;

        if (!this.config.bucketName) {
            throw new Error("S3StorageCrawler: 'bucketName' is required in the configuration.");
        }
        // FIX: awsServiceInstanceName is now required
        if (!this.config.awsServiceInstanceName) {
            throw new Error("S3StorageCrawler: 'awsServiceInstanceName' is required in the configuration. This specifies which S3 client from aws.yaml to use.");
        }

        logger.info(`S3StorageCrawler initialized for bucket: ${this.config.bucketName} using AWS service instance: '${this.config.awsServiceInstanceName}' (Webhook Mode: ${this.config.webhookMode})`);
    }

    /**
     * Initializes the S3 client.
     * This method now primarily logs readiness, as the actual S3 client is managed by the Godspeed AWS plugin.
     * The S3 client instance will be retrieved from ctx.datasources in the execute method.
     */
    public async initClient(): Promise<object> {
        logger.info(`S3StorageCrawler client initialized. Will use AWS S3 service instance '${this.config.awsServiceInstanceName}' from ctx.datasources.`);
        return { status: "ready" };
    }

    /**
     * Executes the S3 Storage Crawler.
     * This method handles both standard listing/fetching and webhook-triggered ingestion.
     * It retrieves the S3 client from ctx.datasources.
     *
     * @param ctx The Godspeed context.
     * @param initialPayload Optional payload, used for webhook data.
     * @returns A GSStatus containing the ingested data.
     */
    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        // FIX: Retrieve the S3 client from ctx.datasources
        const s3Client: GodspeedS3Client | undefined = (ctx.datasources as any)?.aws?.[this.config.awsServiceInstanceName];

        if (!s3Client) {
            const errorMessage = `S3 client '${this.config.awsServiceInstanceName}' not found in ctx.datasources. Ensure @godspeedsystems/plugins-aws-as-datasource is installed and configured in aws.yaml.`;
            logger.error(errorMessage);
            return new GSStatus(false, 500, errorMessage);
        }
        logger.debug(`S3StorageCrawler: Successfully retrieved S3 client instance '${this.config.awsServiceInstanceName}'.`);


        const fetchedAt = new Date();
        const ingestionData: IngestionData[] = [];

        // --- Webhook Mode Logic ---
        if (this.config.webhookMode && initialPayload?.webhookPayload) {
            logger.info(`S3StorageCrawler: Operating in webhook mode.`);
            const rawWebhookPayload = initialPayload.webhookPayload;

            let objectKey: string | undefined;
            let eventName: string | undefined;

            try {
                // Extract object key and event name from the webhook payload
                if (this.config.webhookObjectKeyPath) {
                    objectKey = this.getNestedProperty(rawWebhookPayload, this.config.webhookObjectKeyPath);
                }
                if (this.config.webhookEventNamePath) {
                    eventName = this.getNestedProperty(rawWebhookPayload, this.config.webhookEventNamePath);
                }

                if (!objectKey) {
                    logger.warn(`S3StorageCrawler: Webhook payload missing object key at path '${this.config.webhookObjectKeyPath}'. Ingesting raw payload.`);
                    // Ingest raw payload if object key is not found
                    ingestionData.push({
                        id: `s3-webhook-raw-${fetchedAt.getTime()}`,
                        content: JSON.stringify(rawWebhookPayload),
                        url: 'N/A',
                        statusCode: 200,
                        fetchedAt: fetchedAt,
                        metadata: {
                            sourceWebhookPayload: rawWebhookPayload,
                            ingestionType: 'raw_webhook_payload_s3',
                            eventName: eventName || 'unknown'
                        }
                    });
                } else {
                    logger.info(`S3StorageCrawler: Processing S3 event '${eventName || 'unknown'}' for object: '${objectKey}'`);

                    if (eventName?.startsWith('ObjectCreated') || eventName?.startsWith('ObjectRestore')) {
                        // Handle object creation/modification/restore
                        try {
                            // FIX: Pass s3Client to _getObjectContent
                            const objectContent = await this._getObjectContent(s3Client, this.config.bucketName, objectKey);
                            ingestionData.push({
                                id: `${this.config.bucketName}/${objectKey}`,
                                content: objectContent || '',
                                url: `s3://${this.config.bucketName}/${objectKey}`,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    bucket: this.config.bucketName,
                                    key: objectKey,
                                    eventName: eventName,
                                    ingestionType: 's3_object_created_modified',
                                    sourceWebhookPayload: rawWebhookPayload,
                                }
                            });
                            logger.info(`S3StorageCrawler: Ingested content for object '${objectKey}'.`);
                        } catch (fetchError: any) {
                            logger.error(`S3StorageCrawler: Failed to fetch object '${objectKey}': ${fetchError.message}`);
                            ingestionData.push({
                                id: `${this.config.bucketName}/${objectKey}-error`,
                                content: `Error fetching object: ${fetchError.message}`,
                                url: `s3://${this.config.bucketName}/${objectKey}`,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    bucket: this.config.bucketName,
                                    key: objectKey,
                                    eventName: eventName,
                                    ingestionType: 's3_object_fetch_failed',
                                    error: fetchError.message,
                                    sourceWebhookPayload: rawWebhookPayload
                                }
                            });
                        }
                    } else if (eventName?.startsWith('ObjectRemoved')) {
                        // Handle object deletion
                        ingestionData.push({
                            id: `${this.config.bucketName}/${objectKey}`,
                            content: '', // No content for removed objects
                            url: `s3://${this.config.bucketName}/${objectKey}`,
                            statusCode: 200, // Indicates successful processing of deletion event
                            fetchedAt: fetchedAt,
                            metadata: {
                                bucket: this.config.bucketName,
                                key: objectKey,
                                eventName: eventName,
                                ingestionType: 's3_object_removed',
                                sourceWebhookPayload: rawWebhookPayload
                            }
                        });
                        logger.info(`S3StorageCrawler: Processed removal event for object '${objectKey}'.`);
                    } else {
                        logger.warn(`S3StorageCrawler: Unhandled S3 event type '${eventName}' for object '${objectKey}'. Ingesting raw payload.`);
                        ingestionData.push({
                            id: `s3-webhook-raw-${objectKey}-${fetchedAt.getTime()}`,
                            content: JSON.stringify(rawWebhookPayload),
                            url: `s3://${this.config.bucketName}/${objectKey}`,
                            statusCode: 200,
                            fetchedAt: fetchedAt,
                            metadata: {
                                sourceWebhookPayload: rawWebhookPayload,
                                ingestionType: 'raw_webhook_payload_s3_unhandled_event',
                                eventName: eventName || 'unknown',
                                key: objectKey
                            }
                        });
                    }
                }
            } catch (parseError: any) {
                logger.error(`S3StorageCrawler: Error parsing S3 webhook payload: ${parseError.message}`, { error: parseError, rawPayload: rawWebhookPayload });
                return new GSStatus(false, 400, `Invalid S3 webhook payload: ${parseError.message}`);
            }

            return new GSStatus(true, 200, "S3 webhook processed successfully.", {
                crawledCount: ingestionData.length,
                data: ingestionData,
            });

        } else {
            // --- Standard Mode Logic (List and Fetch All/Filtered Objects) ---
            logger.info(`S3StorageCrawler: Operating in standard (full scan) mode.`);
            let continuationToken: string | undefined;
            let totalObjectsListed = 0;

            try {
                do {
                    // FIX: Pass s3Client to _listObjects
                    const listResult = await this._listObjects(s3Client, this.config.bucketName, this.config.prefix, continuationToken);
                    totalObjectsListed += listResult.Contents?.length || 0;

                    if (listResult.Contents) {
                        for (const s3Object of listResult.Contents) {
                            if (s3Object.Key) {
                                try {
                                    // FIX: Pass s3Client to _getObjectContent
                                    const objectContent = await this._getObjectContent(s3Client, this.config.bucketName, s3Object.Key);
                                    ingestionData.push({
                                        id: `${this.config.bucketName}/${s3Object.Key}`,
                                        content: objectContent || '',
                                        url: `s3://${this.config.bucketName}/${s3Object.Key}`,
                                        statusCode: 200,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            bucket: this.config.bucketName,
                                            key: s3Object.Key,
                                            size: s3Object.Size,
                                            eTag: s3Object.ETag,
                                            lastModified: s3Object.LastModified?.toISOString(),
                                            ingestionType: 's3_full_scan'
                                        }
                                    });
                                } catch (fetchError: any) {
                                    logger.warn(`S3StorageCrawler: Failed to fetch content for object '${s3Object.Key}': ${fetchError.message}`);
                                    ingestionData.push({
                                        id: `${this.config.bucketName}/${s3Object.Key}-error`,
                                        content: `Error fetching object: ${fetchError.message}`,
                                        url: `s3://${this.config.bucketName}/${s3Object.Key}`,
                                        statusCode: 500,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            bucket: this.config.bucketName,
                                            key: s3Object.Key,
                                            size: s3Object.Size,
                                            eTag: s3Object.ETag,
                                            lastModified: s3Object.LastModified?.toISOString(),
                                            ingestionType: 's3_full_scan_fetch_failed',
                                            error: fetchError.message
                                        }
                                    });
                                }
                            }
                        }
                    }
                    continuationToken = listResult.NextContinuationToken;
                } while (continuationToken);

                logger.info(`S3StorageCrawler: Completed full scan. Listed ${totalObjectsListed} objects and ingested ${ingestionData.length} items.`);
                return new GSStatus(true, 200, "S3 bucket scan successful.", {
                    crawledCount: ingestionData.length,
                    data: ingestionData,
                });

            } catch (error: any) {
                logger.error(`S3StorageCrawler: Failed during standard S3 scan: ${error.message}`, { error });
                return new GSStatus(false, 500, `S3 bucket scan failed: ${error.message}`);
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

    // FIX: Added s3Client parameter
    private async _listObjects(s3Client: GodspeedS3Client, bucketName: string, prefix?: string, continuationToken?: string): Promise<ListObjectsV2CommandOutput> {
        const command = new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: prefix,
            ContinuationToken: continuationToken,
        });
        return s3Client.send(command);
    }

    // FIX: Added s3Client parameter
    private async _getObjectContent(s3Client: GodspeedS3Client, bucketName: string, key: string): Promise<string | undefined> {
        const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: key,
        });

        try {
            const response: GetObjectCommandOutput = await s3Client.send(command);
            if (response.Body) {
                const stream = response.Body as Readable;
                return new Promise((resolve, reject) => {
                    const chunks: Buffer[] = [];
                    stream.on('data', (chunk) => chunks.push(chunk));
                    stream.on('error', reject);
                    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
                });
            }
            return undefined;
        } catch (error: any) {
            logger.error(`S3StorageCrawler: Error fetching object content for '${key}': ${error.message}`);
            throw error;
        }
    }
}

const SourceType = 'DS';
const Type = "s3-storage-crawler";
const CONFIG_FILE_NAME = "s3-storage-crawler";
const DEFAULT_CONFIG = {
    bucketName: "",
    awsServiceInstanceName: "", // Now required
    prefix: "",
    webhookMode: false,
    webhookObjectKeyPath: "Records[0].s3.object.key",
    webhookEventNamePath: "Records[0].eventName"
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};
