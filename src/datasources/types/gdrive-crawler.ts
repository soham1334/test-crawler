// src/datasources/types/gdrive-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
import { google, drive_v3 } from 'googleapis';
import { JWT } from 'google-auth-library';
import { Readable } from 'stream';
import { IngestionData } from '../../functions/ingestion/interfaces';

// Define the structure for the service account key JSON
interface ServiceAccountKey {
    type?: string;
    project_id?: string;
    private_key_id?: string;
    private_key?: string;
    client_email?: string;
    client_id?: string;
    auth_uri?: string;
    token_uri?: string;
    auth_provider_x509_cert_url?: string;
    client_x509_cert_url?: string;
    universe_domain?: string;
}

export interface GoogleDriveCrawlerConfig {
    folderId?: string; // Made optional for flexibility, especially in webhookMode
    authType?: 'service_account'; // Made optional, defaults to 'service_account'
    serviceAccountKeyPath?: string; 
    serviceAccountKey?: string; 
    webhookMode?: boolean; 
    webhookFileIdPath?: string; 
    webhookChangeTypePath?: string; 
    pageSize?: number; 
}

export default class DataSource extends GSDataSource {
    private driveClient: drive_v3.Drive | undefined;
    private jwtClient: JWT | undefined;
    public config: GoogleDriveCrawlerConfig;
    private isConfiguredAndReady: boolean = false; // NEW: Internal flag for readiness

    constructor(configWrapper: { config: GoogleDriveCrawlerConfig } | GoogleDriveCrawlerConfig) {
        super(configWrapper);

        const initialConfig = (configWrapper as { config: GoogleDriveCrawlerConfig }).config || (configWrapper as GoogleDriveCrawlerConfig);

        this.config = {
            authType: 'service_account', // Default authType
            webhookMode: false,
            pageSize: 100,
            ...initialConfig,
        } as GoogleDriveCrawlerConfig;

        // --- Constructor-level validation (non-fatal for optional configs) ---
        // folderId is required for standard mode, but optional for webhook mode
        if (!this.config.webhookMode && !this.config.folderId) {
            // This is a critical configuration error for standard mode, so it remains fatal.
            // If the user wants a full crawl, they MUST provide a folderId.
            throw new Error("GoogleDriveCrawler: 'folderId' is required in the configuration when not in webhookMode.");
        } else if (this.config.webhookMode && !this.config.folderId) {
            // In webhook mode, folderId is less critical at init if fileId comes from payload.
            logger.warn("GoogleDriveCrawler: 'folderId' is not provided in webhookMode. This crawler will rely solely on webhook payload for file IDs.");
        }

        logger.info(`GoogleDriveCrawler initialized for folder: ${this.config.folderId || 'N/A'} (Webhook Mode: ${this.config.webhookMode})`);
    }

    /**
     * Initializes the Google Drive API client using a Service Account.
     * This method is designed to fail gracefully without throwing fatal errors.
     */
    public async initClient(): Promise<object> {
        logger.info("GoogleDriveCrawler: Initializing Google Drive client...");
        let serviceAccountKey: ServiceAccountKey;

        try {
            // Prioritize serviceAccountKey (from env var) over serviceAccountKeyFile (from path)
            if (this.config.serviceAccountKey) {
                serviceAccountKey = JSON.parse(this.config.serviceAccountKey);
                logger.info("GoogleDriveCrawler: Using service account key from 'serviceAccountKey' config.");
            } else if (this.config.serviceAccountKeyPath) {
                const fs = await import('fs/promises'); 
                const keyFileContent = await fs.readFile(this.config.serviceAccountKeyPath, 'utf8');
                serviceAccountKey = JSON.parse(keyFileContent);
                logger.info(`GoogleDriveCrawler: Using service account key from file: ${this.config.serviceAccountKeyPath}.`);
            } else {
                // No authentication configuration provided
                logger.warn("GoogleDriveCrawler: No 'serviceAccountKey' or 'serviceAccountKeyFile' provided. This crawler will be disabled.");
                this.isConfiguredAndReady = false;
                return { status: "disabled", reason: "Missing service account key configuration." };
            }
        } catch (error: any) {
            logger.error(`GoogleDriveCrawler: Failed to load/parse service account key: ${error.message}. This crawler will be disabled.`, { error });
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: `Invalid service account key: ${error.message}` };
        }

        // Proceed with JWT client authorization only if key was loaded successfully
        if (!serviceAccountKey.client_email || !serviceAccountKey.private_key) {
            logger.error("GoogleDriveCrawler: Service account key is missing 'client_email' or 'private_key'. This crawler will be disabled.");
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: "Incomplete service account key." };
        }

        this.jwtClient = new google.auth.JWT({
            email: serviceAccountKey.client_email,
            key: serviceAccountKey.private_key,
            scopes: ['https://www.googleapis.com/auth/drive.readonly'], 
        });

        try {
            await this.jwtClient.authorize();
            logger.info("GoogleDriveCrawler: JWT client authorized successfully.");
            this.driveClient = google.drive({ version: 'v3', auth: this.jwtClient });
            this.isConfiguredAndReady = true; // Mark as ready only if authentication and client setup succeeds
            logger.info("GoogleDriveCrawler client initialized and ready.");
            return { status: "connected", folderId: this.config.folderId };
        } catch (error: any) {
            logger.error(`GoogleDriveCrawler: Failed to authorize JWT client or create Drive client: ${error.message}. This crawler will be disabled.`, { error });
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: `Authentication failed: ${error.message}` };
        }
    }

    /**
     * Executes the Google Drive Crawler.
     * This method handles both standard listing/fetching and webhook-triggered ingestion.
     * It checks the internal readiness flag first.
     *
     * @param ctx The Godspeed context.
     * @param initialPayload Optional payload, used for webhook data.
     * @returns A GSStatus containing the ingested data.
     */
    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        // FIX: Check readiness flag first. If not ready, return graceful error immediately.
        if (!this.isConfiguredAndReady || !this.driveClient) {
            logger.warn("GoogleDriveCrawler: Attempted to execute but it is not configured or ready. Skipping operation.");
            return new GSStatus(false, 400, "Google Drive crawler is disabled due to missing or invalid authentication configuration.");
        }

        const fetchedAt = new Date();
        const ingestionData: IngestionData[] = [];

        // --- Webhook Mode Logic ---
        if (this.config.webhookMode && initialPayload?.webhookPayload) {
            logger.info(`GoogleDriveCrawler: Operating in webhook mode.`);
            const rawWebhookPayload = initialPayload.webhookPayload;

            let fileId: string | undefined;
            let changeType: string | undefined; 

            try {
                if (this.config.webhookFileIdPath) {
                    fileId = this.getNestedProperty(rawWebhookPayload, this.config.webhookFileIdPath);
                }
                if (this.config.webhookChangeTypePath) {
                    changeType = this.getNestedProperty(rawWebhookPayload, this.config.webhookChangeTypePath);
                }

                if (!fileId) {
                    logger.warn(`GoogleDriveCrawler: Webhook payload missing file ID at path '${this.config.webhookFileIdPath}'. Ingesting raw payload.`);
                    ingestionData.push({
                        id: `gdrive-webhook-raw-${fetchedAt.getTime()}`,
                        content: JSON.stringify(rawWebhookPayload),
                        url: 'N/A',
                        statusCode: 200,
                        fetchedAt: fetchedAt,
                        metadata: {
                            sourceWebhookPayload: rawWebhookPayload,
                            ingestionType: 'raw_webhook_payload_gdrive',
                            changeType: changeType || 'unknown'
                        }
                    });
                } else {
                    logger.info(`GoogleDriveCrawler: Processing Drive event '${changeType || 'unknown'}' for file ID: '${fileId}'`);

                    if (changeType === 'changed' || changeType === 'exists') {
                        try {
                            const fileMetadata = await this.driveClient.files.get({ fileId: fileId, fields: 'id,name,mimeType,webViewLink,webContentLink,size,createdTime,modifiedTime,parents' });
                            const fileName = fileMetadata.data.name || fileId;
                            const mimeType = fileMetadata.data.mimeType || 'application/octet-stream';
                            const fileSize = fileMetadata.data.size ? parseInt(fileMetadata.data.size) : 0;

                            const fileContent = await this._getFileContent(this.driveClient, fileId, mimeType);

                            ingestionData.push({
                                id: fileId,
                                content: fileContent || '',
                                url: fileMetadata.data.webViewLink || fileMetadata.data.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    fileId: fileId,
                                    fileName: fileName,
                                    mimeType: mimeType,
                                    fileSize: fileSize,
                                    changeType: changeType,
                                    ingestionType: 'gdrive_file_created_modified',
                                    sourceWebhookPayload: rawWebhookPayload,
                                    createdTime: fileMetadata.data.createdTime,
                                    modifiedTime: fileMetadata.data.modifiedTime,
                                    parents: fileMetadata.data.parents,
                                }
                            });
                            logger.info(`GoogleDriveCrawler: Ingested content for file '${fileName}' (ID: ${fileId}).`);
                        } catch (fetchError: any) {
                            logger.error(`GoogleDriveCrawler: Failed to fetch file '${fileId}': ${fetchError.message}`);
                            ingestionData.push({
                                id: `${fileId}-error`,
                                content: `Error fetching file: ${fetchError.message}`,
                                url: `https://drive.google.com/file/d/${fileId}/view`,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    fileId: fileId,
                                    changeType: changeType,
                                    ingestionType: 'gdrive_file_fetch_failed',
                                    error: fetchError.message,
                                    sourceWebhookPayload: rawWebhookPayload
                                }
                            });
                        }
                    } else if (changeType === 'not_exists') {
                        ingestionData.push({
                            id: fileId,
                            content: '', 
                            url: `https://drive.google.com/file/d/${fileId}/view`, 
                            statusCode: 200, 
                            fetchedAt: fetchedAt,
                            metadata: {
                                fileId: fileId,
                                changeType: changeType,
                                ingestionType: 'gdrive_file_removed',
                                sourceWebhookPayload: rawWebhookPayload
                            }
                        });
                        logger.info(`GoogleDriveCrawler: Processed removal event for file ID '${fileId}'.`);
                    } else {
                        logger.warn(`GoogleDriveCrawler: Unhandled Drive change type '${changeType}' for file ID '${fileId}'. Ingesting raw payload.`);
                        ingestionData.push({
                            id: `gdrive-webhook-raw-${fileId}-${fetchedAt.getTime()}`,
                            content: JSON.stringify(rawWebhookPayload),
                            url: `https://drive.google.com/file/d/${fileId}/view`,
                            statusCode: 200,
                            fetchedAt: fetchedAt,
                            metadata: {
                                sourceWebhookPayload: rawWebhookPayload,
                                ingestionType: 'raw_webhook_payload_gdrive_unhandled_event',
                                changeType: changeType || 'unknown',
                                fileId: fileId
                            }
                        });
                    }
                }
            } catch (parseError: any) {
                logger.error(`GoogleDriveCrawler: Error parsing Drive webhook payload: ${parseError.message}`, { error: parseError, rawPayload: rawWebhookPayload });
                return new GSStatus(false, 400, `Invalid Drive webhook payload: ${parseError.message}`);
            }

            return new GSStatus(true, 200, "Google Drive webhook processed successfully.", {
                crawledCount: ingestionData.length,
                data: ingestionData,
            });

        } else {
            // --- Standard Mode Logic (List and Fetch All/Filtered Files) ---
            // FIX: Check folderId here for standard mode
            if (!this.config.folderId) {
                logger.error("GoogleDriveCrawler: 'folderId' is required for standard (non-webhook) crawling mode.");
                return new GSStatus(false, 400, "Missing required configuration: 'folderId' for standard crawling mode.");
            }

            logger.info(`GoogleDriveCrawler: Operating in standard (full scan) mode for folder: ${this.config.folderId}.`);
            let pageToken: string | undefined;
            let totalFilesListed = 0;

            try {
                do {
                    const listResult = await this._listFiles(this.driveClient, this.config.folderId, pageToken, this.config.pageSize);
                    totalFilesListed += listResult.files?.length || 0;

                    if (listResult.files) {
                        for (const file of listResult.files) {
                            if (file.id && file.name) {
                                try {
                                    const fileContent = await this._getFileContent(this.driveClient, file.id, file.mimeType || 'application/octet-stream');
                                    ingestionData.push({
                                        id: file.id,
                                        content: fileContent || '',
                                        url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${file.id}/view`,
                                        statusCode: 200,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            fileId: file.id,
                                            fileName: file.name,
                                            mimeType: file.mimeType,
                                            fileSize: file.size ? parseInt(file.size) : 0,
                                            createdTime: file.createdTime,
                                            modifiedTime: file.modifiedTime,
                                            parents: file.parents,
                                            ingestionType: 'gdrive_full_scan'
                                        }
                                    });
                                } catch (fetchError: any) {
                                    logger.warn(`GoogleDriveCrawler: Failed to fetch content for file '${file.name}' (ID: ${file.id}): ${fetchError.message}`);
                                    ingestionData.push({
                                        id: `${file.id}-error`,
                                        content: `Error fetching file: ${fetchError.message}`,
                                        url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${file.id}/view`,
                                        statusCode: 500,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            fileId: file.id,
                                            fileName: file.name,
                                            mimeType: file.mimeType,
                                            ingestionType: 'gdrive_full_scan_fetch_failed',
                                            error: fetchError.message
                                        }
                                    });
                                }
                            }
                        }
                    }
                    pageToken = listResult.nextPageToken || undefined;
                } while (pageToken);

                logger.info(`GoogleDriveCrawler: Completed full scan. Listed ${totalFilesListed} files and ingested ${ingestionData.length} items.`);
                return new GSStatus(true, 200, "Google Drive folder scan successful.", {
                    crawledCount: ingestionData.length,
                    data: ingestionData,
                });

            } catch (error: any) {
                logger.error(`GoogleDriveCrawler: Failed during standard Drive scan: ${error.message}`, { error });
                return new GSStatus(false, 500, `Google Drive folder scan failed: ${error.message}`);
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

    private async _listFiles(driveClient: drive_v3.Drive, folderId: string, pageToken?: string, pageSize?: number): Promise<drive_v3.Schema$FileList> {
        const query = `'${folderId}' in parents and trashed = false`;
        const fields = 'nextPageToken, files(id, name, mimeType, webViewLink, webContentLink, size, createdTime, modifiedTime, parents)';

        const res = await driveClient.files.list({
            q: query,
            fields: fields,
            spaces: 'drive',
            pageToken: pageToken,
            pageSize: pageSize,
        });
        return res.data;
    }

    private async _getFileContent(driveClient: drive_v3.Drive, fileId: string, mimeType: string): Promise<string | Buffer | undefined> {
        let exportMimeType: string | undefined;

        if (mimeType === 'application/vnd.google-apps.document') {
            exportMimeType = 'text/plain'; 
        } else if (mimeType === 'application/vnd.google-apps.spreadsheet') {
            exportMimeType = 'text/csv'; 
        } else if (mimeType === 'application/vnd.google-apps.presentation') {
            exportMimeType = 'application/pdf'; 
        }
        // Add more Google Apps types as needed

        try {
            const res = await driveClient.files.get({
                fileId: fileId,
                alt: 'media', 
                ...(exportMimeType && { mimeType: exportMimeType }), 
            }, { responseType: 'stream' });

            const stream = res.data as Readable;
            return new Promise((resolve, reject) => {
                const chunks: Buffer[] = [];
                stream.on('data', (chunk) => chunks.push(chunk));
                stream.on('error', reject);
                stream.on('end', () => {
                    const buffer = Buffer.concat(chunks);
                    if (exportMimeType?.startsWith('text/') || mimeType.startsWith('text/') || mimeType.includes('json') || mimeType.includes('xml')) {
                        resolve(buffer.toString('utf8'));
                    } else {
                        resolve(buffer);
                    }
                });
            });
        } catch (error: any) {
            logger.error(`GoogleDriveCrawler: Error fetching file content for '${fileId}': ${error.message}`);
            throw error; 
        }
    }
}

const SourceType = 'DS';
const Type = "gdrive-crawler";
const CONFIG_FILE_NAME = "gdrive-crawler";
const DEFAULT_CONFIG = {
    folderId: "", // Default to empty, as it's conditionally required
    authType: 'service_account',
    webhookMode: false,
    webhookFileIdPath: "id",
    webhookChangeTypePath: "resourceState",
    pageSize: 100
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};
