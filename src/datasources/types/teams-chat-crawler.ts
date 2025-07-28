// src/datasources/types/teams-chat-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
import axios, { AxiosResponse } from 'axios'; // RE-ADDED: Direct Axios import, ADDED AxiosResponse type
import { IngestionData } from '../../functions/ingestion/interfaces';

export interface TeamsChatCrawlerConfig {
    tenantId?: string; 
    clientId?: string; 
    clientSecret?: string; 
    meetingId?: string;
    userIdForMeetings?: string;
    webhookMode?: boolean;
    webhookMeetingIdPath?: string;
    webhookChatIdPath?: string;
}

export default class DataSource extends GSDataSource {
    private accessToken: string | undefined;
    private tokenExpiresAt: number = 0; // Unix timestamp in milliseconds
    public config: TeamsChatCrawlerConfig;
    private isConfiguredAndReady: boolean = false; // Internal flag for readiness

    constructor(configWrapper: { config: TeamsChatCrawlerConfig } | TeamsChatCrawlerConfig) {
        super(configWrapper);

        const initialConfig = (configWrapper as { config: TeamsChatCrawlerConfig }).config || (configWrapper as TeamsChatCrawlerConfig);

        this.config = {
            webhookMode: false,
            ...initialConfig,
        } as TeamsChatCrawlerConfig;

        logger.info(`TeamsChatCrawler initialized for tenant: ${this.config.tenantId || 'N/A'} (Webhook Mode: ${this.config.webhookMode})`);
    }

    /**
     * Initializes the client. Performs configuration validation and sets readiness flag.
     * This method is designed to fail gracefully without throwing fatal errors.
     */
    public async initClient(): Promise<object> {
        logger.info(`TeamsChatCrawler: Initializing client...`);

        // Check for required authentication configurations
        if (!this.config.tenantId || !this.config.clientId || !this.config.clientSecret) {
            logger.error("TeamsChatCrawler: Missing one or more required authentication configurations (tenantId, clientId, clientSecret). This crawler will be disabled.");
            this.isConfiguredAndReady = false;
            return { status: "disabled", reason: "Missing authentication credentials." };
        }

        this.isConfiguredAndReady = true; 
        logger.info(`TeamsChatCrawler client initialized and ready.`);
        return { status: "connected" };
    }

    /**
     * Ensures a valid, non-expired access token is available. Refreshes if needed.
     * (3.3.3.2 Secure authentication)
     */
    private async _ensureAccessToken(): Promise<string> {
        const now = Date.now();
        if (!this.accessToken || this.tokenExpiresAt <= (now + 300000)) {
            logger.info("TeamsChatCrawler: Access token expired or not present. Acquiring new token...");
            try {
                const token = await this._getNewAccessToken(); 
                this.accessToken = token;
                this.tokenExpiresAt = now + (3500 * 1000); 
                logger.info("TeamsChatCrawler: New access token acquired successfully.");
            } catch (error: any) {
                logger.error(`TeamsChatCrawler: Failed to acquire new access token: ${error.message}`, { error });
                throw new Error(`TeamsChatCrawler: Authentication failed during token acquisition: ${error.message}`);
            }
        }
        return this.accessToken!;
    }

    /**
     * Makes the actual request to Azure AD to get a new access token using direct Axios.
     */
    private async _getNewAccessToken(): Promise<string> {
        const tokenEndpoint = `https://login.microsoftonline.com/${this.config.tenantId}/oauth2/v2.0/token`;
        const params = new URLSearchParams();
        params.append('client_id', this.config.clientId!); 
        params.append('client_secret', this.config.clientSecret!); 
        params.append('scope', 'https://graph.microsoft.com/.default');
        params.append('grant_type', 'client_credentials');

        try {
            // FIX: Explicitly type the response from axios.post
            const response: AxiosResponse<any> = await axios.post(tokenEndpoint, params.toString(), {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                validateStatus: (status) => status >= 200 && status < 300 
            });

            if (response.data && response.data.access_token) {
                return response.data.access_token;
            } else {
                const errorMessage = `No access token received. Response: ${JSON.stringify(response.data)}`;
                throw new Error(errorMessage);
            }
        } catch (error: any) {
            logger.error(`TeamsChatCrawler: Error during token request to Azure AD: ${error.message}`, { error: error.response?.data || error.message });
            throw new Error(`Failed to get new access token: ${error.response?.data?.error_description || error.message}`); 
        }
    }

    /**
     * Fetches the chat ID (threadId) associated with a Teams online meeting.
     */
    private async _getMeetingChatId(meetingId: string, userId: string): Promise<string | undefined> { 
        const accessToken = await this._ensureAccessToken(); 
        const graphApiUrl = `https://graph.microsoft.com/v1.0/users/${userId}/onlineMeetings/${meetingId}`;

        try {
            logger.info(`TeamsChatCrawler: Fetching chat ID for meeting '${meetingId}' via user '${userId}'.`);
            // FIX: Explicitly type the response from axios.get
            const response: AxiosResponse<any> = await axios.get(graphApiUrl, {
                headers: { Authorization: `Bearer ${accessToken}` },
                validateStatus: (status) => status >= 200 && status < 300
            });

            if (response.data && response.data.chatInfo && response.data.chatInfo.threadId) {
                logger.info(`TeamsChatCrawler: Found chat ID '${response.data.chatInfo.threadId}' for meeting '${meetingId}'.`);
                return response.data.chatInfo.threadId;
            }
            logger.warn(`TeamsChatCrawler: No chat ID found for meeting '${meetingId}'. Response: ${JSON.stringify(response.data)}`);
            return undefined;
        } catch (error: any) {
            logger.error(`TeamsChatCrawler: Failed to get chat ID for meeting '${meetingId}': ${error.message}`, { error: error.response?.data || error.message });
            throw new Error(`Failed to get chat ID for meeting '${meetingId}': ${error.response?.data?.error_description || error.message}`);
        }
    }

    /**
     * Fetches all chat messages for a given chat ID, handling pagination.
     */
    private async _fetchChatMessages(chatId: string): Promise<any[]> { 
        const accessToken = await this._ensureAccessToken(); 
        let allMessages: any[] = [];
        let nextLink: string | undefined = `https://graph.microsoft.com/v1.0/chats/${chatId}/messages`; 

        logger.info(`TeamsChatCrawler: Fetching chat messages for chat ID: ${chatId}`);

        try {
            while (nextLink) {
                // FIX: Explicitly type the response from axios.get
                const response: AxiosResponse<any> = await axios.get(nextLink, {
                    headers: { Authorization: `Bearer ${accessToken}` },
                    validateStatus: (status) => status >= 200 && status < 300
                });

                if (response.data && Array.isArray(response.data.value)) {
                    allMessages = allMessages.concat(response.data.value);
                    nextLink = response.data['@odata.nextLink']; 
                    logger.debug(`TeamsChatCrawler: Fetched ${response.data.value.length} messages. Total: ${allMessages.length}. Next link: ${nextLink || 'none'}`);
                } else {
                    logger.warn(`TeamsChatCrawler: No messages found or unexpected response for chat '${chatId}'. Response: ${JSON.stringify(response.data)}`);
                    nextLink = undefined;
                }
            }
            logger.info(`TeamsChatCrawler: Finished fetching all ${allMessages.length} messages for chat ID: ${chatId}.`);
            return allMessages;
        } catch (error: any) {
            logger.error(`TeamsChatCrawler: Failed to fetch chat messages for chat ID '${chatId}': ${error.message}`, { error: error.response?.data || error.message });
            throw new Error(`Failed to fetch chat messages for chat ID '${chatId}': ${error.response?.data?.error_description || error.message}`);
        }
    }

    /**
     * Helper to get nested property from an object (e.g., 'data.url')
     * Supports array indexing like 'Records[0]'
     */
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

    /**
     * Executes the Teams Chat Crawler.
     * This method handles both standard fetching by meeting ID and webhook-triggered ingestion.
     * It checks the internal readiness flag first.
     *
     * @param ctx The Godspeed context.
     * @param initialPayload Optional payload, used for webhook data.
     * @returns A GSStatus containing the ingested data.
     */
    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        // Check readiness flag first. If not ready, return graceful error immediately.
        if (!this.isConfiguredAndReady) {
            logger.warn("TeamsChatCrawler: Attempted to execute but it is not configured or ready. Skipping operation.");
            return new GSStatus(false, 400, "TeamsChatCrawler is disabled due to missing configuration/authentication.");
        }

        const fetchedAt = new Date();
        const ingestionData: IngestionData[] = [];
        let targetChatId: string | undefined;

        try {
            // --- Webhook Mode Logic ---
            if (this.config.webhookMode && initialPayload?.webhookPayload) {
                logger.info(`TeamsChatCrawler: Operating in webhook mode.`);
                const rawWebhookPayload = initialPayload.webhookPayload;

                const webhookMeetingId = this.config.webhookMeetingIdPath ? this.getNestedProperty(rawWebhookPayload, this.config.webhookMeetingIdPath) : undefined;
                const webhookChatId = this.config.webhookChatIdPath ? this.getNestedProperty(rawWebhookPayload, this.config.webhookChatIdPath) : undefined;

                if (webhookChatId) {
                    targetChatId = webhookChatId;
                    logger.info(`TeamsChatCrawler: Extracted chat ID '${targetChatId}' directly from webhook payload.`);
                } else if (webhookMeetingId && this.config.userIdForMeetings) {
                    logger.info(`TeamsChatCrawler: Extracted meeting ID '${webhookMeetingId}' from webhook. Attempting to get chat ID.`);
                    targetChatId = await this._getMeetingChatId(webhookMeetingId, this.config.userIdForMeetings); 
                } else {
                    logger.warn(`TeamsChatCrawler: Webhook payload did not contain a usable meeting/chat ID or 'userIdForMeetings' is missing. Ingesting raw payload.`);
                    ingestionData.push({
                        id: `teams-webhook-raw-${fetchedAt.getTime()}`,
                        content: JSON.stringify(rawWebhookPayload),
                        url: 'N/A',
                        statusCode: 200,
                        fetchedAt: fetchedAt,
                        metadata: {
                            sourceWebhookPayload: rawWebhookPayload,
                            ingestionType: 'raw_webhook_payload_teams',
                        }
                    });
                }
            } else {
                // --- Standard Mode Logic ---
                logger.info(`TeamsChatCrawler: Operating in standard mode.`);
                if (!this.config.meetingId || !this.config.userIdForMeetings) {
                    return new GSStatus(false, 400, "TeamsChatCrawler: 'meetingId' and 'userIdForMeetings' are required in standard mode.");
                }
                logger.info(`TeamsChatCrawler: Using configured meeting ID '${this.config.meetingId}' and user ID '${this.config.userIdForMeetings}'.`);
                targetChatId = await this._getMeetingChatId(this.config.meetingId, this.config.userIdForMeetings); 
            }

            if (targetChatId) {
                const messages = await this._fetchChatMessages(targetChatId); 
                messages.forEach(msg => {
                    ingestionData.push({
                        id: `${targetChatId}-${msg.id}`,
                        content: msg.body?.content || '',
                        url: msg.webUrl || `https://teams.microsoft.com/l/chat/${targetChatId}/0?context=%7b%22chatId%22%3a%22${targetChatId}%22%7d`,
                        statusCode: 200,
                        fetchedAt: fetchedAt,
                        metadata: {
                            chatId: targetChatId,
                            messageId: msg.id,
                            createdDateTime: msg.createdDateTime,
                            lastModifiedDateTime: msg.lastModifiedDateTime,
                            messageType: msg.messageType,
                            from: msg.from?.user?.displayName || msg.from?.application?.displayName || 'Unknown',
                            subject: msg.subject,
                            importance: msg.importance,
                            originalMessage: msg
                        }
                    });
                });
                logger.info(`TeamsChatCrawler: Ingested ${ingestionData.length} chat messages for chat ID: ${targetChatId}.`);
            } else if (ingestionData.length === 0) {
                return new GSStatus(true, 200, "TeamsChatCrawler: No chat ID found or no messages to ingest.", { crawledCount: 0 });
            }

            return new GSStatus(true, 200, "Teams chat ingestion successful.", {
                crawledCount: ingestionData.length,
                data: ingestionData,
            });

        } catch (error: any) {
            logger.error(`TeamsChatCrawler: Execution failed: ${error.message}`, { error });
            return new GSStatus(false, 500, `Teams chat ingestion failed: ${error.message}`);
        }
    }
}

const SourceType = 'DS';
const Type = "teams-chat-crawler";
const CONFIG_FILE_NAME = "teams-chat-crawler";
const DEFAULT_CONFIG = {
    tenantId: "", 
    clientId: "",
    clientSecret: "",
    webhookMode: false,
    webhookMeetingIdPath: "data.meetingId",
    webhookChatIdPath: "data.chatId",
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};
