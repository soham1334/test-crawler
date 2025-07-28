// src/functions/test-run.ts
// This file initializes and configures the GlobalIngestionLifecycleManager
// with actual data sources, destinations, and scheduled tasks for integration testing.

import { GlobalIngestionLifecycleManager } from './GlobalIngestionLifecycleManager';
import { IngestionTaskStatus, IngestionTaskDefinition, IngestionDataTransformer, IngestionData, IngestionEvents } from './ingestion/interfaces';
import { GSContext } from '@godspeedsystems/core';

// --- Import your actual Source and Destination Plugin Implementations ---
import { DataSource as HttpCrawlerDataSource } from '../datasources/types/http-crawler';
import { DataSource as GitCrawlerDataSource } from '../datasources/types/git-crawler';
import { DataSource as GoogleDriveCrawlerDataSource } from '../datasources/types/gdrive-crawler';
import { FileSystemDestinationAdapter } from './ingestion/FileSystemDestinationAdapter';
import gitcodeMetadataExtractorTransformer from './Transformers/code-metadata-extractor-transformer';
import htmlToPlaintextTransformer from './Transformers/html-to-plaintext-transformer';
import gdriveContentNormalizerTransformer from './Transformers/gdrive-content-normalizer-transformer';
import passthroughTransformer from './Transformers/passthrough-transformer';
// --- Define a simple placeholder Data Transformer ---
// const passthroughTransformer: IngestionDataTransformer = async (rawData: any[], initialPayload?: any): Promise<IngestionData[]> => {
//     console.log(`[PassthroughTransformer] Transforming ${rawData.length} items.`);
//     const fetchedAt = initialPayload?.fetchedAt ? new Date(initialPayload.fetchedAt) : undefined;
    
//     return rawData.map((item, index) => ({
//         id: item.id || `transformed-item-${Date.now()}-${index}`,
//         content: typeof item === 'object' ? JSON.stringify(item) : String(item.content || item),
//         metadata: { 
//             originalIndex: index, 
//             initialPayloadReceived: initialPayload ? true : false,
//             filename: item.filename, 
//             relativePath: item.relativePath, 
//             // Ensure filePath is passed for proper extension handling in destination
//             filePath: item.metadata?.filePath || item.relativePath,
//         },
//         fetchedAt: fetchedAt, 
//         url: item.url,
//         statusCode: item.statusCode,
//         ...item
//     }));
// };


// Instantiate your GlobalIngestionLifecycleManager as a singleton
const globalIngestionManager = new GlobalIngestionLifecycleManager();

// Function to perform the setup. This will be called during your Godspeed application's startup.
export async function setupGlobalIngestionManager() {
    console.log("--- Setting up GlobalIngestionLifecycleManager for Integration Test ---");

    // 1. Register your Source Plugins
    globalIngestionManager.registerSource('http-crawler', HttpCrawlerDataSource, htmlToPlaintextTransformer);
    console.log("Registered 'http-crawler' source.");

    globalIngestionManager.registerSource('git-crawler', GitCrawlerDataSource, gitcodeMetadataExtractorTransformer);
    console.log("Registered 'git-crawler' source.");

    globalIngestionManager.registerSource('gdrive-crawler', GoogleDriveCrawlerDataSource,passthroughTransformer);
    console.log("Registered 'git-crawler' source.");

    // 2. Register your Destination Plugins
    globalIngestionManager.registerDestination('file-system-destination', FileSystemDestinationAdapter);
    console.log("Registered 'file-system-destination' destination.");

    // 3. Schedule the Ingestion Task Definitions
    const googleDriveCrawlTask: IngestionTaskDefinition = {
    id: 'my-google-drive-crawl-task', // A unique identifier for this specific task
    name: 'Google Drive Data Ingestion',
    enabled: true, // Set to 'true' to enable this task for scheduling or manual triggering

    source: {
        pluginType: 'gdrive-crawler', // This must match the 'type' defined in your google-drive-crawler.yaml
        config: {
            // --- AUTHENTICATION (REQUIRED) ---
            // Choose ONE of the following:
            // Option A: Path to Service Account JSON key file (recommended for local dev)
            // serviceAccountKeyPath: process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH,
            // Option B: Entire JSON content as a string (recommended for production/containers)
            serviceAccountKeyPath: process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH,

            // --- USER IMPERSONATION (REQUIRED for Google Workspace Domain-Wide Delegation) ---
            // Provide the email of the user whose Drive you want to crawl.
            // This user must be within your Google Workspace domain, and DWD must be configured.
            userToImpersonateEmail: process.env.GOOGLE_DRIVE_IMPERSONATE_USER_EMAIL,

            // --- CRAWLING SCOPE (REQUIRED) ---
            // The ID of the Google Drive folder to start crawling from.
            // - Use 'root' to crawl the entire accessible Drive of the impersonated user.
            // - Use a specific folder ID (e.g., '1a2b3c4d5e6f7g8h9i0j') from the folder's URL in Google Drive.
            folderId: '1V_5NJVYdZ7HyHxpYBjLs8TIjVc_nWRJ0' // <<< IMPORTANT: REPLACE THIS with your actual folder ID or 'root'
    
            // --- OPTIONAL CRAWLING SETTINGS ---
            // maxFiles: 500, // Limit the number of files fetched per run (useful for testing)
            // includeSharedDrives: false, // Set to true to include Shared Drives
            // includeMimeTypes: "application/vnd.google-apps.document,application/pdf,text/plain", // Only crawl these MIME types
            // excludeMimeTypes: "image/jpeg,image/png", // Exclude these MIME types
            // exportGoogleDocs: true, // Set to true to convert Google Docs/Sheets/Slides to a standard format
            // exportMimeType: "text/plain", // If exportGoogleDocs is true, specify the target MIME type
        }
    },

    destination: {
        pluginType: 'file-system-destination', // Replace with your actual destination plugin type
        config: {
            outputPath: './crawled_output/gdrive-data' // Path where the crawled data will be saved
        }
    },

    trigger: {
        type: 'cron', // Define how this task will be triggered
        expression: '* * * * *' // Example: Run daily at midnight UTC
        // Other trigger types:
        // type: 'manual', // Trigger manually via globalIngestionManager.triggerManualTask()
        // type: 'webhook', endpointId: 'gdrive-change-webhook', // Trigger via an incoming webhook
    },

    currentStatus: IngestionTaskStatus.SCHEDULED, // Initial status of the task
};
    await globalIngestionManager.scheduleTask(googleDriveCrawlTask);
    console.log(`Scheduled task: '${ googleDriveCrawlTask.id}' to run every 2 minutes.`);

    // --- CRON TASKS ---
    // Cron HTTP crawling task
    // const dailyCrawlTask: IngestionTaskDefinition = {
    //     id: 'cron-http-crawl-daily',
    //     name: 'Daily Godspeed Website Crawl (Cron)',
    //     enabled: true,
    //     source: {
    //         pluginType: 'http-crawler',
    //         config: {
    //             startUrl: 'https://www.godspeed.systems/docs',
    //             maxDepth: 2, // Keep depth low for quick testing
    //             recursiveCrawling: true,
    //             sitemapDiscovery: false,
    //         }
    //     },
    //     destination: {
    //         pluginType: 'file-system-destination',
    //         config: { outputPath: './crawled_output/cron-http' }
    //     },
    //     trigger: {
    //         type: 'cron',
    //         expression: '* * * * *' 
    //     },
    //     currentStatus: IngestionTaskStatus.SCHEDULED,
    // };
    // await globalIngestionManager.scheduleTask(dailyCrawlTask);
    // console.log(`Scheduled task: '${dailyCrawlTask.id}' to run every 2 minutes.`);

    // Cron Git cloning task
    // const dailyGitCloneTask: IngestionTaskDefinition = {
    //     id: 'cron-git-clone-daily',
    //     name: 'Daily Git Repository Clone (Cron)',
    //     enabled: true,
    //     source: {
    //         pluginType: 'git-crawler',
    //         config: {
    //             repoUrl: 'https://github.com/soham1334/Txt-to-Speech', // Example repository
    //             localPath: './cloned_repos/Txt-to-Speech-Cron', // Separate path for cron clones
    //             branch: 'main',
    //             depth: 1,
    //         }
    //     },
    //     destination: {
    //         pluginType: 'file-system-destination',
    //         config: { outputPath: './crawled_output/cron-git' }
    //     },
    //     trigger: {
    //         type: 'cron',
    //         expression: '*/3 * * * *' // Every 3 minutes
    //     },
    //     currentStatus: IngestionTaskStatus.SCHEDULED,
    // };
    // await globalIngestionManager.scheduleTask(dailyGitCloneTask);
    // console.log(`Scheduled task: '${dailyGitCloneTask.id}' to run every 3 minutes.`);


    // --- WEBHOOK TASKS ---
    // Webhook-triggered Git crawling task
    // const webhookGitCrawlTask: IngestionTaskDefinition = {
    //     id: 'webhook-git-crawl', // This ID will be used by triggerIngestionManagerWebhookTasks
    //     name: 'Webhook Triggered Git Crawl',
    //     enabled: true,
    //     source: {
    //         pluginType: 'git-crawler',
    //         config: {
    //             repoUrl: 'https://github.com/soham1334/Txt-to-Speech', // The repo this webhook monitors
    //             localPath: './cloned_repos/Txt-to-Speech-Webhook', // A separate path for webhook clones
    //             branch: 'main',
    //             webhookMode: true, // Enable webhook mode
    //             webhookSecret: 'your_git_webhook_secret' // IMPORTANT: Use a real secret in production
    //         }
    //     },
    //     destination: {
    //         pluginType: 'file-system-destination',
    //         config: { outputPath: './crawled_output/webhook-git' }
    //     },
    //     trigger: {
    //         type: 'webhook',
    //         endpointId: 'github-push-event' // This must match the 'id' in http_events.yaml
    //     },
    //     currentStatus: IngestionTaskStatus.SCHEDULED,
    // };
    // await globalIngestionManager.scheduleTask(webhookGitCrawlTask);
    // console.log(`Scheduled task: '${webhookGitCrawlTask.id}' for webhook trigger 'github-push-event'.`);

    // // Webhook-triggered HTTP crawling task (e.g., for a new article published)
    // const webhookHttpCrawlTask: IngestionTaskDefinition = {
    //     id: 'webhook-http-crawl', // This ID will be used by triggerIngestionManagerWebhookTasks
    //     name: 'Webhook Triggered HTTP Crawl',
    //     enabled: true,
    //     source: {
    //         pluginType: 'http-crawler',
    //         config: {
    //             // In webhook mode, startUrl can be dynamically provided by the webhook payload
    //             // but we can set a fallback or base here if needed.
    //             startUrl: 'https://example.com/default-article', 
    //             webhookMode: true, // Enable webhook mode
    //             webhookUrlPath: 'article.url', // Path in webhook payload to find the URL to crawl
    //         }
    //     },
    //     destination: {
    //         pluginType: 'file-system-destination',
    //         config: { outputPath: './crawled_output/webhook-http' }
    //     },
    //     trigger: {
    //         type: 'webhook',
    //         endpointId: 'http-data-event' // This must match the 'id' in http_events.yaml
    //     },
    //     currentStatus: IngestionTaskStatus.SCHEDULED,
    // };
    // await globalIngestionManager.scheduleTask(webhookHttpCrawlTask);
    // console.log(`Scheduled task: '${webhookHttpCrawlTask.id}' for webhook trigger 'http-data-event'.`);


    // --- Event Listeners for Debugging ---
    globalIngestionManager.getEventBus().on(IngestionEvents.TASK_COMPLETED, (taskId: string, status: IngestionTaskStatus) => {
        console.log(`[Event Listener] Task '${taskId}' completed with status: ${status}`);
    });
    globalIngestionManager.getEventBus().on(IngestionEvents.TASK_FAILED, (taskId: string, error: any) => {
        console.error(`[Event Listener] Task '${taskId}' FAILED:`, error);
    });
    globalIngestionManager.getEventBus().on(IngestionEvents.DATA_TRANSFORMED, (transformedData: IngestionData[], taskId: string) => {
        console.log(`[Event Listener] Task '${taskId}' emitted DATA_TRANSFORMED. Transformed ${transformedData.length} items.`);
        transformedData.slice(0, 2).forEach((item, index) => { // Log a small sample
            console.log(`   Sample Item ${index + 1}: ID='${item.id}', ChangeType='${item.metadata?.changeType || 'N/A'}', ContentLength=${item.content ? String(item.content).length : 0} `);
        });
        if (transformedData.length > 2) {
            console.log(`   ...and ${transformedData.length - 2} more items.`);
        }
    });


    // 4. Initialize and Start the Manager
    await globalIngestionManager.init();
    await globalIngestionManager.start();
    
    console.log("--- GlobalIngestionLifecycleManager setup complete and ready for triggers. ---");
}

// Export the setup function as the default export for Godspeed to run it
export default async function (ctx: GSContext) {
    await setupGlobalIngestionManager();
    return { success: true, message: "Ingestion Manager setup complete." };
}
setupGlobalIngestionManager()
// Export the configured manager instance so other functions can import and use it.
export { globalIngestionManager };
