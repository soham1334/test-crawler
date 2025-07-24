// C:\Users\SOHAM\Desktop\test godspeed\test-project\src\test\runOrchestratorExample.ts

// --- Core Imports from your SDK ---
import { IngestionOrchestrator, IngestionEvents } from '../functions/ingestion/orchestrator';
import { GenericApiDestinationAdapter } from '../functions/ingestion/GenericApiDestinationAdapter';
import { FileSystemDestinationAdapter } from '../functions/ingestion/FileSystemDestinationAdapter';
// Make sure these imports are correct after changing interfaces.ts
import { IngestionData, IngestionDataTransformer, IngestionTaskDefinition } from '../functions/ingestion/interfaces';
import { GlobalIngestionLifecycleManager } from '../functions/GlobalIngestionLifecycleManager';

// --- Godspeed Core and Data Sources (assuming these paths are correct relative to your project structure) ---
import { DataSource as HttpCrawlerDataSource } from '../datasources/types/http-crawler';
import { DataSource as GitCrawlerDataSource } from '../datasources/types/git-crawler';

// --- Node.js Built-in Modules ---
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import * as yaml from 'js-yaml';

// --- Godspeed Logger ---
import { logger } from '@godspeedsystems/core';


// --- Transformation Functions (Specific to each Godspeed DataSource's output format) ---

const httpCrawlerTransformer: IngestionDataTransformer = (gsData: any): IngestionData[] => {
    if (gsData && Array.isArray(gsData.data)) {
        return gsData.data.map((item: any) => ({
            id: item.url,
            content: item.content,
            metadata: {
                statusCode: item.statusCode,
                responseTimeMs: item.responseTimeMs,
                contentType: item.contentType,
                contentLength: item.contentLength,
                extractedLinks: item.extractedLinks,
            }
        }));
    } else {
        logger.warn("httpCrawlerTransformer: Unexpected data format or no data from HttpCrawlerDataSource.");
        return [];
    }
};

// This transformer now correctly matches `IngestionDataTransformer` because it returns a Promise.
const gitCrawlerTransformer: IngestionDataTransformer = async (gsData: any): Promise<IngestionData[]> => {
    if (!gsData || !gsData.path) {
        logger.warn("gitCrawlerTransformer: No localPath provided by GitCrawlerDataSource.");
        return [];
    }

    const clonedRepoPath = gsData.path;
    const ingestionItems: IngestionData[] = [];

    logger.info(`GitCrawler Transformer: Reading files from cloned path: ${clonedRepoPath}`);

    try {
        const files = await fs.readdir(clonedRepoPath, { withFileTypes: true });

        for (const file of files) {
            if (file.isFile()) {
                const filePath = path.join(clonedRepoPath, file.name);
                let content: string | Buffer;
                let fileContentType: string = 'application/octet-stream';

                // Heuristic: Read as string if likely text, else as buffer
                if (file.name.endsWith('.md') || file.name.endsWith('.txt') || file.name.endsWith('.js') || file.name.endsWith('.json') || file.name.endsWith('.yaml') || file.name.endsWith('.yml')) {
                    content = await fs.readFile(filePath, { encoding: 'utf8' });
                    fileContentType = 'text/plain';
                } else {
                    content = await fs.readFile(filePath); // Read as Buffer
                    if (file.name.endsWith('.png')) fileContentType = 'image/png';
                    else if (file.name.endsWith('.jpg') || file.name.endsWith('.jpeg')) fileContentType = 'image/jpeg';
                    else if (file.name.endsWith('.pdf')) fileContentType = 'application/pdf';
                }

                const stats = await fs.stat(filePath);

                ingestionItems.push({
                    id: `git://${gsData.repoUrl}/${file.name}`,
                    content: content,
                    metadata: {
                        filename: file.name,
                        filepath: filePath,
                        relativePath: path.relative(clonedRepoPath, filePath),
                        size: stats.size,
                        lastModified: stats.mtime,
                        contentType: fileContentType,
                        repoUrl: gsData.repoUrl,
                        branch: gsData.branch
                    }
                });
                logger.debug(`    Added file: ${file.name} (Size: ${stats.size} bytes, Type: ${fileContentType})`);
            }
        }
    } catch (error: any) {
        logger.error(`Error reading files from Git cloned path ${clonedRepoPath}: ${error.message}`, { error });
    }

    return ingestionItems;
};

// --- Main Orchestrator Example Runner ---

async function runOrchestratorExample() {
    console.log("Starting Global Ingestion Lifecycle Manager Example...");

    const projectRoot = path.join(__dirname, '..', '..');

    // --- Configuration Loading (Simulating User Input from YAML) ---
    const httpConfigPath = path.join(projectRoot, 'src', 'datasources', 'http-crawler.yaml');
    let httpCrawlerConfig: any = { startUrl: "https://www.godspeed.systems/", maxDepth: 0, recursiveCrawling: false };
    try {
        const fileContents = await fs.readFile(httpConfigPath, 'utf8');
        const rawConfig = yaml.load(fileContents) as any;
        if (rawConfig && rawConfig.startUrl) { httpCrawlerConfig = rawConfig; }
    } catch (e: any) { logger.error(`Error loading HttpCrawler config: ${e.message}. Using default.`); }

    const gitConfigPath = path.join(projectRoot, 'src', 'datasources', 'git-crawler.yaml');
    let gitCrawlerConfig: any = { repoUrl: "https://github.com/git/hello-world.git", localPath: "./cloned-repos/hello-world-test", depth: 1 };
    try {
        const fileContents = await fs.readFile(gitConfigPath, 'utf8');
        const rawConfig = yaml.load(fileContents) as any;
        if (rawConfig && rawConfig.repoUrl) { gitCrawlerConfig = rawConfig; }
    } catch (e: any) { logger.error(`Error loading GitCrawler config: ${e.message}. Using default.`); }


    // --- Instantiate GlobalIngestionLifecycleManager ---
    const manager = new GlobalIngestionLifecycleManager();

    // --- Register Sources and Destinations with the Manager ---
    manager.registerSource("http-crawler", HttpCrawlerDataSource, httpCrawlerTransformer);
    manager.registerSource("git-crawler", GitCrawlerDataSource, gitCrawlerTransformer);
    manager.registerDestination("api-destination", GenericApiDestinationAdapter);
    manager.registerDestination("file-system-saver", FileSystemDestinationAdapter);

    // --- User listens to events from the Manager's central event bus ---
    const managerEventBus = manager.getEventBus();

    managerEventBus.on(IngestionEvents.DATA_RECEIVED, (data: IngestionData[], taskId: string) => {
        console.log(`\n>>> MANAGER EVENT: DATA_RECEIVED for task '${taskId}' (${data.length} items) <<<`);
        data.forEach(item => {
            console.log(`  - Item ID: ${item.id}`);
            if (item.metadata?.filename) {
                console.log(`    Filename: ${item.metadata.filename}`);
            }
            if (item.metadata?.size) {
                console.log(`    Size: ${item.metadata.size} bytes`);
            }
            if (item.metadata?.contentType) {
                console.log(`    Content Type: ${item.metadata.contentType}`);
            }
        });
    });

    managerEventBus.on(IngestionEvents.TASK_COMPLETED, (status: any, taskId: string) => {
        console.log(`\n>>> MANAGER EVENT: TASK_COMPLETED for task '${taskId}' with message: ${status.message} <<<`);
    });

    managerEventBus.on(IngestionEvents.TASK_FAILED, (status: any, taskId: string) => {
        console.error(`\n>>> MANAGER EVENT: TASK_FAILED for task '${taskId}' with error: ${status.message} <<<`);
        if (status.data?.error) {
            console.error(`    Details: ${status.data.error}`);
        }
    });

    // --- Define Ingestion Tasks (as if loaded from ingestion-tasks.yaml) ---
    // With `id?: string;` in IngestionTaskDefinition, this array definition is now correct.
    const tasksToDefine: IngestionTaskDefinition[] = [
        {
            id: "task-http-to-api",
            name: "HTTP Website Ingestion to API",
            description: "Crawls Godspeed website and sends data to a mock API.",
            source: {
                pluginType: "http-crawler",
                config: httpCrawlerConfig
            },
            destination: {
                pluginType: "api-destination",
                config: { endpointUrl: "https://mock-api.com/web-data", auth: { type: "none" } }
            },
            trigger: { type: "manual" },
            enabled: true
        },
        {
            id: "task-git-to-filesystem",
            name: "Git Repo Ingestion to Filesystem (Cron)",
            description: "Clones a Git repo periodically via cron and saves its files locally.",
            source: {
                pluginType: "git-crawler",
                config: gitCrawlerConfig
            },
            destination: {
                pluginType: "file-system-saver",
                config: { outputPath: path.join(projectRoot, 'ingested_files', 'git_repo_output') }
            },
            trigger: { type: "cron", expression: "*/10 * * * * *" },
            enabled: true
        },
        {
            id: "task-filesystem-no-path",
            name: "HTTP Ingestion (Filesystem NO PATH)",
            description: "Crawls a small website; will attempt file save but log a warning due to missing path.",
            source: {
                pluginType: "http-crawler",
                config: { startUrl: "https://jsonplaceholder.typicode.com/posts/2", maxDepth: 0 }
            },
            destination: {
                pluginType: "file-system-saver",
                config: { }
            },
            trigger: { type: "manual" },
            enabled: true
        },
        {
            id: "task-webhook-trigger",
            name: "Webhook Triggered Task",
            description: "A task that waits for a webhook call.",
            source: {
                pluginType: "http-crawler",
                config: { startUrl: "https://example.com/webhook-test", maxDepth: 0 }
            },
            trigger: { type: "webhook", endpointId: "webhook-task-123" },
            enabled: true
        },
        {
            // No 'id' provided here, manager will auto-generate it.
            name: "Auto-Generated ID Task (Event-Only)",
            description: "A task whose ID is automatically generated, handled only by events.",
            source: {
                pluginType: "http-crawler",
                config: { startUrl: "https://jsonplaceholder.typicode.com/posts/1", maxDepth: 0 }
            },
            trigger: { type: "manual" },
            enabled: true
        }
    ];

    // --- Schedule Tasks ---
    console.log("\n--- Scheduling Tasks ---");
    let autoGeneratedTaskId: string | undefined;
    for (const taskDef of tasksToDefine) {
        try {
            const scheduledTask = await manager.scheduleTask(taskDef);
            console.log(`Scheduled: ${scheduledTask.name} (ID: ${scheduledTask.id}) - Status: ${scheduledTask.currentStatus}`);
            // Check if ID was originally provided by the definition or auto-generated
            if (!taskDef.id) { // This checks if the *original definition* had an ID
                console.log(`  (Auto-generated ID for "${scheduledTask.name}")`);
                autoGeneratedTaskId = scheduledTask.id;
            }
        } catch (error: any) {
            console.error(`Failed to schedule task ${taskDef.name}: ${error.message}`);
        }
    }

    // --- Start the Manager ---
    manager.start();
    console.log("\nGlobalIngestionLifecycleManager has started. Cron tasks are running.");

    // --- Demonstrate Manual Triggering ---
    console.log("\n--- Demonstrating Manual Trigger for 'task-http-to-api' ---");
    const manualResult = await manager.triggerManualTask("task-http-to-api");
    console.log(`Manual trigger result: Success: ${manualResult.success}, Message: ${manualResult.message}`);

    // Demonstrate manual trigger for auto-generated ID task (event-only)
    if (autoGeneratedTaskId) {
        console.log(`\n--- Demonstrating Manual Trigger for Auto-Generated ID Task (ID: ${autoGeneratedTaskId}) ---`);
        const autoManualResult = await manager.triggerManualTask(autoGeneratedTaskId);
        console.log(`Auto-ID task trigger result: Success: ${autoManualResult.success}, Message: ${autoManualResult.message}`);
    }


    // --- Demonstrate Manual Triggering for the new task ---
    console.log("\n--- Demonstrating Manual Trigger for 'task-filesystem-no-path' (expecting warning) ---");
    const noPathResult = await manager.triggerManualTask("task-filesystem-no-path");
    console.log(`'task-filesystem-no-path' trigger result: Success: ${noPathResult.success}, Message: ${noPathResult.message}`);


    // --- Demonstrate Webhook Triggering (simulated) ---
    console.log("\n--- Demonstrating Webhook Trigger for 'task-webhook-trigger' (simulated) ---");
    setTimeout(async () => {
        console.log("\nSimulating webhook call for 'task-webhook-trigger'...");
        const webhookResult = await manager.triggerWebhookTask("webhook-task-123", { event: "file_ready", path: "/new-file.txt" });
        console.log(`Webhook trigger result: Success: ${webhookResult.success}, Message: ${webhookResult.message}`);
    }, 8000);


    // Keep the process alive for cron tasks to run for a bit
    console.log("\nManager will run for 25 seconds to show cron tasks. Press Ctrl+C to stop.");
    setTimeout(async () => {
        console.log("\n--- Listing Tasks after some runs ---");
        manager.listTasks().forEach(task => {
            console.log(`- Task: ${task.name} (ID: ${task.id}) | Status: ${task.currentStatus} | Last Run: ${task.lastRun?.toLocaleTimeString() || 'N/A'}`);
        });

        console.log("\n--- Descheduling 'task-git-to-filesystem' ---");
        await manager.descheduleTask("task-git-to-filesystem");
        console.log(`Status of 'task-git-to-filesystem' after deschedule:`, manager.getTaskStatus("task-git-to-filesystem")?.currentStatus);

        console.log("\n--- Stopping GlobalIngestionLifecycleManager ---");
        manager.stop(); // Clean up all intervals
        console.log("GlobalIngestionLifecycleManager stopped. All timers cleared.");

        console.log("Example finished.");
        process.exit(0); // Exit gracefully
    }, 25000);
}

// Run the example
runOrchestratorExample();