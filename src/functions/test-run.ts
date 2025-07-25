// src/functions/test-run.ts
// This file initializes and configures the GlobalIngestionLifecycleManager
// with actual data sources, destinations, and scheduled tasks for integration testing.

import { GlobalIngestionLifecycleManager } from './GlobalIngestionLifecycleManager';
import { IngestionTaskStatus, IngestionTaskDefinition, IngestionDataTransformer, IngestionData, IngestionEvents } from './ingestion/interfaces'; // Import IngestionEvents

// --- Import your actual Source and Destination Plugin Implementations ---
// These are the real classes from your project that the manager will use.
import { DataSource as HttpCrawlerDataSource } from '../datasources/types/http-crawler';
import { FileSystemDestinationAdapter } from './ingestion/FileSystemDestinationAdapter';

// --- Define a simple placeholder Data Transformer ---
// This transformer will just pass data through or add a simple transformation.
const passthroughTransformer: IngestionDataTransformer = async (rawData: any[], initialPayload?: any): Promise<IngestionData[]> => {
    console.log(`[PassthroughTransformer] Transforming ${rawData.length} items.`);
    // FIX: Add debug log for the received initialPayload
    console.log(`[PassthroughTransformer DEBUG] Received initialPayload:`, initialPayload);
    
    // Extract fetchedAt from initialPayload and convert it to a Date object
    const fetchedAt = initialPayload?.fetchedAt ? new Date(initialPayload.fetchedAt) : undefined;
    // FIX: Add debug log for the derived fetchedAt
    console.log(`[PassthroughTransformer DEBUG] Derived fetchedAt:`, fetchedAt);

    return rawData.map((item, index) => ({
        id: item.id || `transformed-item-${Date.now()}-${index}`,
        content: typeof item === 'object' ? JSON.stringify(item) : String(item.content || item),
        metadata: { 
            originalIndex: index, 
            initialPayloadReceived: initialPayload ? true : false,
            filename: item.filename, 
            relativePath: item.relativePath, 
        },
        fetchedAt: fetchedAt, 
        url: item.url,
        statusCode: item.statusCode,
        ...item
    }));
};




// Instantiate your GlobalIngestionLifecycleManager as a singleton
const globalIngestionManager = new GlobalIngestionLifecycleManager();

// Function to perform the setup. This will be called during your Godspeed application's startup.
export async function setupGlobalIngestionManager() {
    console.log("--- Setting up GlobalIngestionLifecycleManager for Integration Test ---");

    // 1. Register your Source Plugins
    // Using your HttpCrawlerDataSource
    globalIngestionManager.registerSource('http-crawler', HttpCrawlerDataSource, passthroughTransformer);
    console.log("Registered 'http-crawler' source.");

    // 2. Register your Destination Plugins
    // Using your FileSystemDestinationAdapter
    globalIngestionManager.registerDestination('file-system-destination', FileSystemDestinationAdapter);
    console.log("Registered 'file-system-destination' destination.");

    // 3. Schedule the Ingestion Task Definitions
    // Define a cron-triggered task that uses the registered plugins.
    const dailyCrawlTask: IngestionTaskDefinition = {
        id: 'daily-website-crawl',
        name: 'Daily Godspeed Website Crawl',
        enabled: true,
        source: {
            pluginType: 'http-crawler', // This must match the string used in registerSource
            config: {
                startUrl: 'https://www.godspeed.systems/',
                maxDepth: 3, // Only crawl the start page and direct links
                recursiveCrawling: true, // Enable recursive crawling
                sitemapDiscovery: false, // Don't use sitemap for this test
                userAgent: 'GodspeedIntegrationTestCrawler/1.0'
            }
        },
        destination: {
            pluginType: 'file-system-destination', // This must match the string used in registerDestination
            config: { outputPath: './crawled_output/godspeed-website' } // Output will go here
        },
        trigger: {
            type: 'cron',
            expression: '* * * * *' // Every minute for easy testing
        },
        currentStatus: IngestionTaskStatus.SCHEDULED,
    };
    await globalIngestionManager.scheduleTask(dailyCrawlTask);
    console.log(`Scheduled task: '${dailyCrawlTask.id}' to run every minute.`);

    // FIX: Add event listener to log transformed data
    globalIngestionManager.getEventBus().on(IngestionEvents.DATA_TRANSFORMED, (transformedData: IngestionData[], taskId: string) => {
        console.log(`[Event Listener] Task '${taskId}' emitted DATA_TRANSFORMED. Transformed ${transformedData.length} items:`);
        // Log a sample of the data to avoid overwhelming the console with very large crawls
        transformedData.slice(0, 5).forEach((item, index) => {
            console.log(`  Item ${index + 1}: ID='${item.id}', ContentLength=${item.content ? String(item.content).length : 0} `);
        });
        if (transformedData.length > 5) {
            console.log(`  ...and ${transformedData.length - 5} more items.`);
        }
    });
    console.log("Added listener for DATA_TRANSFORMED events.");


    // 4. Initialize and Start the Manager
    // This prepares the manager to receive execution commands.
    await globalIngestionManager.init();
    await globalIngestionManager.start();

    console.log("--- GlobalIngestionLifecycleManager setup complete and ready for triggers. ---");
}
setupGlobalIngestionManager()
// Export the configured manager instance so other functions can import and use it.
export { globalIngestionManager };
