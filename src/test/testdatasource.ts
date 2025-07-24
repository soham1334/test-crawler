import { DataSource, HttpCrawlerConfig } from '../datasources/types/http-crawler';
import { GSContext, logger, GSStatus } from '@godspeedsystems/core';
import * as fs from 'fs';
import * as yaml from 'js-yaml';
import * as path from 'path';

async function runCrawlerManually() {
    console.log("Starting manual HTTP Crawler execution...");

    // Determine the project root dynamically
    // __dirname is `project-root/src/test`
    // path.join(__dirname, '..', '..') goes up two levels to `project-root`
    const projectRoot = path.join(__dirname, '..', '..');

    // 1. Load configuration from YAML file
    const configPath = path.join(projectRoot, 'src', 'datasources', 'http-crawler.yaml');
    console.log("Attempting to load config from:", configPath); // Add this for debugging

    let rawConfig: any;
    try {
        const fileContents = fs.readFileSync(configPath, 'utf8');
        rawConfig = yaml.load(fileContents);
        console.log("Configuration loaded successfully:", rawConfig);
    } catch (e: any) {
        console.error(`Error loading config file ${configPath}: ${e.message}`);
        return;
    }

    // Extract the actual config object from the raw YAML content,
    // ignoring the 'type' field which is for Godspeed's internal loader.
    const crawlerConfig: HttpCrawlerConfig = {
        startUrl: rawConfig.startUrl,
        method: rawConfig.method,
        headers: rawConfig.headers,
        params: rawConfig.params,
        data: rawConfig.data,
        userAgent: rawConfig.userAgent,
        maxDepth: rawConfig.maxDepth,
        sitemapDiscovery: rawConfig.sitemapDiscovery,
        recursiveCrawling: rawConfig.recursiveCrawling,
        followExternalLinks: rawConfig.followExternalLinks,
        urlFilterRegex: rawConfig.urlFilterRegex,
    };

    // 2. Instantiate your DataSource class
    let dataSource: DataSource;
    try {
        // Godspeed passes config as { config: HttpCrawlerConfig }, so we simulate that
        dataSource = new DataSource({ config: crawlerConfig });
        console.log("Datasource instantiated.");
    } catch (e: any) {
        console.error(`Error instantiating datasource: ${e.message}`);
        return;
    }

    // 3. Call initClient()
    try {
        const clientStatus = await dataSource.initClient();
        console.log("Client initialized:", clientStatus);
    } catch (e: any) {
        console.error(`Error during client initialization: ${e.message}`);
        return;
    }

    // 4. Create an empty object and assert it as GSContext.
    const mockContext: GSContext = {} as GSContext;

    // 5. Call execute()
    try {
        console.log("Executing datasource...");
        const result: GSStatus = await dataSource.execute(mockContext);
        console.log("\nExecution Result:");
        if (result.success) {
            console.log("SUCCESS:", result.message);
            console.log("Crawled Count:", result.data.crawledCount);
            console.log("Data:", JSON.stringify(result.data.data, null, 2)); // Uncomment for full data
        } else {
            console.error("FAILURE:", result.message);
            console.error("Error:", result.data.error);
        }
    } catch (e: any) {
        console.error(`An unexpected error occurred during execution: ${e.message}`);
    }

    console.log("Manual HTTP Crawler execution finished.");
}

// Run the function
runCrawlerManually();