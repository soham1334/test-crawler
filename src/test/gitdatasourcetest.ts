// Corrected import statement:
// DataSource is the DEFAULT export, so no curly braces for it.
// GitCrawlerConfig is now a NAMED export, so it needs curly braces.
import DataSource, { GitCrawlerConfig } from '../datasources/types/git-crawler';
import { GSContext, logger, GSStatus } from '@godspeedsystems/core';
import * as fs from 'fs';
import * as yaml from 'js-yaml';
import * as path from 'path';

async function runGitCrawlerManually() {
    console.log("Starting manual Git Crawler execution...");

    const projectRoot = path.join(__dirname, '..', '..');

    const configPath = path.join(projectRoot, 'src', 'datasources', 'git-crawler.yaml');
    console.log("Attempting to load Git config from:", configPath);

    let rawConfig: any;
    try {
        const fileContents = fs.readFileSync(configPath, 'utf8');
        rawConfig = yaml.load(fileContents);
        console.log("Git Configuration loaded successfully:", rawConfig);
    } catch (e: any) {
        console.error(`Error loading Git config file ${configPath}: ${e.message}`);
        console.error("Please ensure src/datasources/git-crawler.yaml exists and is accessible.");
        return;
    }

    const gitCrawlerConfig: GitCrawlerConfig = {
        repoUrl: rawConfig.repoUrl,
        localPath: rawConfig.localPath,
        branch: rawConfig.branch,
        depth: rawConfig.depth,
    };

    let dataSource: DataSource;
    try {
        dataSource = new DataSource({ config: gitCrawlerConfig });
        console.log("Git Datasource instantiated.");
    } catch (e: any) {
        console.error(`Error instantiating Git datasource: ${e.message}`);
        return;
    }

    try {
        const clientStatus = await dataSource.initClient();
        console.log("Git Client initialized:", clientStatus);
    } catch (e: any) {
        console.error(`Error during Git client initialization: ${e.message}`);
        return;
    }

    const mockContext: GSContext = {} as GSContext;

    try {
        console.log("Executing Git datasource...");
        const result: GSStatus = await dataSource.execute(mockContext);
        console.log("\nGit Execution Result:");
        if (result.success) {
            console.log("SUCCESS:", result.message);
            console.log("Cloned Path:", result.data.path);
            console.log("Cloned Branch:", result.data.branch);
            console.log("Repository URL:", result.data.repoUrl);
        } else {
            console.error("FAILURE:", result.message);
            console.error("Error:", result.data.error);
        }
    } catch (e: any) {
        console.error(`An unexpected error occurred during Git execution: ${e.message}`);
    }

    console.log("Manual Git Crawler execution finished.");
}

runGitCrawlerManually();