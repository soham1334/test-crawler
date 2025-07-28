// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\datasources\types\git-crawler.ts

import { GSContext, GSDataSource, GSStatus, logger } from "@godspeedsystems/core";
import simpleGit, { SimpleGit, CloneOptions } from "simple-git";
import * as fs from 'fs/promises';
import * as path from 'path';
import { IngestionData } from '../../functions/ingestion/interfaces';

// Define GitHubPushPayload interfaces locally for clarity, or import if it's in interfaces.ts
interface GitHubCommit {
    id: string;
    message: string;
    timestamp: string;
    url: string;
    author: { name: string; email: string; username: string };
    added?: string[];
    removed?: string[];
    modified?: string[];
}

interface GitHubRepository {
    id: number;
    name: string;
    full_name: string;
    html_url: string;
}

interface GitHubPushPayload {
    ref: string;
    before: string;
    after: string;
    repository: GitHubRepository;
    pusher: { name: string; email: string };
    commits: GitHubCommit[];
    head_commit: GitHubCommit;
    // ... many other fields
}

export interface GitCrawlerConfig {
    repoUrl: string;
    localPath: string;
    branch?: string;
    depth?: number;
    webhookMode?: boolean;
    webhookSecret?: string; // Made optional as it might be read from env or simply not used
}

export default class DataSource extends GSDataSource {
    private git: SimpleGit = simpleGit();

    public config: GitCrawlerConfig;

    constructor(configWrapper: { config: GitCrawlerConfig } | GitCrawlerConfig) {
        super(configWrapper);

        let extractedConfig: GitCrawlerConfig;
        if (configWrapper && typeof configWrapper === 'object' && 'config' in configWrapper && typeof (configWrapper as any).config === 'object') {
            extractedConfig = (configWrapper as { config: GitCrawlerConfig }).config;
        } else {
            extractedConfig = configWrapper as GitCrawlerConfig;
        }
            
        this.config = {
            branch: "main",
            depth: 1,
            webhookMode: false,
            ...extractedConfig,
        } as GitCrawlerConfig;

        // These are core configurations. If missing, the crawler cannot function.
        // It's acceptable for these to be fatal errors at startup.
        if (!this.config.repoUrl) {
            throw new Error("GitCrawler: 'repoUrl' is required in the configuration.");
        }
        if (!this.config.localPath) {
            throw new Error("GitCrawler: 'localPath' is required in the configuration.");
        }

        logger.info(`GitCrawler initialized for repo: ${this.config.repoUrl} at ${this.config.localPath} (Webhook Mode: ${this.config.webhookMode})`);
    }

    public async initClient(): Promise<object> {
        // No network calls or authentication checks here.
        // The client is considered ready if the basic config (repoUrl, localPath) is present.
        // Actual git operations and their potential failures are handled in execute().
        logger.info("GitCrawler client initialized (ready).");
        return { status: "connected" };
    }

    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        const fetchedAt = new Date(); 
        const rawWebhookPayload = initialPayload?.webhookPayload;

        if (this.config.webhookMode && rawWebhookPayload) {
            logger.info(`GitCrawler: Operating in webhook mode.`);
            let githubPayload: GitHubPushPayload;

            try {
                githubPayload = rawWebhookPayload as GitHubPushPayload;
                if (!githubPayload.ref || !githubPayload.repository || !Array.isArray(githubPayload.commits)) {
                    throw new Error("Invalid GitHub Push webhook payload structure.");
                }
                logger.info(`GitCrawler: Processing GitHub push for repo '${githubPayload.repository.full_name}', ref: '${githubPayload.ref}', after commit: ${githubPayload.after}`);

                // --- Webhook Secret Validation ---
                // This validation happens here at execution time, not during initClient.
                // If webhookSecret is not provided, validation is skipped, but a warning is logged.
                if (this.config.webhookSecret) {
                    // This part would typically be handled in the Godspeed HTTP event handler
                    // (e.g., triggerIngestionManagerWebhookTasks) before calling execute.
                    // However, if the source itself needs to validate, it can.
                    // For GitHub, the signature is in headers, which are not directly passed to execute here.
                    // Assuming the 'triggerIngestionManagerWebhookTasks' function handles signature validation.
                    logger.debug("GitCrawler: Webhook secret configured. Assuming signature validation handled by webhook trigger function.");
                } else {
                    logger.warn("GitCrawler: webhookMode is true but 'webhookSecret' is not configured. Webhook signature validation will be skipped.");
                }
                // --- END Webhook Secret Validation ---


                await this.ensureLocalRepo(githubPayload.repository.html_url, githubPayload.ref.replace('refs/heads/', ''));

                // Perform git reset --hard to bring local repo to the state of the 'after' commit
                // This will effectively remove deleted files from the local clone.
                await this.git.cwd(this.config.localPath).reset(['--hard', githubPayload.after]);
                logger.info(`GitCrawler: Reset local repo to commit ${githubPayload.after}`);

                const ingestionData: IngestionData[] = [];

                const headCommit = githubPayload.head_commit;
                if (headCommit) {
                    const changedFiles = [
                        ...(headCommit.added || []).map(f => ({ path: f, type: 'added' })),
                        ...(headCommit.modified || []).map(f => ({ path: f, type: 'modified' }))
                    ];

                    for (const fileChange of changedFiles) {
                        const fullPath = path.join(this.config.localPath, fileChange.path);
                        try {
                            const content = await fs.readFile(fullPath, 'utf8');
                            ingestionData.push({
                                id: `${githubPayload.repository.full_name}-${fileChange.path}`,
                                content: content,
                                url: `${githubPayload.repository.html_url}/blob/${githubPayload.ref.replace('refs/heads/', '')}/${fileChange.path}`,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    filePath: fileChange.path,
                                    changeType: fileChange.type,
                                    commitSha: headCommit.id, 
                                    commitMessage: headCommit.message,
                                    repo: githubPayload.repository.full_name,
                                    branch: githubPayload.ref,
                                    pusher: githubPayload.pusher.name,
                                    commitAuthor: headCommit.author.name
                                }
                            });
                        } catch (readError: any) {
                            logger.warn(`GitCrawler: Could not read file ${fullPath} for ingestion (added/modified): ${readError.message}`);
                            ingestionData.push({
                                id: `${githubPayload.repository.full_name}-${fileChange.path}-error`, 
                                content: `Error reading file: ${readError.message}`,
                                url: `${githubPayload.repository.html_url}/blob/${githubPayload.ref.replace('refs/heads/', '')}/${fileChange.path}`,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    filePath: fileChange.path,
                                    changeType: fileChange.type,
                                    commitSha: headCommit.id,
                                    repo: githubPayload.repository.full_name,
                                    branch: githubPayload.ref,
                                    error: readError.message
                                }
                            });
                        }
                    }

                    for (const removedFile of (headCommit.removed || [])) {
                        ingestionData.push({
                            id: `${githubPayload.repository.full_name}-${removedFile}`,
                            content: '', 
                            url: `${githubPayload.repository.html_url}/blob/${githubPayload.ref.replace('refs/heads/', '')}/${removedFile}`,
                            statusCode: 200, 
                            fetchedAt: fetchedAt,
                            metadata: {
                                filePath: removedFile,
                                changeType: 'removed', 
                                commitSha: headCommit.id, 
                                commitMessage: headCommit.message,
                                repo: githubPayload.repository.full_name,
                                branch: githubPayload.ref,
                                pusher: githubPayload.pusher.name,
                                commitAuthor: headCommit.author.name
                            }
                        });
                    }
                } else {
                    logger.warn(`GitCrawler: No head_commit found in GitHub push payload for detailed file changes.`);
                }

                logger.info(`GitCrawler: Processed webhook, generated ${ingestionData.length} IngestionData items.`);
                return new GSStatus(true, 200, "Webhook processed and files ingested.", { data: ingestionData });

            } catch (error: any) {
                logger.error(`GitCrawler: Error processing webhook: ${error.message}`, { error, rawPayload: rawWebhookPayload });
                return new GSStatus(false, 500, `Error processing Git webhook: ${error.message}`);
            }

        } else {
            logger.info(`GitCrawler: Operating in standard (full clone) mode.`);
            const { repoUrl, localPath, branch } = this.config;

            try {
                await this.ensureLocalRepo(repoUrl, branch);

                const allFilesData = await this.readAllFilesFromLocalPath(localPath, repoUrl, fetchedAt);
                logger.info(`GitCrawler: Cloned/Pulled and read ${allFilesData.length} files from ${repoUrl}.`);

                return new GSStatus(true, 200, "Repository cloned/pulled and files read successfully", {
                    path: localPath,
                    branch: branch,
                    repoUrl: repoUrl,
                    data: allFilesData
                });
            } catch (error: any) {
                const errMessage = error instanceof Error ? error.message : "Unknown error during Git operation";
                logger.error(`Git operation failed for ${repoUrl}: ${errMessage}`, { error: error });
                return new GSStatus(false, 500, `Git operation failed: ${errMessage}`, {
                    repoUrl: repoUrl,
                    localPath: localPath,
                    error: errMessage,
                });
            }
        }
    }

    private async ensureLocalRepo(repoUrl: string, branch: string | undefined): Promise<void> {
        const repoExists = await fs.access(this.config.localPath).then(() => true).catch(() => false);
        if (repoExists && (await fs.stat(this.config.localPath)).isDirectory()) {
            try {
                const currentRemote = await this.git.cwd(this.config.localPath).remote(['get-url', 'origin']);
                if (currentRemote && currentRemote.trim() === repoUrl) {
                    logger.info(`GitCrawler: Local repo ${this.config.localPath} exists and matches URL, pulling latest changes.`);
                    await this.git.cwd(this.config.localPath).fetch('origin', branch || 'main');
                } else {
                    logger.warn(`GitCrawler: Local path ${this.config.localPath} exists but is a different repo or not a git repo. Attempting to remove and clone.`);
                    await fs.rm(this.config.localPath, { recursive: true, force: true });
                    await this.cloneRepo(repoUrl, branch);
                }
            } catch (gitError: any) {
                logger.warn(`GitCrawler: Error checking existing repo at ${this.config.localPath}: ${gitError.message}. Attempting to remove and re-clone.`);
                await fs.rm(this.config.localPath, { recursive: true, force: true });
                await this.cloneRepo(repoUrl, branch);
            }
        } else {
            logger.info(`GitCrawler: Local path ${this.config.localPath} does not exist or is not a directory, cloning repo.`);
            await fs.mkdir(this.config.localPath, { recursive: true }); 
            await this.cloneRepo(repoUrl, branch);
        }
    }

    private async cloneRepo(repoUrl: string, branch: string | undefined): Promise<void> {
        const cloneOptions: CloneOptions = {};
        if (branch !== undefined) {
            cloneOptions['--branch'] = branch;
        }
        if (this.config.depth !== undefined) {
            cloneOptions['--depth'] = this.config.depth;
        }
        await this.git.clone(repoUrl, this.config.localPath, cloneOptions);
    }

    private async readAllFilesFromLocalPath(basePath: string, repoUrl: string, fetchedAt: Date): Promise<IngestionData[]> {
        const ingestionData: IngestionData[] = [];
        const files = await this.getFilesRecursive(basePath);

        for (const filePath of files) {
            const fullPath = path.join(basePath, filePath);
            try {
                const content = await fs.readFile(fullPath, 'utf8');
                const relativePath = path.relative(basePath, fullPath);
                ingestionData.push({
                    id: `${repoUrl}-${relativePath}`,
                    content: content,
                    url: `${repoUrl}/blob/${this.config.branch || 'main'}/${relativePath}`, 
                    statusCode: 200,
                    fetchedAt: fetchedAt,
                    metadata: {
                        filePath: relativePath,
                        changeType: 'full_scan', 
                        repo: this.config.repoUrl,
                        branch: this.config.branch
                    }
                });
            } catch (readError: any) {
                logger.warn(`GitCrawler: Could not read file ${fullPath} during full scan: ${readError.message}`);
            }
        }
        return ingestionData;
    }

    private async getFilesRecursive(dir: string): Promise<string[]> {
        let files: string[] = [];
        const entries = await fs.readdir(dir, { withFileTypes: true });

        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.name === '.git' || entry.name.startsWith('.')) {
                continue;
            }

            if (entry.isDirectory()) {
                files = files.concat(await this.getFilesRecursive(fullPath));
            } else {
                files.push(path.relative(this.config.localPath, fullPath)); 
            }
        }
        return files;
    }
}

const SourceType = "DS";
const Type = "git-crawler";
const CONFIG_FILE_NAME = "git-crawler";
const DEFAULT_CONFIG = {
    repoUrl: "",
    localPath: "",
    branch: "main",
    depth: 1,
    webhookMode: false,
    // webhookSecret is intentionally not in DEFAULT_CONFIG as it's sensitive and optional
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};
