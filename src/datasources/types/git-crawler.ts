import { GSContext, GSDataSource, GSStatus, logger } from "@godspeedsystems/core";
import simpleGit, { SimpleGit, CloneOptions } from "simple-git";

// Make sure GitCrawlerConfig is EXPORTED so it can be imported elsewhere
export interface GitCrawlerConfig { // <--- Added 'export' here
  repoUrl: string;
  localPath: string;
  branch?: string;
  depth?: number;
}

// DataSource is already exported as a default export
export default class DataSource extends GSDataSource {
  private git: SimpleGit = simpleGit();

  public config: GitCrawlerConfig;

  constructor(configWrapper: { config: GitCrawlerConfig }) {
    super(configWrapper);

    const initialConfig = configWrapper.config;

    this.config = {
      branch: "main",
      depth: 1,
      ...initialConfig,
    } as GitCrawlerConfig;

    if (!this.config.repoUrl) {
      throw new Error("GitCrawler: 'repoUrl' is required in the configuration.");
    }
    if (!this.config.localPath) {
      throw new Error("GitCrawler: 'localPath' is required in the configuration.");
    }

    logger.info(`GitCrawler initialized for repo: ${this.config.repoUrl} at ${this.config.localPath}`);
  }

  public async initClient(): Promise<object> { // Corrected to public in previous step
    logger.info("GitCrawler client initialized (basic check).");
    return { status: "connected" };
  }

  async execute(ctx: GSContext): Promise<GSStatus> {
    const {
      repoUrl,
      localPath,
      branch,
      depth,
    } = this.config;

    logger.info(`Attempting to clone repository: ${repoUrl} to ${localPath} (branch: ${branch}, depth: ${depth})`);

    try {
      const cloneOptions: CloneOptions = {};

      if (branch !== undefined) {
        cloneOptions['--branch'] = branch;
      }
      if (depth !== undefined) {
        cloneOptions['--depth'] = depth;
      }

      await this.git.clone(repoUrl, localPath, cloneOptions);

      logger.info(`Repository cloned successfully: ${repoUrl} to ${localPath}`);
      return new GSStatus(true, 200, "Repository cloned successfully", {
        path: localPath,
        branch: branch,
        repoUrl: repoUrl,
      });
    } catch (error: any) {
      const errMessage = error instanceof Error ? error.message : "Unknown error during Git clone";
      logger.error(`Git clone failed for ${repoUrl}: ${errMessage}`, { error: error });
      return new GSStatus(false, 500, `Git clone failed: ${errMessage}`, {
        repoUrl: repoUrl,
        localPath: localPath,
        error: errMessage,
      });
    }
  }
}

const SourceType = "DS";
const Type = "git-crawler";
const CONFIG_FILE_NAME = "git-crawler";
const DEFAULT_CONFIG = {
  repoUrl: "",
  localPath: "",
  branch: "main",
  depth: 1
};

export {
  DataSource,
  SourceType,
  Type,
  CONFIG_FILE_NAME,
  DEFAULT_CONFIG
};