
// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\datasources\types\git-crawler.ts

import { GSContext, GSDataSource, GSStatus, logger } from "@godspeedsystems/core";
import simpleGit, { SimpleGit, CloneOptions } from "simple-git";

export interface GitCrawlerConfig {
  repoUrl: string;
  localPath: string;
  branch?: string;
  depth?: number;
}

export default class DataSource extends GSDataSource {
  private git: SimpleGit = simpleGit();

  public config: GitCrawlerConfig;

  // IMPORTANT: The 'configWrapper' parameter's type annotation might be misleading
  // about the exact structure Godspeed passes at runtime (it seems to pass flat config).
  // To adhere to "don't change its structure or logic" for this constructor,
  // we assume the 'super' call correctly populates 'this.config' in the parent class.
  constructor(configWrapper: { config: GitCrawlerConfig }) {
    // Call the parent GSDataSource constructor.
    // We are passing the configWrapper as received, assuming the superclass
    // knows how to handle it (either it expects a wrapped config, or it's flexible).
    super(configWrapper); 

    // FIX: The runtime log shows Godspeed passes the flat config object directly as 'configWrapper'.
    // Therefore, 'configWrapper.config' is undefined.
    // We must use 'configWrapper' itself as the source of the initial configuration.
    // This is the most minimal change to make 'repoUrl' available at runtime.
    // We use 'as unknown as GitCrawlerConfig' to tell TypeScript to treat 'configWrapper'
    // as the GitCrawlerConfig type for this assignment, overriding the declared type.
    const initialConfig = configWrapper as unknown as GitCrawlerConfig; 

    // Now, assign 'this.config' by applying defaults to the 'initialConfig'
    // which now correctly holds the actual configuration from Godspeed.
    this.config = {
      branch: "main",
      depth: 1,
      ...initialConfig, // This will now correctly spread the properties from the object Godspeed provides
    } as GitCrawlerConfig;

    if (!this.config.repoUrl) {
      throw new Error("GitCrawler: 'repoUrl' is required in the configuration.");
    }
    if (!this.config.localPath) {
      throw new Error("GitCrawler: 'localPath' is required in the configuration.");
    }

    logger.info(`GitCrawler initialized for repo: ${this.config.repoUrl} at ${this.config.localPath}`);
  }

  public async initClient(): Promise<object> {
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
