//C:\Users\SOHAM\Desktop\crawler\test-crawler\src\datasources\types\http-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
import axios from "axios";
import * as cheerio from "cheerio";
import { Element } from 'domhandler';


import { parseStringPromise } from "xml2js";
import { URL } from "url"; // Import URL for robust URL handling

export interface HttpCrawlerConfig {
  startUrl: string;
  method?: "GET" | "POST";
  headers?: Record<string, string>;
  params?: Record<string, any>;
  data?: any;
  userAgent?: string;
  maxDepth?: number;
  sitemapDiscovery?: boolean;
  recursiveCrawling?: boolean;
  // Added for better control over what links to follow
  followExternalLinks?: boolean;
  // Added for allowing regex-based URL filtering during recursive crawling
  urlFilterRegex?: string;
}

export class DataSource extends GSDataSource {
  private visited: Set<string>;
  public config: HttpCrawlerConfig; // Explicitly declare 'config' here

  constructor(configWrapper: { config: HttpCrawlerConfig } | HttpCrawlerConfig) { // Allow both wrapped and flat config
    // Call super with the configWrapper. GSDataSource will process it.
    super(configWrapper); 

    // FIX: The runtime log shows Godspeed passes the flat config object directly as 'configWrapper'.
    // Therefore, 'configWrapper.config' is undefined if it's flat.
    // We need to correctly extract the initial config.
    // If configWrapper has a 'config' property, use it; otherwise, assume configWrapper itself is the config.
    const initialConfig = (configWrapper as { config: HttpCrawlerConfig }).config || (configWrapper as HttpCrawlerConfig);

    // Apply defaults and merge, and then ensure 'this.config' holds the final merged config.
    this.config = {
      method: "GET",
      maxDepth: 0,
      sitemapDiscovery: false,
      recursiveCrawling: false,
      followExternalLinks: false,
      ...initialConfig, // Merge provided config over defaults
    } as HttpCrawlerConfig; // Type assertion

    // Now, perform the required check using the fully prepared this.config
    if (!this.config.startUrl) {
      throw new Error("startUrl is required in http-crawler config");
    }

    this.visited = new Set();
  }


  // initClient typically returns a "client" object or a status indicating readiness.
  // For a crawler, "connecting" to the startUrl is a good check.
  async initClient(): Promise<any> {
    logger.info(`Attempting to connect to ${this.config.startUrl}`);
    try {
      await axios.head(this.config.startUrl, {
        headers: {
          'User-Agent': this.config.userAgent || 'Godspeed-HttpCrawler-Init', // Specific User-Agent for init
        },
        validateStatus: (status) => status >= 200 && status < 400, // Consider 2xx and 3xx as successful connection
      });
      logger.info(`Successfully connected to ${this.config.startUrl}`);
      return { status: "connected", url: this.config.startUrl };
    } catch (error: any) {
      logger.error(`Unable to connect to URL: ${this.config.startUrl}. Error: ${error?.message}`);
      // Throwing an error here will prevent the datasource from being loaded,
      // which is appropriate if the initial connection fails.
      throw new Error(`Unable to connect to URL: ${this.config.startUrl}. ${error?.message}`);
    }
  }

  // The execute method will handle the actual crawling logic.
  // It should return a GSStatus object for consistent error/success handling.
  async execute(ctx: GSContext): Promise<GSStatus> {
    logger.info("http-crawler triggered");

    // It's good practice to get the config from `this.config`
    const {
      startUrl,
      sitemapDiscovery,
      recursiveCrawling,
      maxDepth,
      followExternalLinks,
      urlFilterRegex
    } = this.config;

    // We can also extract arguments passed to the execute method from the context.
    // For a crawler, perhaps override values from config dynamically, though not explicitly requested here.
    // const { overrideStartUrl } = ctx.args;
    // const currentStartUrl = overrideStartUrl || startUrl;

    if (!startUrl) {
      return new GSStatus(false, 400, "Missing required configuration: startUrl");
    }

    // Reset visited set for each execution if it's a new crawl.
    // This depends on whether you want a persistent visited set across multiple `execute` calls
    // or if each `execute` call should be a fresh crawl. For a typical "crawl this now" scenario,
    // resetting is usually desired.
    this.visited = new Set();

    const results: any[] = [];
    let regexFilter: RegExp | undefined;
    if (urlFilterRegex) {
        try {
            regexFilter = new RegExp(urlFilterRegex);
        } catch (e: any) { // Explicitly define e as 'any' for simpler error handling
            logger.warn(`Invalid urlFilterRegex provided: ${urlFilterRegex}. It will be ignored. Error: ${e.message}`);
        }
    }


    try {
      if (sitemapDiscovery) {
        logger.info(`Attempting sitemap discovery for ${startUrl}`);
        const sitemapLinks = await this._parseSitemap(startUrl, regexFilter);
        logger.info(`Found ${sitemapLinks.length} links in sitemap for ${startUrl}`);
        for (const link of sitemapLinks) {
          try {
            const page = await this._fetchPage(link);
            results.push(page);
          } catch (error: any) {
            logger.warn(`Failed to fetch page from sitemap: ${link}. Error: ${error.message}`);
          }
        }
      }

      // Ensure recursive crawling only happens if explicitly enabled or sitemapDiscovery is off
      // FIX: Use nullish coalescing to ensure recursiveCrawling is a boolean for the condition
      if (!sitemapDiscovery || (sitemapDiscovery && (recursiveCrawling ?? false))) {
        logger.info(`Starting recursive crawl from ${startUrl} with maxDepth ${maxDepth}`);
        await this._crawl(startUrl, maxDepth ?? 0, results, followExternalLinks ?? false, regexFilter);

      }

      logger.info(`Crawl completed. Total crawled pages: ${results.length}`);
      return new GSStatus(true, 200, "Crawl successful", {
        crawledCount: results.length,
        data: results,
      });

    } catch (err: any) {
      logger.error(`Crawl failed for ${startUrl}. Error: ${err.message}`);
      return new GSStatus(false, 500, `Crawl failed: ${err.message}`, {
        crawledCount: results.length,
        data: results,
        error: err.message,
      });
    }
  }

  // Renamed to `_parseSitemap` to indicate it's a private helper method
  private async _parseSitemap(baseUrl: string, regexFilter?: RegExp): Promise<string[]> {
    try {
      const sitemapUrl = new URL('sitemap.xml', baseUrl).toString(); // Robust URL concatenation
      logger.info(`Fetching sitemap from: ${sitemapUrl}`);
      const res = await axios.get(sitemapUrl, {
        headers: {
          'User-Agent': this.config.userAgent || 'Godspeed-HttpCrawler-Sitemap',
        },
      });
      const parsed = await parseStringPromise(res.data);
      const links = parsed?.urlset?.url?.map((entry: any) => entry.loc[0]) || [];

      // Apply regex filter to sitemap links
      if (regexFilter) {
          const filteredLinks = links.filter((link: string) => regexFilter!.test(link));
          logger.info(`Sitemap: ${filteredLinks.length} links after applying regex filter.`);
          return filteredLinks;
      }
      return links;
    } catch (e: any) {
      logger.warn(`Failed to parse sitemap from ${baseUrl}: ${e.message}`);
      return [];
    }
  }

  // Renamed to `_crawl` to indicate it's a private helper method
  private async _crawl(url: string, depth: number, results: any[], followExternalLinks: boolean, regexFilter?: RegExp) {
    if (this.visited.has(url) || depth < 0) {
      logger.debug(`Skipping ${url}. Visited: ${this.visited.has(url)}, Depth: ${depth}`);
      return;
    }

    // Check if the URL matches the regex filter before fetching
    if (regexFilter && !regexFilter.test(url)) {
        logger.debug(`Skipping ${url}. Does not match URL filter regex.`);
        return;
    }

    this.visited.add(url);
    logger.info(`Crawling: ${url} (Depth: ${depth}, Visited: ${this.visited.size})`);

    const page = await this._fetchPage(url);
    if (page.statusCode >= 200 && page.statusCode < 400) { // Only add successful pages
      results.push(page);
    } else {
      logger.warn(`Failed to fetch ${url}. Status Code: ${page.statusCode}`);
    }

    // FIX: Use nullish coalescing to ensure recursiveCrawling is a boolean
    if (depth === 0 || !(this.config.recursiveCrawling ?? false) || !page.content) {
      logger.debug(`Stopping recursive crawl for ${url}. Depth: ${depth}, Recursive: ${this.config.recursiveCrawling}, Content: ${!!page.content}`);
      return;
    }

    const baseUrl = new URL(url); // Parse the current URL to resolve relative links
    const $ = cheerio.load(page.content);
    const links = $("a[href]")
      .map((_: number, el: Element) => $(el).attr("href"))
      .get()
      .filter((href: string | undefined) => {
        if (typeof href !== "string" || href.trim() === "") return false;

        try {
          // Resolve relative URLs
          const resolvedUrl = new URL(href, baseUrl).toString();

          // Apply followExternalLinks logic
          if (!followExternalLinks) {
            if (new URL(resolvedUrl).hostname !== baseUrl.hostname) {
              return false; // Skip external links if not allowed
            }
          }

          // Apply regex filter to discovered links
          if (regexFilter && !regexFilter.test(resolvedUrl)) {
              return false;
          }

          return true;
        } catch (e: any) { // Explicitly type e as 'any' for simpler error handling
          logger.debug(`Invalid link found: ${href}. Error: ${e.message}`);
          return false; // Ignore malformed URLs
        }
      });


    for (const link of links) {
      try {
        await this._crawl(link, depth - 1, results, followExternalLinks, regexFilter);
      } catch (err: any) {
        logger.debug(`Error crawling link ${link}: ${err.message}`);
        // Ignore broken links or timeout errors silently, as per original logic.
        // The _fetchPage already handles returning a failed status.
      }
    }
  }

  // Renamed to `_fetchPage` to indicate it's a private helper method
  private async _fetchPage(url: string): Promise<any> {
    const start = Date.now();
    let res: any; // Declare res outside try for finally block access if needed

    try {
      res = await axios.get(url, {
        method: this.config.method,
        headers: {
          ...this.config.headers,
          'User-Agent': this.config.userAgent || 'Godspeed-HttpCrawler',
        },
        params: this.config.params,
        data: this.config.data,
        validateStatus: () => true, // Do not throw on HTTP errors, handle them manually
        timeout: 10000, // Added a timeout for robustness (10 seconds)
      });

      const end = Date.now();
      const content = typeof res.data === "string" ? res.data : JSON.stringify(res.data);
      const contentType = res.headers["content-type"] || "unknown";

      const $ = cheerio.load(content);
      // Ensure extracted links are absolute URLs for consistency
      const extractedLinks = $("a[href]")
        .map((_: number, el: Element) => $(el).attr("href"))
        .get()
        .filter((href: string | undefined) => typeof href === "string")
        .map((href: string) => {
          try {
            return new URL(href, url).toString(); // Resolve to absolute URL
          } catch {
            return null; // Malformed URL
          }
        })
        .filter((link: string | null): link is string => link !== null); // Filter out nulls


      return {
        url,
        statusCode: res.status,
        statusText: res.statusText, // Added status text for more context
        responseTimeMs: end - start,
        contentType,
        contentLength: content.length, // Added content length
        content,
        extractedLinks
      };
    } catch (error: any) {
      logger.error(`Error fetching page ${url}: ${error.message}`);
      return {
        url,
        statusCode: res?.status || 0, // Use res.status if available, otherwise 0
        statusText: res?.statusText || 'Error',
        responseTimeMs: Date.now() - start,
        contentType: "unknown",
        contentLength: 0,
        content: "",
        extractedLinks: [],
        fetchError: error.message, // Provide the error message
      };
    }
  }
}

const SourceType = 'DS';
const Type = "http-crawler";
const CONFIG_FILE_NAME = "http-crawler";
const DEFAULT_CONFIG = {};

export {
  SourceType,
  Type,
  CONFIG_FILE_NAME,
  DEFAULT_CONFIG
}
