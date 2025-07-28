//C:\Users\SOHAM\Desktop\crawler\test-crawler\src\datasources\types\http-crawler.ts

import { GSDataSource, GSContext, GSStatus, logger } from "@godspeedsystems/core";
import axios, { AxiosResponse } from "axios"; // KEEP: Direct Axios import, ADDED AxiosResponse
import * as cheerio from "cheerio";
import { Element } from 'domhandler';
import { parseStringPromise } from "xml2js";
import { URL } from "url";
import { IngestionData } from '../../functions/ingestion/interfaces'; 

export interface HttpCrawlerConfig {
    startUrl?: string; 
    method?: "GET" | "POST";
    headers?: Record<string, string>;
    params?: Record<string, any>;
    data?: any;
    userAgent?: string;
    maxDepth?: number;
    sitemapDiscovery?: boolean;
    recursiveCrawling?: boolean;
    followExternalLinks?: boolean;
    urlFilterRegex?: string;
    webhookMode?: boolean; 
    webhookUrlPath?: string; 
}

export default class DataSource extends GSDataSource {
    private visited: Set<string>;
    public config: HttpCrawlerConfig;

    constructor(configWrapper: { config: HttpCrawlerConfig } | HttpCrawlerConfig) {
        super(configWrapper);

        const initialConfig = (configWrapper as { config: HttpCrawlerConfig }).config || (configWrapper as HttpCrawlerConfig);

        this.config = {
            method: "GET",
            maxDepth: 0,
            sitemapDiscovery: false,
            recursiveCrawling: false,
            followExternalLinks: false,
            webhookMode: false, 
            ...initialConfig,
        } as HttpCrawlerConfig;

        if (!this.config.webhookMode && !this.config.startUrl) {
            throw new Error("startUrl is required in http-crawler config when not in webhookMode.");
        }
        
        this.visited = new Set();
        logger.info(`HttpCrawler initialized (Webhook Mode: ${this.config.webhookMode}).`);
    }

    async initClient(): Promise<any> {
        logger.info("HttpCrawler client initialized (ready to make requests).");
        return { status: "connected" };
    }

    async execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        logger.info("http-crawler triggered");
        const fetchedAt = new Date(); 

        if (this.config.webhookMode && initialPayload?.webhookPayload) {
            logger.info(`HttpCrawler: Operating in webhook mode.`);
            const rawWebhookPayload = initialPayload.webhookPayload;
            let urlToCrawl: string | undefined;

            if (this.config.webhookUrlPath) {
                urlToCrawl = this.getNestedProperty(rawWebhookPayload, this.config.webhookUrlPath);
                if (urlToCrawl) {
                    logger.info(`HttpCrawler: Extracted URL '${urlToCrawl}' from webhook payload via path '${this.config.webhookUrlPath}'.`);
                } else {
                    logger.warn(`HttpCrawler: No URL found in webhook payload at path '${this.config.webhookUrlPath}'. Ingesting raw payload.`);
                }
            } else {
                logger.info(`HttpCrawler: No webhookUrlPath configured. Ingesting raw webhook payload.`);
            }

            const results: IngestionData[] = [];

            if (urlToCrawl) {
                try {
                    const page = await this._fetchPage(urlToCrawl); 
                    results.push({
                        id: page.url,
                        content: page.content,
                        url: page.url,
                        statusCode: page.statusCode,
                        fetchedAt: fetchedAt,
                        metadata: { 
                            ...page, 
                            mimeType: page.mimeType, // FIX: Ensure mimeType is explicitly passed to metadata
                            crawledByWebhook: true
                        }
                    });
                    logger.info(`HttpCrawler: Fetched URL from webhook payload: ${urlToCrawl}. Status: ${page.statusCode}`);
                } catch (fetchError: any) {
                    logger.error(`HttpCrawler: Failed to fetch URL '${urlToCrawl}' from webhook payload: ${fetchError.message}`);
                    return new GSStatus(false, 500, `Failed to fetch URL from webhook: ${fetchError.message}`, { error: fetchError.message, url: urlToCrawl });
                }
            } else {
                results.push({
                    id: `webhook-payload-${fetchedAt.getTime()}`, 
                    content: JSON.stringify(rawWebhookPayload),
                    url: 'N/A', 
                    statusCode: 200, 
                    fetchedAt: fetchedAt,
                    metadata: {
                        sourceWebhookPayload: rawWebhookPayload,
                        crawledByWebhook: true,
                        ingestionType: 'raw_webhook_payload',
                        mimeType: 'application/json' // FIX: Explicitly set mimeType for raw JSON payload
                    }
                });
                logger.info(`HttpCrawler: Ingested raw webhook payload directly.`);
            }

            return new GSStatus(true, 200, "Webhook processed successfully.", {
                crawledCount: results.length,
                data: results,
            });

        } else {
            logger.info(`HttpCrawler: Operating in standard (full crawl) mode.`);
            const {
                startUrl,
                sitemapDiscovery,
                recursiveCrawling,
                maxDepth,
                followExternalLinks,
                urlFilterRegex
            } = this.config;

            if (!startUrl) { 
                return new GSStatus(false, 400, "Missing required configuration: startUrl for standard crawling mode.");
            }

            this.visited = new Set();
            const results: IngestionData[] = []; 
            let regexFilter: RegExp | undefined;
            if (urlFilterRegex) {
                try {
                    regexFilter = new RegExp(urlFilterRegex);
                } catch (e: any) {
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
                            results.push({
                                id: page.url,
                                content: page.content,
                                url: page.url,
                                statusCode: page.statusCode,
                                fetchedAt: fetchedAt, 
                                metadata: { 
                                    ...page, 
                                    mimeType: page.mimeType, // FIX: Ensure mimeType is explicitly passed to metadata
                                    crawledBySitemap: true 
                                } 
                            });
                        } catch (error: any) {
                            logger.warn(`Failed to fetch page from sitemap: ${link}. Error: ${error.message}`);
                        }
                    }
                }

                if (!sitemapDiscovery || (sitemapDiscovery && (recursiveCrawling ?? false))) {
                    logger.info(`Starting recursive crawl from ${startUrl} with maxDepth ${maxDepth}`);
                    await this._crawl(startUrl, maxDepth ?? 0, results, followExternalLinks ?? false, fetchedAt, regexFilter); 
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
    }

    private getNestedProperty(obj: any, path: string): any | undefined {
        return path.split('.').reduce((acc, part) => acc && acc[part], obj);
    }

    private async _parseSitemap(baseUrl: string, regexFilter?: RegExp): Promise<string[]> {
        try {
            const sitemapUrl = new URL('sitemap.xml', baseUrl).toString();
            logger.info(`Fetching sitemap from: ${sitemapUrl}`);
            const res: AxiosResponse<string> = await axios.get(sitemapUrl, { // FIX: Explicitly request text response for sitemap
                headers: {
                    'User-Agent': this.config.userAgent || 'Godspeed-HttpCrawler-Sitemap',
                },
                responseType: 'text', // FIX: Ensure response is text for XML parsing
            });
            const parsed = await parseStringPromise(res.data);
            const links = parsed?.urlset?.url?.map((entry: any) => entry.loc[0]) || [];

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

    private async _crawl(url: string, depth: number, results: IngestionData[], followExternalLinks: boolean, fetchedAt: Date, regexFilter?: RegExp) {
        if (this.visited.has(url) || depth < 0) {
            logger.debug(`Skipping ${url}. Visited: ${this.visited.has(url)}, Depth: ${depth}`);
            return;
        }

        if (regexFilter && !regexFilter.test(url)) {
            logger.debug(`Skipping ${url}. Does not match URL filter regex.`);
            return;
        }

        this.visited.add(url);
        logger.info(`Crawling: ${url} (Depth: ${depth}, Visited: ${this.visited.size})`);

        const page = await this._fetchPage(url); 
        if (page.statusCode >= 200 && page.statusCode < 400) {
            results.push({
                id: page.url,
                content: page.content,
                url: page.url,
                statusCode: page.statusCode,
                fetchedAt: fetchedAt, 
                metadata: { 
                    ...page, 
                    mimeType: page.mimeType, // FIX: Ensure mimeType is explicitly passed to metadata
                    crawledByRecursive: true 
                } 
            });
        } else {
            logger.warn(`Failed to fetch ${url}. Status Code: ${page.statusCode}`);
        }

        if (depth === 0 || !(this.config.recursiveCrawling ?? false) || !page.content) {
            logger.debug(`Stopping recursive crawl for ${url}. Depth: ${depth}, Recursive: ${this.config.recursiveCrawling}, Content: ${!!page.content}`);
            return;
        }

        const baseUrl = new URL(url); 
        // FIX: Ensure page.content is a string before loading into cheerio
        const $ = cheerio.load(String(page.content)); 
        
        const links = $("a[href]")
            .map((_: number, el: Element) => $(el).attr("href"))
            .get()
            .map((href: string | undefined) => { 
                if (typeof href !== "string" || href.trim() === "") return null;
                try {
                    const resolvedUrl = new URL(href, baseUrl).toString(); 
                    
                    if (!followExternalLinks && new URL(resolvedUrl).hostname !== baseUrl.hostname) {
                        return null; 
                    }
                    if (regexFilter && !regexFilter.test(resolvedUrl)) {
                        return null; 
                    }
                    return resolvedUrl; 
                } catch (e: any) {
                    logger.debug(`Error resolving/filtering link ${href} against ${baseUrl}: ${e.message}`);
                    return null;
                }
            })
            .filter((link: string | null): link is string => link !== null); 

        logger.info(`_crawl Debug: Extracted and filtered ${links.length} absolute links from ${url}. Sample: ${JSON.stringify(links.slice(0, 5))}`);

        for (const link of links) {
            try {
                await this._crawl(link, depth - 1, results, followExternalLinks, fetchedAt, regexFilter); 
            } catch (err: any) {
                logger.debug(`Error crawling link ${link}: ${err.message}`);
            }
        }
    }

    private async _fetchPage(url: string): Promise<any> {
        const start = Date.now();
        let res: AxiosResponse<string>; // FIX: Explicitly type res as AxiosResponse<string>
        let content: string = ""; // FIX: Initialize content as string
        let mimeType: string = "unknown"; // FIX: Use mimeType directly

        try {
            res = await axios.get(url, { 
                method: this.config.method, 
                headers: {
                    ...this.config.headers, 
                    'User-Agent': this.config.userAgent || 'Godspeed-HttpCrawler', 
                },
                params: this.config.params, 
                data: this.config.data, 
                validateStatus: () => true, 
                timeout: 10000, 
                responseType: 'text' // FIX: Crucial - force Axios to return response data as a string
            });

            const end = Date.now();
            content = res.data; // Now res.data is guaranteed to be a string
            mimeType = res.headers["content-type"] || "text/html"; // FIX: Default to text/html if not specified

            return {
                url,
                statusCode: res.status,
                statusText: res.statusText,
                responseTimeMs: end - start,
                mimeType, // FIX: Changed from contentType to mimeType
                contentLength: content.length,
                content,
                extractedLinks: [] 
            };
        } catch (error: any) {
            logger.error(`Error fetching page ${url}: ${error.message}`);
            return {
                url,
                statusCode: error.response?.status || 0, 
                statusText: error.response?.statusText || 'Error',
                responseTimeMs: Date.now() - start,
                mimeType: "unknown", // FIX: Changed from contentType to mimeType
                contentLength: 0,
                content: "",
                extractedLinks: [],
                fetchError: error.message,
            };
        }
    }
}

const SourceType = 'DS';
const Type = "http-crawler";
const CONFIG_FILE_NAME = "http-crawler";
const DEFAULT_CONFIG = {}; 

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
}
