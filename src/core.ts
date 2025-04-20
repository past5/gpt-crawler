// For more information, see https://crawlee.dev/
import { Configuration, PlaywrightCrawler, downloadListOfUrls, RequestQueue } from "crawlee";
import { readFile, writeFile } from "fs/promises";
import { glob } from "glob";
import { Config, configSchema } from "./config.js";
import { Page } from "playwright";
import { isWithinTokenLimit } from "gpt-tokenizer";
import { PathLike } from "fs";

// Remove global variables to prevent shared state between crawls
export function getPageHtml(page: Page, selector = "body") {
  return page.evaluate((selector) => {
    // Check if the selector is an XPath
    if (selector.startsWith("/")) {
      const elements = document.evaluate(
        selector,
        document,
        null,
        XPathResult.ANY_TYPE,
        null,
      );
      let result = elements.iterateNext();
      return result ? result.textContent || "" : "";
    } else {
      // Handle as a CSS selector
      const el = document.querySelector(selector) as HTMLElement | null;
      return el?.innerText || "";
    }
  }, selector);
}

export async function waitForXPath(page: Page, xpath: string, timeout: number) {
  await page.waitForFunction(
    (xpath) => {
      const elements = document.evaluate(
        xpath,
        document,
        null,
        XPathResult.ANY_TYPE,
        null,
      );
      return elements.iterateNext() !== null;
    },
    xpath,
    { timeout },
  );
}

// Update the crawl function to handle arrays
export async function crawl(config: Config | Config[]) {
  if (Array.isArray(config)) {
    // Process each config in the array
    let rqNo = 0;
    for (const singleConfig of config) {
      const rq = await RequestQueue.open(`rq_${rqNo}`);
      await crawlSingle(singleConfig, rq, true);
      await rq.drop();
      rqNo ++;
    }
  } else {
    // Process a single config
    const rq = await RequestQueue.open();
    await crawlSingle(config, rq);
  }
}

// Extract the single config crawling functionality
async function crawlSingle(config: Config, rq: RequestQueue, isArray: boolean = false ) {
  configSchema.parse(config);

  if (process.env.NO_CRAWL !== "true") {
    // Handle URLs based on whether config.url is a string or array
    if (isArray) {
      // For each URL in the array
      const url = config.url
      // Create a separate counter for this crawl
      let localPageCounter = 0;

      // Create a new crawler instance for each URL to avoid sharing request queues
      const crawler = new PlaywrightCrawler({
        requestQueue: rq,
        // Use the requestHandler to process each of the crawled pages.
        async requestHandler({ request, page, enqueueLinks, log, pushData }) {
          if (config.cookie) {
            // Handle the cookie based on whether it's an array or a single object
            if (Array.isArray(config.cookie)) {
              // If it's an array, add all cookies
              const cookies = config.cookie.map((cookie) => ({
                name: cookie.name,
                value: cookie.value,
                url: request.loadedUrl,
              }));
              await page.context().addCookies(cookies);
            } else {
              // If it's a single object
              const cookie = {
                name: config.cookie.name,
                value: config.cookie.value,
                url: request.loadedUrl,
              };
              await page.context().addCookies([cookie]);
            }
          }

          const title = await page.title();
          localPageCounter++;
          log.info(
            `Crawling: Page ${localPageCounter} / ${config.maxPagesToCrawl} - URL: ${request.loadedUrl}...`,
          );

          // Use custom handling for XPath selector
          if (config.selector) {
            if (config.selector.startsWith("/")) {
              await waitForXPath(
                page,
                config.selector,
                config.waitForSelectorTimeout ?? 1000,
              );
            } else {
              await page.waitForSelector(config.selector, {
                timeout: config.waitForSelectorTimeout ?? 1000,
              });
            }
          }

          const html = await getPageHtml(page, config.selector);

          // Save results as JSON to ./storage/datasets/default
          // Include the source URL in the data to help with file naming later
          await pushData({
            title,
            url: request.loadedUrl,
            html,
            sourceUrl: request.loadedUrl,
          });

          if (config.onVisitPage) {
            await config.onVisitPage({ page, pushData });
          }

          // Extract links from the current page
          // and add them to the crawling queue.
          await enqueueLinks({
            globs:
              typeof config.match === "string"
                ? [config.match]
                : config.match,
          });
        },
        maxRequestsPerCrawl: config.maxPagesToCrawl,
        // Other options remain the same...
        preNavigationHooks: [
          async ({ page, log }) => {
            const RESOURCE_EXCLUSTIONS = config.resourceExclusions ?? [];
            if (RESOURCE_EXCLUSTIONS.length === 0) {
              return;
            }
            await page.route(
              `**\/*.{${RESOURCE_EXCLUSTIONS.join()}}`,
              (route) => route.abort("aborted"),
            );
            log.info(
              `Aborting requests for as this is a resource excluded route`,
            );
          },
        ],
      });

      const SITEMAP_SUFFIX = "sitemap.xml";
      const isUrlASitemap = url.endsWith(SITEMAP_SUFFIX);

      if (isUrlASitemap) {
        const listOfUrls = await downloadListOfUrls({ url });
        await crawler.addRequests(listOfUrls);
        await crawler.run();
      } else {
        // Process single URL
        await crawler.run([url]);
      }
    } else {
      // Local page counter for this specific crawl
      let localPageCounter = 0;

      // Create a new crawler for a single URL configuration
      const crawler = new PlaywrightCrawler({
        // Use the requestHandler to process each of the crawled pages.
        async requestHandler({ request, page, enqueueLinks, log, pushData }) {
          if (config.cookie) {
            // Handle the cookie based on whether it's an array or a single object
            if (Array.isArray(config.cookie)) {
              // If it's an array, add all cookies
              const cookies = config.cookie.map((cookie) => ({
                name: cookie.name,
                value: cookie.value,
                url: request.loadedUrl,
              }));
              await page.context().addCookies(cookies);
            } else {
              // If it's a single object
              const cookie = {
                name: config.cookie.name,
                value: config.cookie.value,
                url: request.loadedUrl,
              };
              await page.context().addCookies([cookie]);
            }
          }

          const title = await page.title();
          localPageCounter++;
          log.info(
            `Crawling: Page ${localPageCounter} / ${config.maxPagesToCrawl} - URL: ${request.loadedUrl}...`,
          );

          // Use custom handling for XPath selector
          if (config.selector) {
            if (config.selector.startsWith("/")) {
              await waitForXPath(
                page,
                config.selector,
                config.waitForSelectorTimeout ?? 1000,
              );
            } else {
              await page.waitForSelector(config.selector, {
                timeout: config.waitForSelectorTimeout ?? 1000,
              });
            }
          }

          const html = await getPageHtml(page, config.selector);

          // Save results as JSON to ./storage/datasets/default
          await pushData({
            title,
            url: request.loadedUrl,
            html,
            sourceUrl: request.loadedUrl,
          });

          if (config.onVisitPage) {
            await config.onVisitPage({ page, pushData });
          }

          // Extract links from the current page
          // and add them to the crawling queue.
          await enqueueLinks({
            globs:
              typeof config.match === "string" ? [config.match] : config.match,
          });
        },
        maxRequestsPerCrawl: config.maxPagesToCrawl,
        preNavigationHooks: [
          async ({ page, log }) => {
            const RESOURCE_EXCLUSTIONS = config.resourceExclusions ?? [];
            if (RESOURCE_EXCLUSTIONS.length === 0) {
              return;
            }
            await page.route(
              `**\/*.{${RESOURCE_EXCLUSTIONS.join()}}`,
              (route) => route.abort("aborted"),
            );
            log.info(
              `Aborting requests for as this is a resource excluded route`,
            );
          },
        ],
      });

      // Original behavior for single URL
      const SITEMAP_SUFFIX = "sitemap.xml";
      const isUrlASitemap = config.url.endsWith(SITEMAP_SUFFIX);

      if (isUrlASitemap) {
        const listOfUrls = await downloadListOfUrls({ url: config.url });
        await crawler.addRequests(listOfUrls);
        await crawler.run();
      } else {
        await crawler.run([config.url]);
      }
    }
  }
}

// The remaining code stays the same...
export async function write(config: Config | Config[]) {
  if (Array.isArray(config)) {
    const results: PathLike[] = [];
    for (const singleConfig of config) {
      const result = await writeSingle(singleConfig);
      results.push(result);
    }
    return results;
  } else {
    return await writeSingle(config);
  }
}

// Extract the single config writing functionality
async function writeSingle(config: Config): Promise<PathLike> {
  // Code remains the same...
  const jsonFiles = await glob("storage/datasets/default/*.json", {
    absolute: true,
  });

  console.log(`Found ${jsonFiles.length} files to combine...`);

  // Group files by the source URL to create separate output files
  const filesByUrl = new Map<string, string[]>();

  // First, categorize the files by their source URL
  for (const file of jsonFiles) {
    try {
      const fileContent = await readFile(file, "utf-8");
      const data = JSON.parse(fileContent);

      // Use either the sourceUrl property or fallback to the url property
      const sourceUrl = data.sourceUrl || data.url;

      if (sourceUrl) {
        // Create a clean filename from the URL
        const urlObj = new URL(sourceUrl);
        const hostname = urlObj.hostname;
        const pathname = urlObj.pathname.replace(/\//g, "_").replace(/^_/, "");
        const cleanUrl = `${hostname}${pathname}`;

        // Use config's outputFileName instead of cleanUrl for the map key
        // This ensures each config gets its own output file
        const configKey = config.outputFileName.replace(/\.json$/, "");

        if (!filesByUrl.has(configKey)) {
          filesByUrl.set(configKey, []);
        }

        filesByUrl.get(configKey)!.push(file);
      } else {
        // If no URL is found, use the config's outputFileName
        const configKey = config.outputFileName.replace(/\.json$/, "");
        if (!filesByUrl.has(configKey)) {
          filesByUrl.set(configKey, []);
        }
        filesByUrl.get(configKey)!.push(file);
      }
    } catch (error) {
      console.error(`Error processing file ${file}:`, error);
      // If there's an error, use the config's outputFileName
      const configKey = config.outputFileName.replace(/\.json$/, "");
      if (!filesByUrl.has(configKey)) {
        filesByUrl.set(configKey, []);
      }
      filesByUrl.get(configKey)!.push(file);
    }
  }

  // Process each URL group
  for (const [urlKey, files] of filesByUrl.entries()) {
    // Create a custom config for this URL group with a unique output filename
    const urlConfig = {
      ...config,
      outputFileName: `${urlKey}.json`,
    };

    console.log(`Processing ${files.length} files for ${urlKey}...`);

    let currentResults: Record<string, any>[] = [];
    let currentSize: number = 0;
    let fileCounter: number = 1;
    const maxBytes: number = urlConfig.maxFileSize
      ? urlConfig.maxFileSize * 1024 * 1024
      : Infinity;

    const getStringByteSize = (str: string): number =>
      Buffer.byteLength(str, "utf-8");

    const nextFileName = (): string =>
      `${urlConfig.outputFileName.replace(/\.json$/, "")}-${fileCounter}.json`;

    const writeBatchToFile = async (): Promise<void> => {
      await writeFile(nextFileName(), JSON.stringify(currentResults, null, 2));
      console.log(`Wrote ${currentResults.length} items to ${nextFileName()}`);
      currentResults = [];
      currentSize = 0;
      fileCounter++;
    };

    let estimatedTokens: number = 0;

    const addContentOrSplit = async (
      data: Record<string, any>,
    ): Promise<void> => {
      const contentString: string = JSON.stringify(data);
      const tokenCount: number | false = isWithinTokenLimit(
        contentString,
        urlConfig.maxTokens || Infinity,
      );

      if (typeof tokenCount === "number") {
        if (estimatedTokens + tokenCount > urlConfig.maxTokens!) {
          if (currentResults.length > 0) {
            await writeBatchToFile();
          }
          estimatedTokens = Math.floor(tokenCount / 2);
          currentResults.push(data);
        } else {
          currentResults.push(data);
          estimatedTokens += tokenCount;
        }
      }

      currentSize += getStringByteSize(contentString);
      if (currentSize > maxBytes) {
        await writeBatchToFile();
      }
    };

    // Process files for this URL group
    for (const file of files) {
      const fileContent = await readFile(file, "utf-8");
      const data: Record<string, any> = JSON.parse(fileContent);
      await addContentOrSplit(data);
    }

    // Check if any remaining data needs to be written to a file.
    if (currentResults.length > 0) {
      await writeBatchToFile();
    }
  }

  // Return the output filename for the crawler class
  return config.outputFileName;
}

// Update the class for handling both single and array configs
class GPTCrawlerCore {
  private config: Config | Config[];

  constructor(config: Config | Config[]) {
    this.config = config;
  }

  async crawl() {
    return await crawl(this.config);
  }

  async write(): Promise<PathLike | PathLike[]> {
    return await write(this.config);
  }
}

export default GPTCrawlerCore;
