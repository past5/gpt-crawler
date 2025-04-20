// Modified core.ts that removes the write function and integrates file writing into the crawl

// For more information, see https://crawlee.dev/
import {
  Configuration,
  PlaywrightCrawler,
  downloadListOfUrls,
  RequestQueue,
} from "crawlee";
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

// Function to generate a clean filename from a URL
function generateFilenameFromUrl(baseFilename: string, url: string): string {
  try {
    const urlObj = new URL(url);
    const hostname = urlObj.hostname;
    // Create a clean pathname for the filename
    const pathname = urlObj.pathname
      .replace(/\//g, "_")
      .replace(/^_/, "")
      .replace(/[^\w\-_.]/g, "_");
    
    return `${baseFilename.replace(/\.json$/, "")}_${hostname}${pathname}.json`;
  } catch (error) {
    // Fallback if URL parsing fails
    const timestamp = new Date().getTime();
    return `${baseFilename.replace(/\.json$/, "")}_${timestamp}.json`;
  }
}

// Create a function to write page data to file
async function writePageToFile(config: Config, data: Record<string, any>): Promise<string> {
  const sourceUrl = data.sourceUrl || data.url;
  const filename = generateFilenameFromUrl(config.outputFileName, sourceUrl);
  
  await writeFile(filename, JSON.stringify(data, null, 2));
  console.log(`Wrote file: ${filename}`);
  
  return filename;
}

// Update the crawl function to handle arrays and write files during crawl
export async function crawl(config: Config | Config[]): Promise<string[]> {
  const outputFiles: string[] = [];

  if (Array.isArray(config)) {
    // Process each config in the array
    let rqNo = 0;
    for (const singleConfig of config) {
      const rq = await RequestQueue.open(`rq_${rqNo}`);
      const files = await crawlSingle(singleConfig, rq, true);
      outputFiles.push(...files);
      await rq.drop();
      rqNo++;
    }
  } else {
    // Process a single config
    const rq = await RequestQueue.open();
    const files = await crawlSingle(config, rq);
    outputFiles.push(...files);
  }
  
  return outputFiles;
}

// Modified crawlSingle function that writes files during crawl
async function crawlSingle(
  config: Config,
  rq: RequestQueue,
  isArray: boolean = false,
): Promise<string[]> {
  configSchema.parse(config);
  const outputFiles: string[] = [];

  if (process.env.NO_CRAWL !== "true") {
    // Handle URLs based on whether config.url is a string or array
    if (isArray) {
      // For each URL in the array
      const url = config.url;
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

          // Prepare page data
          const pageData = {
            title,
            url: request.loadedUrl,
            html,
            sourceUrl: request.loadedUrl,
          };

          // Write the data to a file immediately
          const filename = await writePageToFile(config, pageData);
          outputFiles.push(filename);

          // Also push to dataset for compatibility with existing code
          await pushData(pageData);

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

          // Prepare page data
          const pageData = {
            title,
            url: request.loadedUrl,
            html,
            sourceUrl: request.loadedUrl,
          };
          
          // Write the data to a file immediately
          const filename = await writePageToFile(config, pageData);
          outputFiles.push(filename);
          
          // Also push to dataset for compatibility with existing code
          await pushData(pageData);

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
  
  return outputFiles;
}

// Update the class for handling both single and array configs
class GPTCrawlerCore {
  private config: Config | Config[];

  constructor(config: Config | Config[]) {
    this.config = config;
  }

  async crawl(): Promise<string[]> {
    return await crawl(this.config);
  }
}

export default GPTCrawlerCore;
