// For more information, see https://crawlee.dev/
import { Configuration, PlaywrightCrawler, downloadListOfUrls } from "crawlee";
import { readFile, writeFile } from "fs/promises";
import { glob } from "glob";
import { Config, configSchema } from "./config.js";
import { Page } from "playwright";
import { isWithinTokenLimit } from "gpt-tokenizer";
import { PathLike } from "fs";

let pageCounter = 0;
let crawler: PlaywrightCrawler;

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

// Modify the crawl function to handle array of URLs
export async function crawl(config: Config) {
  configSchema.parse(config);

  if (process.env.NO_CRAWL !== "true") {
    // Create the crawler with the same configuration
    const crawler = new PlaywrightCrawler({
      // Use the requestHandler to process each of the crawled pages.
      async requestHandler({ request, page, enqueueLinks, log, pushData }) {
        // Same requestHandler code...
        if (config.cookie) {
          const cookie = {
            name: config.cookie.name,
            value: config.cookie.value,
            url: request.loadedUrl,
          };
          await page.context().addCookies([cookie]);
        }

        const title = await page.title();
        pageCounter++;
        log.info(
          `Crawling: Page ${pageCounter} / ${config.maxPagesToCrawl} - URL: ${request.loadedUrl}...`,
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
            typeof config.match === "string" ? [config.match] : config.match,
        });
      },
      maxRequestsPerCrawl: config.maxPagesToCrawl,
      // Other options remain the same...
      preNavigationHooks: [
        // Same preNavigationHooks code...
        async ({ page, log }) => {
          const RESOURCE_EXCLUSTIONS = config.resourceExclusions ?? [];
          if (RESOURCE_EXCLUSTIONS.length === 0) {
            return;
          }
          await page.route(`**\/*.{${RESOURCE_EXCLUSTIONS.join()}}`, (route) =>
            route.abort("aborted"),
          );
          log.info(
            `Aborting requests for as this is a resource excluded route`,
          );
        },
      ],
    });

    // Handle URLs based on whether config.url is a string or array
    if (Array.isArray(config.url)) {
      // For each URL in the array
      for (const url of config.url) {
        pageCounter = 0; // Reset page counter for each URL
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
      }
    } else {
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

export async function write(config: Config) {
  let nextFileNameString: PathLike = "";
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

        if (!filesByUrl.has(cleanUrl)) {
          filesByUrl.set(cleanUrl, []);
        }

        filesByUrl.get(cleanUrl)!.push(file);
      } else {
        // If no URL is found, use the default output filename
        if (!filesByUrl.has("default")) {
          filesByUrl.set("default", []);
        }
        filesByUrl.get("default")!.push(file);
      }
    } catch (error) {
      console.error(`Error processing file ${file}:`, error);
      // If there's an error, add to default group
      if (!filesByUrl.has("default")) {
        filesByUrl.set("default", []);
      }
      filesByUrl.get("default")!.push(file);
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
}
