import { Config } from "./src/config";

export const defaultConfig: Config = {
  url: [

  ],
  match: "**", // Match all URLs within the domains
  maxPagesToCrawl: 1, // For each URL, only crawl the initial page
  outputFileName: "", // Default output name
};
