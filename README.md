## What does Shopify Scraper do?

Using this tool, you can automate monitoring prices on the most popular solution for building online stores and selling products online. Crawl arbitrary Shopify-powered online stores and extract a list of all products in a structured form, including product title, price, description, etc.

## Need to find product pairs between Shopify and another online shop?

Use the [AI Product Matcher](https://apify.com/equidem/ai-product-matcher). This AI model allows you to compare items from different web stores, identifying exact matches and comparing real-time data obtained via web scraping. With the AI Product Matcher, you can use scraped product data to monitor product matches across the industry, implement dynamic pricing for your website, replace or complement manual mapping, and obtain realistic estimates against your competition for upcoming promo campaigns. 

Most importantly, it is relatively easy to get started with (just follow [this guide](https://blog.apify.com/product-matching-ai-pricing-intelligence-web-scraping/)) and it can match thousands of product pairs.

## Extend Scraper and Output Function

Extend output function allows to filter the items that are output:

```js
async ({ item, customData }) => {
    if (!item.title.includes('cuisine')) {
        return null; // omit the output
    }

    delete item.additional; // remove data from output

    item.requestId = customData.requestId; // add data from the outside

    return item;
}
```

Extend scraper function allows you to interact with scraper phases:

```js
async ({ label, url, filter, fns, filteredSitemapUrls, customData }) => {
    switch (label) {
        case 'FILTER_SITEMAP_URL': {
            // product url, like .../products/cooking-for-dummies-2002-289854
            filter(
                url.includes('cooking') || url.includes(customData.filter)
            );
            break;
        }
        case 'SETUP': {
            // filteredSitemapUrls is a `Set` instance and can be edited in-place
            filteredSitemapUrls.add('https://example.com/secret-unlisted-sitemap.xml');
            filteredSitemapUrls.forEach((sitemapURL) => {
                if (!sitemapURL.includes('en-us')) {
                    filteredSitemapUrls.delete(sitemapURL);
                }
            });
            break;
        }
    }
}
```

## License

Apache 2.0
