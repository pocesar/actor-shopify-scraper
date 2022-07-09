# Shopify Scraper

Automate monitoring prices on the most popular solution for building online stores and selling products online. Crawl arbitrary Shopify-powered online stores and extract a list of all products in a structured form, including product title, price, description, etc.

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
