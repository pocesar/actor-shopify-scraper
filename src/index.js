const Apify = require('apify');
const fns = require('./fns');

const { log } = Apify.utils;

const entry = async () => {
    /** @type {any} */
    const input = await Apify.getInput();

    const {
        startUrls = [],
        maxConcurrency = 20,
        proxyConfig,
        maxRequestRetries = 3,
        debugLog = false,
        fetchHtml = false,
    } = input;

    if (debugLog) {
        log.setLevel(log.LEVELS.DEBUG);
    }

    const proxyConfiguration = await fns.proxyConfiguration({
        proxyConfig,
    });

    if (!startUrls?.length) {
        throw new Error('Missing "startUrls" input');
    }

    /**
     * @type {Set<string>}
     */
    const filteredSitemapUrls = new Set(await Apify.getValue('FILTERED') || []);

    const persistState = async () => {
        await Apify.setValue('FILTERED', [...filteredSitemapUrls.entries()]);
    };

    Apify.events.on('aborting', persistState);
    Apify.events.on('migrating', persistState);

    await fns.checkForRobots({
        startUrls,
        proxyConfiguration,
        filteredSitemapUrls,
    });

    const extendOutputFunction = await fns.extendFunction({
        key: 'extendOutputFunction',
        map: async ({ product, url, images, imagesWithoutVariants }) => {
            if (!product) {
                return;
            }

            return product.variants.map((variant) => {
                const { name, props } = fns.getVariantAttributes(variant, product);

                return {
                    url,
                    color: props.color || null,
                    size: props.size || null,
                    material: props.material || null,
                    title: product.title,
                    id: `${product.id}`,
                    description: (product.body_html && fns.stripHtml(product.body_html)?.result) || null,
                    sku: `${variant.sku || variant.id}`,
                    availability: variant.inventory_quantity > 0 ? 'in stock' : 'out of stock',
                    price: +variant.price || null,
                    currency: 'USD',
                    images_urls: fns.uniqueNonEmptyArray([
                        images.get(variant.image_id)?.src,
                        imagesWithoutVariants,
                        product.image?.src,
                    ].flat().filter((s) => s).map(fns.removeUrlQueryString)),
                    brand: product.vendor,
                    video_urls: [],
                    additional: {
                        variant_attributes: name,
                        variant_title: variant.title,
                        created_at: new Date(variant.created_at),
                        updated_at: new Date(variant.updated_at),
                        scraped_at: new Date(),
                        barcode: variant.barcode || null,
                        taxcode: variant.taxcode || null,
                        stock_count: variant.inventory_quantity,
                        tags: fns.uniqueNonEmptyArray((product.tags ?? '').split(/,\s*/g)),
                        weight: variant.weight ? `${variant.weight} ${variant.weight_unit}` : null,
                        requires_shipping: variant.requires_shipping || null,
                        ...Object.entries(props)
                            .filter(([name]) => !['color', 'size', 'material'].includes(name))
                            .reduce((out, [name, value]) => ({ ...out, [name]: value }), {}),
                    },
                };
            });
        },
        output: async (data) => {
            await Apify.pushData(data);
        },
        input,
        helpers: {
            fns,
        },
    });

    const extendScraperFunction = await fns.extendFunction({
        key: 'extendScraperFunction',
        input,
        helpers: {
            fns,
        },
    });

    const requestQueue = await Apify.openRequestQueue();

    await extendScraperFunction(undefined, {
        proxyConfiguration,
        filteredSitemapUrls,
        requestQueue,
        label: 'SETUP',
    });

    const requestList = await fns.requestListFromSitemaps({
        proxyConfiguration,
        requestQueue,
        filter: (url) => {
            return /sitemap_products_\d+/.test(url)
                || /\/products\//.test(url);
        },
        map: (url) => {
            return {
                url: fetchHtml ? url : `${url}.json`,
                userData: {
                    url,
                    label: !fetchHtml
                        ? 'JSON'
                        : 'HTML',
                },
            };
        },
        sitemapUrls: [...filteredSitemapUrls.values()],
    });

    await Apify.setValue('STATS', { count: requestList.length() });

    const crawler = new Apify.CheerioCrawler({
        requestList,
        proxyConfiguration,
        requestQueue,
        useSessionPool: true,
        maxConcurrency,
        handlePageTimeoutSecs: 60,
        maxRequestRetries,
        persistCookiesPerSession: false,
        preNavigationHooks: [async (crawlingContext, requestAsBrowserOptions) => {
            await extendScraperFunction(undefined, {
                label: 'PRENAVIGATION',
                crawlingContext,
                requestAsBrowserOptions,
            });
        }],
        postNavigationHooks: [async (crawlingContext) => {
            await extendScraperFunction(undefined, {
                label: 'POSTNAVIGATION',
                crawlingContext,
            });
        }],
        handlePageFunction: async (context) => {
            const { json, request } = context;

            log.debug(`Scraping ${request.url}`);

            if (request.userData.label === 'HTML') {
                await requestQueue.addRequest({
                    url: `${request.url}.json`,
                    userData: {
                        label: 'JSON',
                        body: context.body,
                    },
                }, { forefront: true });

                return;
            }

            const { product } = json;

            if (!product || !product.title) {
                throw new Error('Missing product prop or title');
            }

            const url = request.url.replace('.json', '');
            const variants = fns.mapIdsFromArray(product.variants);
            const images = fns.mapIdsFromArray([...product.images, product.image]);
            /** @type {string[]} */
            const imagesWithoutVariants = (product.images ?? [])
                .filter(({ variant_ids, src }) => (src && !(variant_ids?.length)))
                .map(({ src }) => src);

            await extendOutputFunction({
                product,
                variants,
                url,
                images,
                imagesWithoutVariants,
            }, context);
        },
        handleFailedRequestFunction: async ({ request, error }) => {
            log.exception(error, 'Failed all retries', { url: request.url });

            await Apify.pushData({
                '#failed': Apify.utils.createRequestDebugInfo(request),
            });
        },
    });

    await extendScraperFunction(undefined, {
        crawler,
        requestList,
        label: 'RUN',
    });

    if (!debugLog) {
        fns.patchLog(crawler);
    }

    await crawler.run();

    await extendScraperFunction(undefined, {
        crawler,
        label: 'FINISHED',
    });

    await persistState();
};

module.exports = entry;
