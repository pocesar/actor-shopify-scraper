import Apify from 'apify';

import { load } from 'cheerio';
import * as fns from './fns.js';

const { log } = Apify.utils;

export const entry = async () => {
    /** @type {any} */
    const input = await Apify.getInput();

    const {
        startUrls = [],
        maxConcurrency = 20,
        maxRequestsPerCrawl,
        maxRequestRetries = 3,
        proxyConfig,
        debugLog = false,
        fetchHtml = false,
        checkForBanner = true,
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
        await Apify.setValue('FILTERED', [...filteredSitemapUrls.values()]);
    };

    Apify.events.on('aborting', persistState);
    Apify.events.on('migrating', persistState);

    await fns.checkForRobots({
        startUrls,
        proxyConfiguration,
        filteredSitemapUrls,
        checkForBanner,
    });

    const extendOutputFunction = await fns.extendFunction({
        key: 'extendOutputFunction',
        map: async ({ product, url, images, imagesWithoutVariants }) => {
            if (!product) {
                return;
            }

            const created_at = fns.coalesceProps([product], ['created_at', 'createdAt']);
            const updated_at = fns.coalesceProps([product], ['updated_at', 'updatedAt']);
            const published_at = fns.coalesceProps([product], ['published_at', 'publishedAt']);
            const product_type = fns.coalesceProps([product], ['product_type', 'productType']);

            return product.variants.map((variant) => {
                const { name, props } = fns.getVariantAttributes(variant, product);
                const description = fns.coalesceProps([product], ['body_html', 'descriptionHtml', 'description']);
                const stock_count = fns.coalesceProps([variant], ['inventoryQuantity', 'inventory_quantity']);
                const availableForSale = fns.coalesceProps([variant], ['availableForSale', 'available_for_sale']);
                const weight_unit = fns.coalesceProps([variant], ['weight_unit', 'weightUnit']);
                const requires_shipping = fns.coalesceProps([variant], ['requiresShipping', 'requires_shipping']);
                const display_name = fns.coalesceProps([variant], ['displayName', 'display_name']);

                return {
                    url,
                    color: props.color ?? null,
                    size: props.size ?? null,
                    material: props.material ?? null,
                    display_name: display_name ?? null,
                    title: product.title,
                    id: `${fns.removeGuid(product.id)}`,
                    description: (description && fns.stripHtml(description)?.result) || null,
                    sku: `${variant.sku || fns.removeGuid(variant.id)}`,
                    // eslint-disable-next-line no-nested-ternary
                    availability: +stock_count
                        ? (stock_count > 0 ? 'in stock' : 'out of stock')
                        : availableForSale ? 'in stock' : 'out of stock',
                    price: +variant.price || null,
                    currency: 'USD',
                    product_type,
                    images_urls: fns.uniqueNonEmptyArray([
                        images.get(variant.image_id)?.src,
                        imagesWithoutVariants,
                        product.image?.src,
                    ].flat().filter((s) => s).map(fns.removeUrlQueryString)),
                    brand: product.vendor,
                    video_urls: [],
                    created_at: fns.safeIsoDate(props.created_at ?? created_at),
                    updated_at: fns.safeIsoDate(props.updated_at ?? updated_at),
                    published_at: fns.safeIsoDate(props.published_at ?? published_at),
                    additional: {
                        variant_attributes: name,
                        variant_title: variant.title,
                        scraped_at: new Date(),
                        barcode: variant.barcode || null,
                        taxcode: variant.taxcode || null,
                        stock_count: stock_count ?? null,
                        tags: fns.uniqueNonEmptyArray(Array.isArray(product.tags) ? product.tags : (product.tags ?? '').split(/,\s*/g)),
                        weight: variant.weight ? `${variant.weight} ${weight_unit}` : null,
                        requires_shipping: requires_shipping || null,
                        ...Object.entries(props)
                            .filter(([prop]) => ![
                                'color',
                                'size',
                                'material',
                                'created_at',
                                'updated_at',
                                'published_at',
                            ].includes(prop))
                            .reduce((out, [prop, value]) => ({ ...out, [prop]: value }), {}),
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
        maxConcurrency,
        limit: +maxRequestsPerCrawl,
        filter: async (url) => {
            const isProduct = /\/products\//.test(url);
            const isSitemap = /sitemap_products_\d+/.test(url);

            if (isSitemap) {
                return true;
            }

            if (!isProduct) {
                return false;
            }

            /** @type {boolean} */
            let filtered = isProduct;

            /** @param {boolean} result */
            const filter = (result) => {
                filtered = filtered && result;
            };

            await extendScraperFunction(undefined, {
                url,
                filter,
                isSitemap,
                isProduct,
                label: 'FILTER_SITEMAP_URL',
            });

            return filtered;
        },
        map: (url) => {
            return {
                url: fetchHtml ? url : `${url}.json`,
                userData: {
                    url,
                    label: fetchHtml
                        ? 'HTML'
                        : 'JSON',
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
        ignoreSslErrors: true,
        sessionPoolOptions: {
            sessionOptions: {
                maxErrorScore: 0.5,
            },
        },
        maxRequestRetries,
        maxRequestsPerCrawl: +maxRequestsPerCrawl > 0
            ? (+maxRequestsPerCrawl * (fetchHtml ? 2 : 1)) + await requestQueue.handledCount() // reusing the same request queue
            : undefined,
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
            const { json, request, response } = context;

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

            if (!json?.product?.title) {
                if (!json?.title) {
                    // this is the last resort
                    if (debugLog) {
                        const kvFriendlyNameUrl = new URL(request.url);
                        await Apify.setValue(kvFriendlyNameUrl.pathname.replace(/[^a-z\-.0-9]/gi, '').slice(0, 100), json);
                    }

                    if (response.statusCode !== 404) {
                        throw new Error('Missing product prop or title');
                    }

                    return;
                }
            }

            const product = json.product ?? json;

            context.$ = request.userData.body
                ? load(request.userData.body, { decodeEntities: true })
                : context.$;

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
            }, { context });
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
