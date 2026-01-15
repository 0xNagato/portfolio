<?php

namespace App\Actions\Compare;

use App\DataTransferObjects\ComparePageData;
use App\DataTransferObjects\Media\ImageMediaItem;
use App\DataTransferObjects\PresentedProductData;
use App\Models\CrossReferenceEntry;
use App\Models\PriceSeriesAggregate;
use App\Models\Product;
use App\Models\SkuRegion;
use App\Services\Catalogue\PriceCrossReferencer;
use App\Services\Compare\SpotlightProductScorer;
use App\Support\Media\ProductMediaResolver;
use App\Support\ProductPresenter;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\URL;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;

class BuildComparePageDataAction
{
    /**
     * Request-local memoization for Schema::hasTable().
     *
     * On Postgres this can be surprisingly expensive (pg_catalog probes), and we
     * call it multiple times while building the compare page.
     *
     * @var array<string, bool>
     */
    private array $tableExistsCache = [];

    /**
     * Compare page cache repository (memoized per request).
     */
    private ?\Illuminate\Contracts\Cache\Repository $compareCache = null;

    public function __construct(private readonly SpotlightProductScorer $spotlightScorer)
    {
    }

    protected function hasTable(string $table): bool
    {
        return $this->tableExistsCache[$table] ??= Schema::hasTable($table);
    }

    /**
     * The compare page originally used persisted rows from `cross_reference_entries`.
     *
     * We now also support a runtime build via {@see PriceCrossReferencer}, which is useful:
     * - during early bootstrapping (table not present yet)
     * - when you want the compare explorer to reflect the live merged catalog sources
     */
    protected function shouldUseRuntimeCrossReferences(): bool
    {
        if ((bool) config('catalogue.cross_reference.use_runtime_referencer', false)) {
            return true;
        }

        // If the table doesn't exist, runtime is the only viable option.
        return ! $this->hasTable('cross_reference_entries');
    }

    /**
     * The app currently defaults to the database cache store; on Postgres (often
     * over a network connection) DB-backed cache reads can take seconds.
     *
     * For the compare page, prefer local file cache in local/testing.
     */
    protected function compareCache(): \Illuminate\Contracts\Cache\Repository
    {
        if ($this->compareCache) {
            return $this->compareCache;
        }

        $overrideStore = config('compare.cache_store');

        if (is_string($overrideStore) && $overrideStore !== '') {
            return $this->compareCache = Cache::store($overrideStore);
        }

        $defaultStore = (string) config('cache.default');

        if (app()->environment(['local', 'testing']) && $defaultStore === 'database') {
            return $this->compareCache = Cache::store('file');
        }

        return $this->compareCache = Cache::store($defaultStore);
    }

    public function handle(bool $withCrossReference = true): ComparePageData
    {
        $spotlight = $this->loadSpotlightProducts();
        $initialProduct = $this->resolveInitialProduct($spotlight);

        if (! $initialProduct) {
            $initialProduct = $this->placeholderProduct();
        }

        if (empty($spotlight)) {
            $spotlight = [$initialProduct];
        }

        $regionOptions = $this->loadRegionOptions();
        if (empty($regionOptions)) {
            $regionOptions = ['US', 'EU', 'JP'];
        }

        $stats = $withCrossReference
            ? $this->loadCrossReferenceStats()
            : $this->emptyCrossReferenceStats();

        // For safety: default to a bounded initial payload. Loading all matches is expensive.
        $frontendLimit = $this->normalizeFrontendLimit(config('catalogue.cross_reference.frontend_limit', 500));

        $matches = [];

        if ($withCrossReference) {
            $matches = $this->loadPrioritizedMatches($frontendLimit);
        }

        if ($withCrossReference) {
            $stats['displayed'] = count($matches);
            $stats['display_limit'] = $frontendLimit;
        } else {
            $stats['displayed'] = 0;
            $stats['display_limit'] = $frontendLimit;
        }

        $apiEndpoints = [
            'stats' => route('api.compare.stats'),
            'entries' => route('api.compare.entries'),
            'spotlight' => route('api.compare.spotlight'),
        ];

        $platformFilters = $this->extractPlatformFilters($matches);
        $currencyFilters = $this->extractCurrencyFilters($matches);

        return new ComparePageData(
            initialProduct: $initialProduct,
            spotlight: $spotlight,
            regionOptions: $regionOptions,
            crossReferenceStats: $stats,
            apiEndpoints: $apiEndpoints,
            crossReferenceMatches: $matches,
            crossReferencePlatforms: $platformFilters,
            crossReferenceCurrencies: $currencyFilters,
        );
    }

    /**
     * @return array<string, int|string|array>
     */
    protected function emptyCrossReferenceStats(): array
    {
        return [
            'total' => 0,
            'digital' => 0,
            'physical' => 0,
            'both' => 0,
            'platforms' => [],
            'currencies' => [],
            'generated_at' => null,
            'displayed' => 0,
            'display_limit' => null,
        ];
    }

    protected function placeholderProduct(): PresentedProductData
    {
        return PresentedProductData::fromArray([
            'id' => 0,
            'name' => 'Spotlight warming up',
            'slug' => 'spotlight-warming-up',
            'platform' => 'TBD',
            'category' => null,
            'release_date' => null,
            'rating' => null,
            'popularity_score' => null,
            'image' => asset('images/placeholders/game-cover.svg'),
            'trailer_url' => null,
            'trailer_thumbnail' => asset('images/placeholders/game-cover.svg'),
            'trailer_play_url' => null,
            'trailer_source' => null,
            'trailer_title' => null,
            'region_codes' => [],
            'price_summary' => null,
            'updated_at' => now()->toIso8601String(),
        ]);
    }

    /**
     * @return PresentedProductData[]
     */
    protected function loadSpotlightProducts(): array
    {
        if (! $this->hasTable('products')) {
            // No local products yet — try external Top 20 bootstrap
            return $this->bootstrapTopSpotlight();
        }
        /**
         * @var Collection<int, array<string, mixed>> $payload
         */
        $cacheKey = 'compare:spotlight-products:v8';
        $cacheTtl = now()->addMinutes(15);

        $builder = function (): Collection {
            $hasLegacyMedia = $this->hasTable('product_media');
            $hasMediaTable = $this->hasTable('media');
            $hasToplists = $this->hasTable('provider_toplists') && $this->hasTable('provider_toplist_items');

            $toplistRanks = $hasToplists
                ? $this->loadSpotlightToplistRankMaps(limitPerProvider: 80)
                : ['rawg' => [], 'igdb' => [], 'psstore' => []];
            $toplistCandidateIds = array_values(array_unique(array_merge(
                array_keys($toplistRanks['rawg'] ?? []),
                array_keys($toplistRanks['igdb'] ?? []),
                array_keys($toplistRanks['psstore'] ?? [])
            )));

            // We often have multiple `products` rows representing region/platform variants of the
            // same underlying game. Oversample and dedupe before scoring/limiting so landing +
            // compare both render one spotlight card per canonical game.
            $candidateLimit = 80;

            $query = Product::query()
                ->select([
                    'id',
                    'name',
                    'slug',
                    'platform',
                    'category',
                    'release_date',
                    'updated_at',
                    'popularity_score',
                    'rating',
                    'external_ids',
                    'metadata',
                ])
                // Spotlight is driven by Rust-persisted provider toplists (RAWG/IGDB).
                // If toplists are missing/empty (bootstrap/testing), fall back to external_ids.
                ->when(
                    count($toplistCandidateIds) >= 20,
                    fn ($q) => $q->whereIn('id', $toplistCandidateIds),
                    fn ($q) => $q->where(function ($q): void {
                        $q->whereNotNull('external_ids->igdb')
                            ->orWhereNotNull('external_ids->rawg');
                    })
                )
                ->with([
                    'skuRegions' => fn ($query) => $query
                        ->select(['id', 'product_id', 'region_code', 'retailer', 'currency', 'is_active']),
                    'platforms:id,name,code,family',
                ]);

            // Spotlight image/video fallback relies on Spatie media attached to video games.
            // Eager-load it when available to avoid N+1 queries through the resolver.
            if ($hasMediaTable) {
                $query->with([
                    'media',
                    'videoGames' => fn ($relation) => $relation
                        ->select(['id', 'product_id', 'title', 'created_at', 'updated_at'])
                        ->orderByDesc('updated_at'),
                    'videoGames.media',
                ]);
            }

            if ($hasLegacyMedia) {
                $query->with([
                    'productMedia' => fn ($relation) => $relation
                        ->orderByRaw("CASE WHEN media_type = 'video' THEN 0 ELSE 1 END")
                        ->orderByDesc('is_primary')
                        ->orderByDesc('quality_score')
                        ->orderByDesc('fetched_at')
                        ->orderByDesc('id')
                        ->limit(18),
                ]);
            } else {
                // If legacy product_media is not present, we still want some video game context.
                // Note: video_games does not guarantee vendor sync columns; stick to safe columns.
                $query->with([
                    'videoGames' => fn ($relation) => $relation
                        ->select(['id', 'product_id', 'title', 'created_at', 'updated_at'])
                        ->orderByDesc('updated_at')
                        ->limit(50), // Prevent loading 10k+ video games per product
                ]);

                if ($hasMediaTable) {
                    $query->with('videoGames.media');
                }
            }

            // First, try to get products with video content
            $productsWithVideos = collect();

            if ($hasMediaTable) {
                $productsWithVideos = (clone $query)
                    ->whereHas('videoGames', function ($videoQuery) {
                        $videoQuery->whereHas('media', function ($mediaQuery) {
                            $mediaQuery->whereIn('collection_name', [
                                'game_videos',
                                'giantbomb-videos',
                                'provider-videos',
                                'igdb-videos',
                            ]);
                        });
                    })
                    // Rank by IGDB-driven popularity with safe fallbacks
                    ->orderByDesc('popularity_score')
                    ->orderByDesc('rating')
                    ->orderByDesc('release_date')
                    ->orderByDesc('updated_at')
                        ->limit($candidateLimit)
                    ->get();
            }

            // If we don't have enough games with videos, fill with regular games
            $products = $productsWithVideos;
            if ($productsWithVideos->count() < 20) {
                $existingIds = $productsWithVideos->pluck('id')->toArray();
                $additionalProducts = $query
                    ->whereNotIn('id', $existingIds)
                    ->orderByDesc('popularity_score')
                    ->orderByDesc('rating')
                    ->orderByDesc('release_date')
                    ->orderByDesc('updated_at')
                    ->limit(max(0, $candidateLimit - $productsWithVideos->count()))
                    ->get();

                $products = $productsWithVideos->merge($additionalProducts);
            }

            // Warm media in one batch to avoid N+1 cache-store roundtrips when
            // presenting spotlight cards (and their gallery) on landing/compare.
            // This is especially important when the default cache driver is `database`.
            ProductMediaResolver::resolveMany($products, true);

            $aggregates = ProductPresenter::aggregateMap($products);

            $spatieCoverImages = $this->spotlightCoverImagesFromSpatie($products);

            $collection = $products
                ->map(function (Product $product) use ($aggregates, $spatieCoverImages, $toplistRanks, $hasToplists): array {
                    $aggregateSet = $aggregates->get($product->id);

                    $presented = ProductPresenter::present($product, $aggregateSet);

                    // Used for upstream dedupe; removed before returning.
                    $presented['canonical_key'] = $this->spotlightCanonicalKey($product);

                    // Spotlight cover fallback order:
                    // 1) legacy product_media (handled by presenter)
                    // 2) Spatie videoGames->media (preferred)
                    // 3) cross-reference best match (applied in a second pass below)
                    if (empty($presented['image'])) {
                        $presented['image'] = $spatieCoverImages[$product->id] ?? null;
                    }

                    // Prefer toplist ranks when the schema is present; fall back to legacy
                    // metadata ranks for older DB snapshots or early bootstraps.
                    if ($hasToplists && (! empty($toplistRanks['rawg']) || ! empty($toplistRanks['igdb']) || ! empty($toplistRanks['psstore']))) {
                        $rawgRank = $toplistRanks['rawg'][$product->id] ?? null;
                        $igdbRank = $toplistRanks['igdb'][$product->id] ?? null;
                        $psstoreRank = $toplistRanks['psstore'][$product->id] ?? null;
                        $best = null;
                        foreach ([$rawgRank, $igdbRank, $psstoreRank] as $rank) {
                            if (! is_int($rank) || $rank <= 0) {
                                continue;
                            }
                            $best = $best === null ? $rank : min($best, $rank);
                        }

                        $presented['_spotlight_rank_rawg'] = $rawgRank;
                        $presented['_spotlight_rank_igdb'] = $igdbRank;
                        $presented['_spotlight_rank_psstore'] = $psstoreRank;
                        $presented['_spotlight_best_rank'] = $best;
                    } else {
                        $providerRanks = $this->spotlightProviderRanks($product);
                        $presented['_spotlight_rank_rawg'] = $providerRanks['rawg'];
                        $presented['_spotlight_rank_igdb'] = $providerRanks['igdb'];
                        $presented['_spotlight_rank_psstore'] = null;
                        $presented['_spotlight_best_rank'] = $providerRanks['best_rank'];
                    }

                    $score = $this->spotlightScorer->score($product, $aggregateSet);

                    $presented['spotlight_score'] = $score->toArray();
                    $presented['spotlight_gallery'] = $this->transformGallery($product);
                    $presented['platform_labels'] = $this->resolvePlatformLabels($product, $score->context());
                    $presented['retailer_names'] = $score->context()['retailer_names'] ?? [];

                    return $presented;
                });

            $missingImageIds = $collection
                ->filter(static fn (array $item): bool => empty($item['image']))
                ->pluck('id')
                ->filter(fn ($id) => is_numeric($id))
                ->map(fn ($id) => (int) $id)
                ->unique()
                ->values()
                ->all();

            $crossReferenceImages = $this->spotlightCoverImagesFromCrossReferences($missingImageIds);

            $collection = $collection
                ->map(function (array $item) use ($crossReferenceImages): array {
                    if (empty($item['image'])) {
                        $id = isset($item['id']) && is_numeric($item['id']) ? (int) $item['id'] : null;

                        $item['image'] = $id !== null
                            ? ($crossReferenceImages[$id] ?? asset('images/placeholders/game-cover.svg'))
                            : asset('images/placeholders/game-cover.svg');
                    }

                    return $item;
                });

            // Dedupe variants (platform/region) into one canonical spotlight item before ranking.
            // This must happen before cache write so all consumers (compare + landing) are fixed.
            $collection = $this->dedupeSpotlightCollection($collection);

            $limit = 20;
            $perSource = 10;

            $sorted = $collection
                ->filter(static fn (array $item): bool => ! empty($item['image']) && ! empty($item['name']))
                ->sort(function (array $a, array $b): int {
                    // Prefer provider-ranked monthly entries; within that, prefer stronger compare readiness.
                    $rankA = is_numeric($a['_spotlight_best_rank'] ?? null) ? (int) $a['_spotlight_best_rank'] : PHP_INT_MAX;
                    $rankB = is_numeric($b['_spotlight_best_rank'] ?? null) ? (int) $b['_spotlight_best_rank'] : PHP_INT_MAX;
                    $hasRankA = (int) ($rankA !== PHP_INT_MAX);
                    $hasRankB = (int) ($rankB !== PHP_INT_MAX);
                    if ($hasRankA !== $hasRankB) {
                        return $hasRankB <=> $hasRankA;
                    }
                    if ($rankA !== $rankB) {
                        return $rankA <=> $rankB;
                    }

                    $scoreA = (float) data_get($a, 'spotlight_score.total', 0);
                    $scoreB = (float) data_get($b, 'spotlight_score.total', 0);
                    if ($scoreA !== $scoreB) {
                        return $scoreB <=> $scoreA;
                    }

                    // Trailer presence is a tie-breaker (not the primary sort signal).
                    $trailA = (int) (! empty($a['trailer_url']));
                    $trailB = (int) (! empty($b['trailer_url']));
                    if ($trailA !== $trailB) {
                        return $trailB <=> $trailA;
                    }

                    $popA = is_numeric($a['popularity_score'] ?? null) ? (float) $a['popularity_score'] : 0.0;
                    $popB = is_numeric($b['popularity_score'] ?? null) ? (float) $b['popularity_score'] : 0.0;
                    if ($popA !== $popB) {
                        return $popB <=> $popA;
                    }

                    return ((int) ($b['id'] ?? 0)) <=> ((int) ($a['id'] ?? 0));
                })
                ->values();

            // Aim for a blend of RAWG + IGDB monthly picks when both are present.
            $rawg = $sorted
                ->filter(static fn (array $item): bool => is_numeric($item['_spotlight_rank_rawg'] ?? null))
                ->take($perSource)
                ->values();

            $igdb = $sorted
                ->filter(static fn (array $item): bool => is_numeric($item['_spotlight_rank_igdb'] ?? null))
                ->take($perSource)
                ->values();

            $mixed = collect();
            for ($i = 0; $i < $perSource; $i++) {
                if (isset($rawg[$i])) {
                    $mixed->push($rawg[$i]);
                }
                if (isset($igdb[$i])) {
                    $mixed->push($igdb[$i]);
                }
            }

            $result = $mixed
                ->merge($sorted)
                ->unique(static fn (array $item): string => (string) ($item['id'] ?? '0'))
                ->take($limit)
                ->values();

            $result = $result
                ->map(static function (array $item): array {
                    unset(
                        $item['_spotlight_rank_rawg'],
                        $item['_spotlight_rank_igdb'],
                        $item['_spotlight_rank_psstore'],
                        $item['_spotlight_best_rank']
                    );
                    return $item;
                });

            if ($result->isEmpty()) {
                // If DB produced nothing useful, fallback to external bootstrap (not cached in this key)
                return collect();
            }

            return $result;
        };

        // In tests, avoid cross-test cache pollution (the array cache store persists in-process).
        // Production still benefits from a warm spotlight payload.
        $payload = app()->runningUnitTests()
            ? $builder()
            : $this->compareCache()->remember($cacheKey, $cacheTtl, $builder);

        // If DB cache miss or empty, try external bootstrap and cache separately
        if ($payload->isEmpty()) {
            $bootstrap = $this->bootstrapTopSpotlight();
            if (! empty($bootstrap)) {
                return $bootstrap;
            }
        }

        return $payload
            ->map(static fn (array $item) => PresentedProductData::fromArray($item))
            ->all();
    }

    /**
     * Load the most recent ranked toplist items for Spotlight.
     *
     * @return array{rawg: array<int,int>, igdb: array<int,int>, psstore: array<int,int>}
     */
    protected function loadSpotlightToplistRankMaps(int $limitPerProvider = 80): array
    {
        if (! $this->hasTable('provider_toplists') || ! $this->hasTable('provider_toplist_items')) {
            return ['rawg' => [], 'igdb' => [], 'psstore' => []];
        }

        $rawgToplistId = $this->resolveSpotlightToplistId('rawg');
        $igdbToplistId = $this->resolveSpotlightToplistId('igdb');
        $psstoreToplistId = $this->resolveSpotlightToplistId('psstore');

        $rawg = $rawgToplistId
            ? DB::table('provider_toplist_items')
                ->where('provider_toplist_id', $rawgToplistId)
                ->orderBy('rank')
                ->limit($limitPerProvider)
                ->pluck('rank', 'product_id')
                ->mapWithKeys(static fn ($rank, $productId) => [(int) $productId => (int) $rank])
                ->all()
            : [];

        $igdb = $igdbToplistId
            ? DB::table('provider_toplist_items')
                ->where('provider_toplist_id', $igdbToplistId)
                ->orderBy('rank')
                ->limit($limitPerProvider)
                ->pluck('rank', 'product_id')
                ->mapWithKeys(static fn ($rank, $productId) => [(int) $productId => (int) $rank])
                ->all()
            : [];

        $psstore = $psstoreToplistId
            ? DB::table('provider_toplist_items')
                ->where('provider_toplist_id', $psstoreToplistId)
                ->orderBy('rank')
                ->limit($limitPerProvider)
                ->pluck('rank', 'product_id')
                ->mapWithKeys(static fn ($rank, $productId) => [(int) $productId => (int) $rank])
                ->all()
            : [];

        return [
            'rawg' => $rawg,
            'igdb' => $igdb,
            'psstore' => $psstore,
        ];
    }

    protected function resolveSpotlightToplistId(string $providerKey): ?int
    {
        if (! $this->hasTable('provider_toplists')) {
            return null;
        }

        // Prefer monthly lists, then fall back to the newest list of any type.
        $monthly = DB::table('provider_toplists')
            ->where('provider_key', $providerKey)
            ->where('list_type', 'top_monthly')
            ->orderByDesc('snapshot_at')
            ->orderByDesc('period_start')
            ->value('id');

        if (is_numeric($monthly)) {
            return (int) $monthly;
        }

        $any = DB::table('provider_toplists')
            ->where('provider_key', $providerKey)
            ->orderByDesc('snapshot_at')
            ->orderByDesc('period_start')
            ->value('id');

        return is_numeric($any) ? (int) $any : null;
    }

    /**
     * Extract provider popularity ranks from the seeded metadata sources.
     *
     * @return array{rawg:?int, igdb:?int, best_rank:?int}
     */
    protected function spotlightProviderRanks(Product $product): array
    {
        $metadata = (array) ($product->metadata ?? []);
        $sources = (array) ($metadata['sources'] ?? []);

        $rawgRank = data_get($sources, 'rawg.popularity_rank');
        $igdbRank = data_get($sources, 'igdb.popularity_rank');

        $rawgRank = is_numeric($rawgRank) ? (int) $rawgRank : null;
        $igdbRank = is_numeric($igdbRank) ? (int) $igdbRank : null;

        $best = null;
        foreach ([$rawgRank, $igdbRank] as $rank) {
            if ($rank === null || $rank <= 0) {
                continue;
            }
            $best = $best === null ? $rank : min($best, $rank);
        }

        return [
            'rawg' => $rawgRank,
            'igdb' => $igdbRank,
            'best_rank' => $best,
        ];
    }

    protected function spotlightCanonicalKey(Product $product): string
    {
        $igdb = data_get($product->external_ids, 'igdb');
        if (is_numeric($igdb)) {
            return 'igdb:' . (int) $igdb;
        }

        $slug = is_string($product->slug ?? null) ? trim((string) $product->slug) : '';
        if ($slug !== '') {
            return 'slug:' . strtolower($slug);
        }

        $name = is_string($product->name ?? null) ? trim((string) $product->name) : '';
        if ($name !== '') {
            return 'name:' . Str::slug($name, '-');
        }

        return 'id:' . (int) $product->id;
    }

    /**
     * @param  Collection<int, array<string, mixed>>  $collection
     * @return Collection<int, array<string, mixed>>
     */
    protected function dedupeSpotlightCollection(Collection $collection): Collection
    {
        if ($collection->isEmpty()) {
            return $collection;
        }

        $placeholder = asset('images/placeholders/game-cover.svg');

        $deduped = $collection
            ->groupBy(static fn (array $item) => (string) ($item['canonical_key'] ?? ('id:' . (string) ($item['id'] ?? '0'))))
            ->map(function (Collection $group) use ($placeholder): array {
                return $group
                    ->sort(function (array $a, array $b) use ($placeholder): int {
                        // Prefer items with a trailer, then non-placeholder images, then best score.
                        $trailA = (int) (! empty($a['trailer_url']));
                        $trailB = (int) (! empty($b['trailer_url']));
                        if ($trailA !== $trailB) {
                            return $trailB <=> $trailA;
                        }

                        $imgA = (string) ($a['image'] ?? '');
                        $imgB = (string) ($b['image'] ?? '');
                        $hasImgA = (int) ($imgA !== '' && $imgA !== $placeholder);
                        $hasImgB = (int) ($imgB !== '' && $imgB !== $placeholder);
                        if ($hasImgA !== $hasImgB) {
                            return $hasImgB <=> $hasImgA;
                        }

                        // Prefer the best provider rank (lower is better) when available.
                        $rankA = is_numeric($a['_spotlight_best_rank'] ?? null) ? (int) $a['_spotlight_best_rank'] : PHP_INT_MAX;
                        $rankB = is_numeric($b['_spotlight_best_rank'] ?? null) ? (int) $b['_spotlight_best_rank'] : PHP_INT_MAX;
                        $hasRankA = (int) ($rankA !== PHP_INT_MAX);
                        $hasRankB = (int) ($rankB !== PHP_INT_MAX);
                        if ($hasRankA !== $hasRankB) {
                            return $hasRankB <=> $hasRankA;
                        }
                        if ($rankA !== $rankB) {
                            return $rankA <=> $rankB;
                        }

                        $scoreA = (float) data_get($a, 'spotlight_score.total', 0);
                        $scoreB = (float) data_get($b, 'spotlight_score.total', 0);
                        if ($scoreA !== $scoreB) {
                            return $scoreB <=> $scoreA;
                        }

                        $popA = is_numeric($a['popularity_score'] ?? null) ? (float) $a['popularity_score'] : 0.0;
                        $popB = is_numeric($b['popularity_score'] ?? null) ? (float) $b['popularity_score'] : 0.0;
                        if ($popA !== $popB) {
                            return $popB <=> $popA;
                        }

                        // Stable tie-breaker.
                        return ((int) ($b['id'] ?? 0)) <=> ((int) ($a['id'] ?? 0));
                    })
                    ->first() ?? [];
            })
            ->values()
            ->filter(static fn (array $item): bool => ! empty($item));

        return $deduped
            ->map(static function (array $item): array {
                unset($item['canonical_key']);
                return $item;
            });
    }

    /**
     * @param  Collection<int, Product>  $products
     * @return array<int, string>
     */
    protected function spotlightCoverImagesFromSpatie(Collection $products): array
    {
        $covers = [];

        foreach ($products as $product) {
            if (! $product instanceof Product) {
                continue;
            }

            $mediaSet = ProductMediaResolver::resolve($product);
            $best = $this->selectBestCoverImage($mediaSet->images);

            if (! $best) {
                continue;
            }

            $url = $best->thumbnail ?: $best->url;
            if (! is_string($url) || trim($url) === '') {
                continue;
            }

            $covers[(int) $product->id] = $url;
        }

        return $covers;
    }

    /**
     * @param  int[]  $productIds
     * @return array<int, string>
     */
    protected function spotlightCoverImagesFromCrossReferences(array $productIds): array
    {
        $productIds = array_values(array_unique(array_filter(array_map(static fn ($id) => is_numeric($id) ? (int) $id : null, $productIds))));

        if ($productIds === []) {
            return [];
        }

        // Prefer the persisted cross-reference table when available.
        // Building the runtime referencer can be expensive (large file parsing + DB scans), and
        // we don't want to do that on a web request just to fill a fallback cover image.
        if (! $this->shouldUseRuntimeCrossReferences() && $this->hasTable('cross_reference_entries')) {
            $products = Product::query()
                ->select(['id', 'name'])
                ->whereIn('id', $productIds)
                ->get();

            /** @var array<int, string> $keysByProductId */
            $keysByProductId = [];

            foreach ($products as $product) {
                if (! $product instanceof Product) {
                    continue;
                }

                $key = $this->normalizeCrossReferenceName($product->name);
                if ($key === null) {
                    continue;
                }

                $keysByProductId[(int) $product->id] = $key;
            }

            if ($keysByProductId === []) {
                return [];
            }

            $keys = array_values(array_unique(array_values($keysByProductId)));

            $entries = CrossReferenceEntry::query()
                ->select(['normalized_key', 'image_url'])
                ->whereIn('normalized_key', $keys)
                ->get();

            $imageByKey = [];
            foreach ($entries as $entry) {
                if (! $entry instanceof CrossReferenceEntry) {
                    continue;
                }

                $normalized = is_string($entry->normalized_key ?? null) ? trim((string) $entry->normalized_key) : '';
                $image = is_string($entry->image_url ?? null) ? trim((string) $entry->image_url) : '';

                if ($normalized === '' || $image === '') {
                    continue;
                }

                $imageByKey[$normalized] = $image;
            }

            if ($imageByKey === []) {
                return [];
            }

            $images = [];
            foreach ($keysByProductId as $productId => $key) {
                $image = $imageByKey[$key] ?? null;
                if (! is_string($image) || trim($image) === '') {
                    continue;
                }

                $images[(int) $productId] = $image;
            }

            return $images;
        }

        $wanted = array_fill_keys($productIds, true);
        $placeholderNeedle = '/images/placeholders/game-cover.svg';

        try {
            /** @var PriceCrossReferencer $referencer */
            $referencer = app(PriceCrossReferencer::class);
            $rows = $referencer->build();
        } catch (\Throwable $e) {
            report($e);
            return [];
        }

        if ($rows->isEmpty()) {
            return [];
        }

        $images = [];

        foreach ($rows as $row) {
            if (! is_array($row)) {
                continue;
            }

            $pid = $row['product_id'] ?? null;
            $pid = is_numeric($pid) ? (int) $pid : null;

            if ($pid === null || ! isset($wanted[$pid]) || isset($images[$pid])) {
                continue;
            }

            $image = $row['image'] ?? null;
            if (! is_string($image) || trim($image) === '') {
                continue;
            }

            // Ignore placeholder-only images; we only want a meaningful cross-reference fallback.
            if (str_contains($image, $placeholderNeedle)) {
                continue;
            }

            $images[$pid] = $image;
        }

        return $images;
    }

    /**
     * Normalize a product name to match {@see PriceCrossReferencer}'s normalized keys.
     */
    protected function normalizeCrossReferenceName(?string $name): ?string
    {
        if (! is_string($name) || trim($name) === '') {
            return null;
        }

        $clean = preg_replace('/(\[[^\]]*\]|\([^)]*\))/u', ' ', $name) ?? $name;
        $normalized = Str::of($clean)
            ->ascii()
            ->lower()
            ->replaceMatches('/[^a-z0-9]+/u', ' ')
            ->squish()
            ->value();

        return $normalized !== '' ? $normalized : null;
    }

    /**
     * Pick a stable "cover" image from the resolved media set.
     *
     * @param  Collection<int, ImageMediaItem>  $images
     */
    protected function selectBestCoverImage(Collection $images): ?ImageMediaItem
    {
        if ($images->isEmpty()) {
            return null;
        }

        $nonScreenshots = $images
            ->filter(fn ($img) => $img instanceof ImageMediaItem)
            ->filter(function (ImageMediaItem $img): bool {
                $kind = strtolower(trim((string) ($img->kind ?? '')));
                if ($kind === '') {
                    return true;
                }

                if ($kind === 'screenshot') {
                    return false;
                }

                return ! str_contains($kind, 'screenshot');
            });

        // Strict policy: screenshots are allowed only in galleries.
        // If we have no non-screenshot candidates, return null and let callers fall back to
        // trailer thumbnails/placeholders.
        if ($nonScreenshots->isEmpty()) {
            return null;
        }

        $candidates = $nonScreenshots;

        return $candidates
            ->sort(function (ImageMediaItem $a, ImageMediaItem $b): int {
                $rankA = $this->coverKindRankFromDto($a);
                $rankB = $this->coverKindRankFromDto($b);
                if ($rankA !== $rankB) {
                    return $rankB <=> $rankA;
                }

                $qualityA = is_numeric($a->quality ?? null) ? (float) $a->quality : null;
                $qualityB = is_numeric($b->quality ?? null) ? (float) $b->quality : null;
                if ($qualityA !== $qualityB) {
                    return ($qualityB ?? -1.0) <=> ($qualityA ?? -1.0);
                }

                $fetchedA = is_string($a->fetchedAt ?? null) ? strtotime((string) $a->fetchedAt) ?: 0 : 0;
                $fetchedB = is_string($b->fetchedAt ?? null) ? strtotime((string) $b->fetchedAt) ?: 0 : 0;
                if ($fetchedA !== $fetchedB) {
                    return $fetchedB <=> $fetchedA;
                }

                return (int) $b->id <=> (int) $a->id;
            })
            ->first();
    }

    protected function coverKindRankFromDto(ImageMediaItem $item): int
    {
        $kind = strtolower(trim((string) ($item->kind ?? '')));

        return match ($kind) {
            'cover', 'background', 'poster' => 400,
            'art', 'artwork', 'promo', 'promotional', 'banner' => 300,
            'logo', 'icon' => 200,
            '' => 100,
            default => 150,
        };
    }

    /**
     * Build a Top 20 spotlight list from external providers (IGDB/RAWG/TGDB mirror)
     * when the local DB has no candidate spotlight entries yet.
     *
     * @return PresentedProductData[]
     */
    protected function bootstrapTopSpotlight(): array
    {
        try {
            /** @var \App\Services\Catalogue\TopGamesBootstrapService $svc */
            $svc = app(\App\Services\Catalogue\TopGamesBootstrapService::class);

            /** @var PresentedProductData[] $items */
            $items = $this->compareCache()->remember('compare:spotlight-bootstrap:v1', now()->addMinutes(50), fn () => $svc->top(50));

            return $items;
        } catch (\Throwable $e) {
            // Best-effort only; if providers fail or are unconfigured, return empty
            return [];
        }
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    protected function transformGallery(Product $product): array
    {
        $mediaSet = ProductMediaResolver::resolve($product);

        $items = collect();

        // Prefer to inject a playable trailer first
        $primaryTrailer = $mediaSet->trailers()->first();
        if ($primaryTrailer) {
            // Get the best available URL for the video
            $embedUrl = $primaryTrailer->embedUrl;
            $originalUrl = $primaryTrailer->url;

            $videoUrl = null;

            // Priority 1: If embedUrl is a YouTube embed, use it directly
            if (is_string($embedUrl) && str_contains($embedUrl, 'youtube.com/embed/')) {
                $videoUrl = $embedUrl;
            }
            // Priority 2: Check if original URL is YouTube and convert to embed
            elseif (is_string($originalUrl) && str_contains($originalUrl, 'youtube.com/watch')) {
                parse_str(parse_url($originalUrl, PHP_URL_QUERY) ?: '', $params);
                $videoId = $params['v'] ?? null;
                if ($videoId) {
                    $videoUrl = "https://www.youtube.com/embed/{$videoId}";
                }
            }
            elseif (is_string($originalUrl) && preg_match('/youtu\.be\/([a-zA-Z0-9_-]+)/', $originalUrl, $matches)) {
                $videoUrl = "https://www.youtube.com/embed/{$matches[1]}";
            }
            // Priority 3: For direct video files (webm, mp4, etc.), use the URL directly without proxy
            // Iframes can load these directly from external sources like Wikimedia Commons
            else {
                $playSrc = $embedUrl ?: $originalUrl;
                if (is_string($playSrc) && $playSrc !== '') {
                    $videoUrl = $playSrc;
                }
            }

            if ($videoUrl !== null) {
                $items->push(array_filter([
                    'id' => $primaryTrailer->id,
                    'type' => 'video',
                    'url' => $videoUrl,
                    'thumbnail' => $primaryTrailer->thumbnail,
                    'title' => $primaryTrailer->title,
                    'source' => $primaryTrailer->attribution ?? $primaryTrailer->source,
                    'duration_seconds' => $primaryTrailer->durationSeconds,
                    'ordinal' => $primaryTrailer->ordinal,
                    'fetched_at' => $primaryTrailer->fetchedAt,
                ], static fn ($value) => $value !== null));
            }
        }

        // Append image gallery items
        $imageItems = $mediaSet->gallery()
            ->map(static function (ImageMediaItem $item): ?array {
                $url = trim($item->url);

                if ($url === '') {
                    return null;
                }

                $thumbnail = is_string($item->thumbnail) ? trim($item->thumbnail) : null;

                $payload = [
                    'id' => $item->id,
                    'type' => 'image',
                    'url' => $url,
                    'thumbnail' => $thumbnail !== '' ? $thumbnail : null,
                    'title' => $item->title !== '' ? $item->title : null,
                    'source' => $item->source !== '' ? $item->source : null,
                    'license' => $item->licenseUrl ?? $item->license,
                    'attribution' => $item->attribution,
                    'is_primary' => $item->isPrimary,
                    'quality_score' => $item->quality !== null ? round((float) $item->quality, 3) : null,
                    'width' => $item->width,
                    'height' => $item->height,
                    'kind' => $item->kind,
                    'ordinal' => $item->ordinal,
                    'fetched_at' => $item->fetchedAt,
                ];

                return array_filter($payload, static fn ($value) => $value !== null);
            })
            ->filter()
            ->values();

        return $items->concat($imageItems)->values()->all();
    }

    protected function base64UrlEncode(string $value): string
    {
        $encoded = base64_encode($value);
        return rtrim(strtr($encoded, '+/', '-_'), '=');
    }

    protected function guessVideoExtension(string $url): string
    {
        $path = (string) parse_url($url, PHP_URL_PATH);
        $ext = strtolower(pathinfo($path, PATHINFO_EXTENSION));
        return in_array($ext, ['mp4', 'm4v', 'webm', 'mov'], true) ? $ext : 'mp4';
    }

    /**
     * @param array<string, mixed> $scoreContext
     * @return string[]
     */
    protected function resolvePlatformLabels(Product $product, array $scoreContext): array
    {
        $labels = $scoreContext['platform_labels'] ?? null;

        if (is_array($labels) && ! empty($labels)) {
            return collect($labels)
                ->map(static fn ($value) => strtoupper(trim((string) $value)))
                ->filter()
                ->unique()
                ->values()
                ->all();
        }

        $platforms = $product->getRelationValue('platforms');

        if ($platforms instanceof Collection && $platforms->isNotEmpty()) {
            return $platforms
                ->pluck('name')
                ->filter()
                ->map(static fn ($value) => strtoupper(trim((string) $value)))
                ->filter()
                ->unique()
                ->values()
                ->all();
        }

        $platform = $product->platform;

        return $platform ? [strtoupper((string) $platform)] : [];
    }

    /**
     * @param PresentedProductData[] $spotlight
     */
    protected function resolveInitialProduct(array $spotlight): ?PresentedProductData
    {
        if (! $this->hasTable('products')) {
            return null;
        }
        $candidate = $spotlight[0] ?? null;

        if ($candidate instanceof PresentedProductData) {
            return $candidate;
        }

        /**
         * @var array<string, mixed>|null $payload
         */
        $payload = $this->compareCache()->remember('compare:initial-product', now()->addMinutes(10), function () {
            $hasLegacyMedia = $this->hasTable('product_media');

            $query = Product::query()
                ->select(['id', 'name', 'slug', 'platform', 'category', 'release_date', 'updated_at'])
                ->with([
                    'skuRegions:id,product_id,region_code',
                ]);

            if ($hasLegacyMedia) {
                $query->with([
                    'productMedia' => fn ($relation) => $relation
                        ->orderByRaw("CASE WHEN media_type = 'video' THEN 0 ELSE 1 END")
                        ->orderByDesc('fetched_at')
                        ->orderByDesc('id')
                        ->limit(12),
                ])->whereHas('productMedia', fn ($relation) => $relation->where('media_type', 'image'));
            } else {
                $query->with([
                    'videoGames' => fn ($relation) => $relation
                        ->select(['id', 'product_id', 'title', 'slug', 'last_synced_at'])
                        ->orderByDesc('last_synced_at'),
                ]);
            }

            $product = $query
                ->orderByDesc('release_date')
                ->orderByDesc('updated_at')
                ->first();

            if (! $product) {
                return null;
            }

            $aggregateCollection = ProductPresenter::aggregateMap([$product]);
            $aggregateSet = $aggregateCollection->get($product->id);

            $presented = ProductPresenter::present($product, $aggregateSet);

            return ! empty($presented['image']) ? $presented : null;
        });

        return is_array($payload) ? PresentedProductData::fromArray($payload) : null;
    }

    /**
     * @return string[]
     */
    protected function loadRegionOptions(): array
    {
        /**
         * @var array<int, string> $regionCodes
         */
        $regionCodes = $this->compareCache()->remember('compare:regions', now()->addHours(6), function () {
            $regions = collect();

            if ($this->hasTable('sku_regions')) {
                $regions = SkuRegion::query()
                    ->select('region_code')
                    ->distinct()
                    ->orderBy('region_code')
                    ->pluck('region_code');
            }

            if ($regions->isEmpty() && $this->hasTable('price_series_aggregates')) {
                $regions = PriceSeriesAggregate::query()
                    ->select('region_code')
                    ->distinct()
                    ->orderBy('region_code')
                    ->pluck('region_code');
            }

            return $regions
                ->map(static fn ($code) => strtoupper((string) $code))
                ->filter()
                ->unique()
                ->values()
                ->all();
        });

        return array_values($regionCodes);
    }

    protected function loadCrossReferenceStats(): array
    {
        if ($this->shouldUseRuntimeCrossReferences()) {
            return $this->loadCrossReferenceStatsFromRuntime();
        }

        return $this->loadCrossReferenceStatsFromDatabase();
    }

    /**
     * Load cross-reference stats from actual database pricing tables.
     *
     * @return array{total:int,digital:int,physical:int,both:int,platforms:string[],currencies:string[],generated_at:string,displayed:int,display_limit:int|null}
     */
    protected function loadCrossReferenceStatsFromDatabase(): array
    {
        $cacheKey = 'compare:stats:db:v3';

        $cached = $this->compareCache()->get($cacheKey);
        if (is_array($cached)) {
            return $cached;
        }

        // Count products with active pricing
        $total = Product::query()
            ->whereExists(function ($query) {
                $query->select(\DB::raw(1))
                    ->from('sku_regions')
                    ->whereColumn('sku_regions.product_id', 'products.id')
                    ->where('sku_regions.is_active', true);
            })
            ->count();

        $digital = Product::query()
            ->whereExists(function ($query) {
                $query->select(\DB::raw(1))
                    ->from('sku_regions')
                    ->whereColumn('sku_regions.product_id', 'products.id')
                    ->where('sku_regions.is_active', true);
            })
            ->count();

        // Get distinct platforms
        $platforms = DB::table('products')
            ->join('game_platform', 'products.id', '=', 'game_platform.product_id')
            ->join('platforms', 'game_platform.platform_id', '=', 'platforms.id')
            ->whereExists(function ($query) {
                $query->select(DB::raw(1))
                    ->from('sku_regions')
                    ->whereColumn('sku_regions.product_id', 'products.id')
                    ->where('sku_regions.is_active', true);
            })
            ->distinct()
            ->pluck('platforms.family')
            ->filter()
            ->unique()
            ->sort()
            ->values()
            ->all();

        // Get distinct currencies
        $currencies = DB::table('sku_regions')
            ->join('currencies', 'sku_regions.currency_id', '=', 'currencies.id')
            ->where('sku_regions.is_active', true)
            ->distinct()
            ->pluck('currencies.code')
            ->map(fn ($code) => strtoupper((string) $code))
            ->unique()
            ->sort()
            ->values()
            ->all();

        $computed = [
            'total' => $total,
            'digital' => $digital,
            'physical' => 0, // Physical pricing not yet implemented
            'both' => 0,
            'platforms' => $platforms,
            'currencies' => $currencies,
            'generated_at' => now()->toIso8601String(),
            'displayed' => 0,
            'display_limit' => null,
        ];

        $this->compareCache()->put($cacheKey, $computed, now()->addHours(6));

        return $computed;
    }

    /**
     * Runtime cross-reference stats (backed by {@see PriceCrossReferencer}).
     *
     * @return array{total:int,digital:int,physical:int,both:int,platforms:string[],currencies:string[],generated_at:string,displayed:int,display_limit:int|null}
     */
    protected function loadCrossReferenceStatsFromRuntime(): array
    {
        $cacheKey = 'compare:stats:runtime:v1';

        $cached = $this->compareCache()->get($cacheKey);
        if (is_array($cached)) {
            return $cached;
        }

        try {
            /** @var PriceCrossReferencer $referencer */
            $referencer = app(PriceCrossReferencer::class);
            $rows = $referencer->build();
        } catch (\Throwable $e) {
            report($e);
            return $this->emptyCrossReferenceStats();
        }

        if ($rows->isEmpty()) {
            return $this->emptyCrossReferenceStats();
        }

        $matches = $rows
            ->map(fn (array $row): array => $this->presentRuntimeCrossReferenceRow($row))
            ->values()
            ->all();

        $computed = [
            'total' => count($matches),
            'digital' => count(array_filter($matches, static fn (array $m): bool => (bool) ($m['has_digital'] ?? false))),
            'physical' => count(array_filter($matches, static fn (array $m): bool => (bool) ($m['has_physical'] ?? false))),
            'both' => count(array_filter($matches, static fn (array $m): bool => (bool) ($m['has_digital'] ?? false) && (bool) ($m['has_physical'] ?? false))),
            'platforms' => $this->extractPlatformFilters($matches),
            'currencies' => $this->extractCurrencyFilters($matches),
            'generated_at' => now()->toIso8601String(),
            'displayed' => 0,
            'display_limit' => null,
        ];

        $this->compareCache()->put($cacheKey, $computed, now()->addMinutes(20));

        return $computed;
    }

    /**
     * Extract distinct values from a text column containing a JSON array.
     *
     * `cross_reference_entries.platforms` / `currencies` are stored as TEXT (JSON-encoded arrays).
     * We defensively treat non-array payloads as empty.
     *
     * @return string[]
     */
    protected function distinctJsonTextArrayValuesFromCrossReferences(string $column, bool $upper = false): array
    {
        if (! in_array($column, ['platforms', 'currencies'], true)) {
            throw new \InvalidArgumentException('Unsupported column for extraction.');
        }

        // Note: platforms/currencies are TEXT columns with JSON arrays.
        // Cast to jsonb only when it looks like an array to avoid most cast failures.
        $selectValue = $upper ? 'upper(value)' : 'value';

        try {
            /** @var array<int, object{value: string|null}> $rows */
            $rows = DB::select(
                "select distinct {$selectValue} as value\n".
                "from cross_reference_entries\n".
                "cross join lateral jsonb_array_elements_text(\n".
                "  case\n".
                "    when {$column} is null then '[]'::jsonb\n".
                "    when {$column} ~ '^\\s*\\[' then {$column}::jsonb\n".
                "    else '[]'::jsonb\n".
                "  end\n".
                ") as value\n".
                "order by value"
            );
        } catch (\Throwable $e) {
            report($e);
            return [];
        }

        return collect($rows)
            ->map(static fn ($row) => is_string($row->value ?? null) ? trim((string) $row->value) : '')
            ->filter()
            ->unique()
            ->values()
            ->all();
    }

    /**
     * @param int|null $limit
     * @return array<int, array<string, mixed>>
     */
    protected function loadPrioritizedMatches(?int $limit): array
    {
        if ($this->shouldUseRuntimeCrossReferences()) {
            return $this->loadPrioritizedMatchesFromRuntime($limit);
        }

        // Title-centric approach: query video_game_titles as primary entity.
        return $this->loadPrioritizedMatchesFromTitles($limit);
    }

    /**
     * Load prioritized matches from actual database pricing tables.
     * This replaces the old cross-reference system with live price data.
     *
     * @param int|null $limit
     * @return array<int, array<string, mixed>>
     */
    protected function loadPrioritizedMatchesFromDatabase(?int $limit): array
    {
        $limit = $limit !== null ? max(50, $limit) : null;
        $cacheKey = $limit ? sprintf('compare:matches:db:%d:v3', $limit) : 'compare:matches:db:all:v3';

        return $this->compareCache()->remember($cacheKey, now()->addMinutes(10), function () use ($limit): array {
            // Temporarily increase execution time for cache building
            $previousLimit = ini_get('max_execution_time');
            set_time_limit(300); // 5 minutes for cache warmup

            try {
                // Use chunked loading to handle thousands of price points without timeouts
                $matches = collect();
                $chunkSize = 5; // Process products in very small batches

            $baseQuery = Product::query()
                ->select([
                    'id',
                    'name',
                    'slug',
                    'platform',
                    'category',
                    'release_date',
                    'updated_at',
                    'popularity_score',
                    'rating',
                ])
                ->whereExists(function ($query) {
                    $query->select(\DB::raw(1))
                        ->from('sku_regions')
                        ->whereColumn('sku_regions.product_id', 'products.id')
                        ->where('sku_regions.is_active', true);
                })
                ->orderByDesc('popularity_score')
                ->orderByDesc('rating')
                ->orderByDesc('release_date');

            if ($limit) {
                $baseQuery->limit($limit);
            }

            // Get product IDs first
            $productIds = $baseQuery->pluck('id');

            // Process in chunks to avoid memory/timeout issues
            $productIds->chunk($chunkSize)->each(function ($chunk) use (&$matches) {
                $products = Product::query()
                    ->whereIn('id', $chunk)
                    ->with([
                        'platforms:id,name,code,family',
                        'skuRegions' => fn ($q) => $q
                            ->where('is_active', true)
                            ->with([
                                'latestPrice',
                                'currency:id,code,symbol',
                                'country:id,code,name',
                            ]),
                    ])
                    ->get();

                // Batch-resolve media for this chunk
                ProductMediaResolver::resolveMany($products, true);

                $chunkMatches = $products
                    ->map(fn (Product $product): array => $this->presentProductAsCompareMatch($product))
                    ->filter(fn (array $match): bool => $match['has_digital'] || $match['has_physical']);

                $matches = $matches->concat($chunkMatches);
            });

                return $matches->values()->all();
            } finally {
                // Restore previous execution time limit
                set_time_limit((int) $previousLimit);
            }
        });
    }

    /**
     * Load prioritized matches using video_game_titles as the primary entity.
     * This provides a cleaner title-centric approach where one title shows
     * pricing across all platform variants.
     *
     * @param int|null $limit
     * @return array<int, array<string, mixed>>
     */
    protected function loadPrioritizedMatchesFromTitles(?int $limit): array
    {
        $limit = $limit !== null ? max(50, $limit) : null;
        $cacheKey = $limit ? sprintf('compare:matches:titles:%d:v4', $limit) : 'compare:matches:titles:all:v4';

        return $this->compareCache()->remember($cacheKey, now()->addMinutes(10), function () use ($limit): array {
            // Temporarily increase execution time for cache building
            $previousLimit = ini_get('max_execution_time');
            set_time_limit(300); // 5 minutes for cache warmup

            try {
                $matches = collect();
                $chunkSize = 5; // Process titles in very small batches

                // Query video_game_titles with their related video_games and pricing
                // IMPORTANT: Only include titles that have videos AND cover images
                $baseQuery = \App\Models\VideoGameTitle::query()
                    ->select([
                        'video_game_titles.id',
                        'video_game_titles.raw_title',
                        'video_game_titles.normalized_title',
                        'video_game_titles.video_game_id',
                        // Extract user rating from video_games metadata (IGDB aggregated_rating)
                        \DB::raw("COALESCE(
                            CAST(video_games.metadata->'sources'->'igdb'->>'aggregated_rating' AS FLOAT),
                            CAST(video_games.metadata->'sources'->'rawg'->>'rating' AS FLOAT) * 20,
                            CAST(products.metadata->'sources'->'igdb'->>'aggregated_rating' AS FLOAT),
                            products.popularity_score,
                            products.rating,
                            0
                        ) as user_rating"),
                    ])
                    ->join('video_games', 'video_game_titles.video_game_id', '=', 'video_games.id')
                    ->join('products', 'video_games.product_id', '=', 'products.id')
                    ->whereExists(function ($query) {
                        $query->select(\DB::raw(1))
                            ->from('sku_regions')
                            ->whereColumn('sku_regions.product_id', 'products.id')
                            ->where('sku_regions.is_active', true);
                    })
                    ->whereRaw("(products.metadata->'sources'->'igdb'->>'videos') IS NOT NULL")
                    ->whereRaw("json_array_length(products.metadata->'sources'->'igdb'->'videos') > 0")
                    // Require cover/artwork in metadata (screenshots array must have at least one entry)
                    ->whereRaw("(
                        (products.metadata->'sources'->'igdb'->>'cover') IS NOT NULL
                        OR json_array_length(COALESCE(products.metadata->'sources'->'igdb'->'screenshots', '[]'::json)) > 0
                    )")
                    ->groupBy('video_game_titles.id', 'video_game_titles.raw_title', 'video_game_titles.normalized_title', 'video_game_titles.video_game_id', 'video_games.id', 'products.id', 'products.popularity_score', 'products.rating', 'products.release_date')
                    ->orderByDesc('user_rating')
                    ->orderByDesc('products.release_date');

                if ($limit) {
                    $baseQuery->limit($limit);
                }

                // Get title IDs first
                $titleIds = $baseQuery->pluck('video_game_titles.id');

                // Process in chunks to avoid memory/timeout issues
                $titleIds->chunk($chunkSize)->each(function ($chunk) use (&$matches) {
                    $titles = \App\Models\VideoGameTitle::query()
                        ->whereIn('id', $chunk)
                        ->with([
                            'videoGame.product' => fn ($q) => $q->with([
                                'platforms:id,name,code,family',
                                'skuRegions' => fn ($q) => $q
                                    ->where('is_active', true)
                                    ->with([
                                        'latestPrice',
                                        'currency:id,code,symbol',
                                        'country:id,code,name',
                                    ]),
                            ]),
                        ])
                        ->get();

                    // Batch-resolve media for all products
                    $products = $titles->map(fn ($title) => $title->videoGame?->product)->filter();
                    ProductMediaResolver::resolveMany($products, true);

                    $chunkMatches = $titles
                        ->map(fn (\App\Models\VideoGameTitle $title): ?array => $this->presentTitleAsCompareMatch($title))
                        ->filter()
                        ->filter(fn (array $match): bool => $match['has_digital'] || $match['has_physical']);

                    $matches = $matches->concat($chunkMatches);
                });

                return $matches->values()->all();
            } finally {
                // Restore previous execution time limit
                set_time_limit((int) $previousLimit);
            }
        });
    }

    /**
     * Present a VideoGameTitle with its pricing data as a compare match entry.
     *
     * @return array<string, mixed>|null
     */
    protected function presentTitleAsCompareMatch(\App\Models\VideoGameTitle $title): ?array
    {
        $videoGame = $title->videoGame;
        if (! $videoGame || ! $videoGame->product) {
            return null;
        }

        $product = $videoGame->product;
        $media = ProductMediaResolver::resolve($product);
        $cover = $media->primaryCoverImage();

        // Load videos from product metadata and convert YouTube URLs to embed format
        $videos = collect($product->metadata['sources']['igdb']['videos'] ?? [])
            ->map(function ($video) {
                $url = $video['url'] ?? null;

                // Convert YouTube watch URLs to embed URLs for iframe playback
                if ($url && str_contains($url, 'youtube.com/watch')) {
                    parse_str(parse_url($url, PHP_URL_QUERY), $params);
                    $videoId = $params['v'] ?? null;
                    if ($videoId) {
                        $url = "https://www.youtube.com/embed/{$videoId}";
                    }
                } elseif ($url && preg_match('/youtu\.be\/([a-zA-Z0-9_-]+)/', $url, $matches)) {
                    // Handle youtu.be short URLs
                    $url = "https://www.youtube.com/embed/{$matches[1]}";
                }

                return [
                    'url' => $url,
                    'provider' => 'youtube',
                    'type' => 'video',
                    'title' => $video['name'] ?? null,
                    'thumbnail_url' => $video['thumbnail_url'] ?? null,
                ];
            })
            ->filter(fn ($video) => ! empty($video['url']))
            ->take(5)
            ->values()
            ->all();

        // Group SKU regions by region/currency
        $skuRegions = $product->skuRegions;
        $digitalOffers = collect();
        $currencies = collect();

        foreach ($skuRegions as $skuRegion) {
            $latestPrice = $skuRegion->latestPrice;
            if (! $latestPrice) {
                continue;
            }

            $currencyCode = $skuRegion->currency?->code ?? 'USD';
            $currencies->push($currencyCode);

            $digitalOffers->push([
                'region' => $skuRegion->region_code,
                'currency' => $currencyCode,
                'amount' => (float) $latestPrice->fiat_amount,
                'btc_value' => (float) $latestPrice->btc_value,
                'retailer' => $skuRegion->retailer,
                'url' => $skuRegion->metadata['url'] ?? null,
            ]);
        }

        // Find best (lowest) digital price
        $bestDigital = $digitalOffers->sortBy('btc_value')->first();

        // Group offers by currency and find best price per currency
        $currencyPrices = $digitalOffers->groupBy('currency')->map(function ($offers, $currencyCode) {
            $bestOffer = $offers->sortBy('btc_value')->first();
            return [
                'code' => $currencyCode,
                'symbol' => $this->getCurrencySymbol($currencyCode),
                'amount' => $bestOffer['amount'],
                'formatted' => number_format($bestOffer['amount'], 2),
            ];
        })->values()->all();

        // Build digital pricing payload
        $digitalPayload = [
            'best' => $bestDigital,
            'offers' => $digitalOffers->values()->all(),
            'currencies' => $currencyPrices,
        ];

        // Platform labels
        $platformLabels = $product->platforms->pluck('family')->unique()->filter()->values()->all();

        // Extract rating from metadata - prioritize video_games metadata over product metadata
        $rating = null;
        if (isset($videoGame->metadata['sources']['igdb']['aggregated_rating'])) {
            $rating = $videoGame->metadata['sources']['igdb']['aggregated_rating'];
        } elseif (isset($videoGame->metadata['sources']['rawg']['rating'])) {
            // RAWG rating is 0-5 scale, convert to 0-100 for consistency
            $rating = $videoGame->metadata['sources']['rawg']['rating'] * 20;
        } elseif (isset($product->metadata['sources']['igdb']['aggregated_rating'])) {
            $rating = $product->metadata['sources']['igdb']['aggregated_rating'];
        } elseif (isset($product->metadata['user_rating'])) {
            $rating = $product->metadata['user_rating'];
        } elseif (isset($product->metadata['product_star_rating'])) {
            $rating = $product->metadata['product_star_rating'];
        } elseif ($product->rating) {
            $rating = $product->rating;
        }

        return [
            'product_id' => $product->id,
            'product_slug' => $product->slug,
            'title_id' => $title->id,
            'name' => $title->raw_title,  // Use title name instead of product name
            'normalized_title' => $title->normalized_title,
            'image' => $cover?->url ?? asset('images/placeholders/game-cover.svg'),
            'videos' => $videos,  // Include videos for playback
            'has_videos' => ! empty($videos),
            'has_digital' => $digitalOffers->isNotEmpty(),
            'has_physical' => false,
            'platforms' => $platformLabels,
            'currencies' => $currencies->unique()->values()->all(),
            'digital' => $digitalPayload,
            'physical' => [],
            'best_digital' => $bestDigital,
            'best_physical' => null,
            'rating' => $rating,
            'normalized_key' => $title->normalized_title ?? \Str::slug($title->raw_title, '-'),
            'normalized' => $title->normalized_title ?? \Str::slug($title->raw_title, '-'),
            'updated_at' => $product->updated_at?->toIso8601String() ?? now()->toIso8601String(),
        ];
    }

    /**
     * Present a Product with its pricing data as a compare match entry.
     *
     * @return array<string, mixed>
     */
    protected function presentProductAsCompareMatch(Product $product): array
    {
        $media = ProductMediaResolver::resolve($product);
        $cover = $media->primaryCoverImage();

        // Group SKU regions by region/currency
        $skuRegions = $product->skuRegions;
        $digitalOffers = collect();
        $currencies = collect();

        foreach ($skuRegions as $skuRegion) {
            $latestPrice = $skuRegion->latestPrice;
            if (! $latestPrice) {
                continue;
            }

            $currencyCode = $skuRegion->currency?->code ?? 'USD';
            $currencies->push($currencyCode);

            $digitalOffers->push([
                'region' => $skuRegion->region_code,
                'currency' => $currencyCode,
                'amount' => (float) $latestPrice->fiat_amount, // View expects 'amount' key
                'btc_value' => (float) $latestPrice->btc_value,
                'retailer' => $skuRegion->retailer,
                'url' => $skuRegion->metadata['url'] ?? null,
            ]);
        }

        // Find best (lowest) digital price
        $bestDigital = $digitalOffers->sortBy('btc_value')->first();

        // Group offers by currency and find best price per currency
        $currencyPrices = $digitalOffers->groupBy('currency')->map(function ($offers, $currencyCode) {
            $bestOffer = $offers->sortBy('btc_value')->first();
            return [
                'code' => $currencyCode,
                'symbol' => $this->getCurrencySymbol($currencyCode),
                'amount' => $bestOffer['amount'],
                'formatted' => number_format($bestOffer['amount'], 2),
            ];
        })->values()->all();

        // Build digital pricing payload
        $digitalPayload = [
            'best' => $bestDigital,
            'offers' => $digitalOffers->values()->all(),
            'currencies' => $currencyPrices,
        ];

        // Platform labels
        $platformLabels = $product->platforms->pluck('family')->unique()->filter()->values()->all();

        return [
            'product_id' => $product->id,
            'product_slug' => $product->slug,
            'name' => $product->name,
            'image' => $cover?->url ?? asset('images/placeholders/game-cover.svg'),
            'has_digital' => $digitalOffers->isNotEmpty(),
            'has_physical' => false, // Physical pricing not yet implemented
            'platforms' => $platformLabels,
            'currencies' => $currencies->unique()->values()->all(),
            'digital' => $digitalPayload,
            'physical' => [], // Physical pricing not yet implemented
            'best_digital' => $bestDigital,
            'best_physical' => null,
            'normalized_key' => Str::slug($product->name, '-'),
            'normalized' => Str::slug($product->name, '-'),
            'updated_at' => $product->updated_at?->toIso8601String() ?? now()->toIso8601String(),
        ];
    }

    /**
     * Get currency symbol for a currency code.
     */
    protected function getCurrencySymbol(string $code): string
    {
        return match (strtoupper($code)) {
            'USD' => '$',
            'EUR' => '€',
            'GBP' => '£',
            'JPY' => '¥',
            'CAD' => 'C$',
            'AUD' => 'A$',
            default => $code,
        };
    }

    /**
     * @param int|null $limit
     * @return array<int, array<string, mixed>>
     */
    protected function loadPrioritizedMatchesFromRuntime(?int $limit): array
    {
        $limit = $limit !== null ? max(50, $limit) : null;

        $cacheKey = $limit
            ? sprintf('compare:matches:runtime:%d:v1', $limit)
            : 'compare:matches:runtime:all:v1';

        return $this->compareCache()->remember($cacheKey, now()->addMinutes(10), function () use ($limit): array {
            try {
                /** @var PriceCrossReferencer $referencer */
                $referencer = app(PriceCrossReferencer::class);
                $rows = $referencer->build();
            } catch (\Throwable $e) {
                report($e);
                return [];
            }

            if ($rows->isEmpty()) {
                return [];
            }

            $matches = $rows
                ->map(fn (array $row): array => $this->presentRuntimeCrossReferenceRow($row))
                ->sortBy([
                    [static fn (array $row) => (bool) ($row['has_digital'] ?? false), 'desc'],
                    [static fn (array $row) => (bool) ($row['has_physical'] ?? false), 'desc'],
                    [static fn (array $row) => strtolower((string) ($row['name'] ?? '')), 'asc'],
                ])
                ->values();

            if ($limit !== null) {
                $matches = $matches->take($limit);
            }

            return $matches->values()->all();
        });
    }

    /**
     * Normalize a runtime row from {@see PriceCrossReferencer} into the same payload shape
     * that the compare page expects from {@see CrossReferenceEntry}.
     *
     * @param array<string, mixed> $row
     * @return array<string, mixed>
     */
    protected function presentRuntimeCrossReferenceRow(array $row): array
    {
        $digital = $row['digital'] ?? [];
        if (! is_array($digital)) {
            $digital = [];
        }

        $digitalCurrencies = [];

        if (isset($digital['currencies']) && is_array($digital['currencies'])) {
            $digital['currencies'] = array_values($digital['currencies']);
            $digitalCurrencies = collect($digital['currencies'])
                ->filter(fn ($item) => is_array($item) && is_string($item['code'] ?? null) && trim((string) $item['code']) !== '')
                ->map(fn (array $item) => strtoupper(trim((string) $item['code'])))
                ->unique()
                ->values()
                ->all();
        } else {
            $digital['currencies'] = [];
        }

        $physical = $row['physical'] ?? [];
        if (! is_array($physical)) {
            $physical = [];
        }

        $currencies = $digitalCurrencies;

        // Price guide rows are currently USD-based; surface USD for filtering when physical data exists.
        if (! empty($physical) && ! in_array('USD', $currencies, true)) {
            $currencies[] = 'USD';
        }

        $normalized = $row['normalized'] ?? null;
        $normalized = is_string($normalized) ? $normalized : null;

        return [
            'normalized_key' => $normalized,
            'normalized' => $normalized,
            'product_id' => $row['product_id'] ?? null,
            'product_slug' => $row['product_slug'] ?? null,
            'name' => (string) ($row['name'] ?? ''),
            'image' => $row['image'] ?? asset('images/placeholders/game-cover.svg'),
            'has_digital' => (bool) ($row['has_digital'] ?? false),
            'has_physical' => (bool) ($row['has_physical'] ?? false),
            'platforms' => is_array($row['platforms'] ?? null) ? $row['platforms'] : [],
            'currencies' => $currencies,
            'digital' => $digital,
            'physical' => $physical,
            'best_digital' => is_array($digital) ? ($digital['best'] ?? null) : null,
            'best_physical' => $row['best_physical'] ?? null,
            'updated_at' => now()->toIso8601String(),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    protected function presentCrossReferenceEntry(CrossReferenceEntry $entry): array
    {
        $digital = $entry->digital_payload ?? [];

        if (is_array($digital)) {
            $digital['currencies'] = array_values($digital['currencies'] ?? []);
        } else {
            $digital = [];
        }

        $physical = $entry->physical_payload ?? [];

        return [
            'normalized_key' => $entry->normalized_key,
            'normalized' => $entry->normalized_key,
            'name' => $entry->name,
            'image' => $entry->image_url,
            'has_digital' => (bool) $entry->has_digital,
            'has_physical' => (bool) $entry->has_physical,
            'platforms' => $entry->platforms ?? [],
            'currencies' => $entry->currencies ?? [],
            'digital' => $digital,
            'physical' => is_array($physical) ? $physical : [],
            'best_digital' => $entry->best_digital,
            'best_physical' => $entry->best_physical,
            'updated_at' => optional($entry->updated_at)->toIso8601String(),
        ];
    }

    /**
     * @param array<int, array<string, mixed>> $matches
     * @return string[]
     */
    protected function extractPlatformFilters(array $matches): array
    {
        return collect($matches)
            ->flatMap(fn (array $match) => $this->normalizeStringList($match['platforms'] ?? []))
            ->unique()
            ->sort()
            ->values()
            ->all();
    }

    /**
     * @param array<int, array<string, mixed>> $matches
     * @return string[]
     */
    protected function extractCurrencyFilters(array $matches): array
    {
        $codes = [];

        foreach ($matches as $match) {
            foreach (Arr::wrap($match['currencies'] ?? []) as $currency) {
                if (is_string($currency) && trim($currency) !== '') {
                    $codes[] = $currency;
                }
            }

            $digitalCurrencies = data_get($match, 'digital.currencies', []);

            foreach (Arr::wrap($digitalCurrencies) as $currency) {
                if (is_array($currency)) {
                    $code = $currency['code'] ?? null;
                    if (is_string($code) && trim($code) !== '') {
                        $codes[] = $code;
                    }
                    continue;
                }

                if (is_string($currency) && trim($currency) !== '') {
                    $codes[] = $currency;
                }
            }

            // Price guide rows are currently USD-based; ensure filter parity even when stored payloads omit USD.
            if ((bool) ($match['has_physical'] ?? false)) {
                $codes[] = 'USD';
            }
        }

        return collect($codes)
            ->map(static fn ($code) => strtoupper(trim((string) $code)))
            ->filter()
            ->unique()
            ->sort()
            ->values()
            ->all();
    }

    /**
     * @param mixed $value
     * @return string[]
     */
    protected function normalizeStringList(mixed $value): array
    {
        $values = [];

        foreach (Arr::wrap($value) as $key => $item) {
            if (is_string($key) && ! is_int($key)) {
                $values[] = (string) $key;
            }

            if (is_scalar($item)) {
                $values[] = (string) $item;
            } elseif (is_array($item)) {
                $values = array_merge($values, $this->normalizeStringList($item));
            }
        }

        return collect($values)
            ->map(fn ($item) => trim((string) $item))
            ->filter()
            ->values()
            ->all();
    }

    /**
     * @param mixed $value
     */
    protected function normalizeFrontendLimit(mixed $value): ?int
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value) && strtolower(trim($value)) === 'all') {
            return null;
        }

        $intValue = (int) $value;

        return $intValue > 0 ? $intValue : null;
    }
}
