<?php

declare(strict_types=1);

namespace App\Services\Providers;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\PlatformNormalizer;
use App\Services\Normalization\RatingPercentHelper;
use GuzzleHttp\Client as HTTPClient;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use PlaystationStoreApi\Client;
use PlaystationStoreApi\Enum\CategoryEnum;
use PlaystationStoreApi\Enum\RegionEnum;
use PlaystationStoreApi\Request\RequestConceptById;
use PlaystationStoreApi\Request\RequestProductList;

class PlayStationStoreProvider
{
    private const PROVIDER_KEY = 'playstation_store';

    private const PROVIDER_NAME = 'PlayStation Store';

    private const API_URL = 'https://web.np.playstation.com/api/graphql/v1/';

    private VideoGameSource $providerSource;

    private PlatformNormalizer $platformNormalizer;

    private RatingPercentHelper $ratingPercentHelper;

    /**
     * @param  array<string>  $regions  List of regions to fetch pricing for (e.g., ['en-us', 'en-gb', 'ja-jp'])
     */
    public function __construct(
        private readonly array $regions = ['en-us'],
        private readonly int $timeout = 15,
        ?PlatformNormalizer $platformNormalizer = null,
        ?RatingPercentHelper $ratingPercentHelper = null,
    ) {
        // Ensure provider source exists in database
        $this->providerSource = VideoGameSource::query()->firstOrCreate(
            ['provider_key' => self::PROVIDER_KEY],
            [
                'provider' => self::PROVIDER_KEY,
                'display_name' => self::PROVIDER_NAME,
                'category' => 'store',
                'slug' => Str::slug(self::PROVIDER_NAME),
            ]
        );

        $this->platformNormalizer = $platformNormalizer ?? new PlatformNormalizer;
        $this->ratingPercentHelper = $ratingPercentHelper ?? app(RatingPercentHelper::class);
    }

    /**
     * Ingest products with multi-region pricing (avoids duplicates).
     */
    public function ingestProducts(array $options = []): array
    {
        $category = Arr::get($options, 'category', CategoryEnum::PS5_GAMES);
        $maxPages = (int) Arr::get($options, 'max_pages', 1);

        // Step 1: Fetch catalog from PRIMARY region to discover games
        $primaryRegion = $this->regions[0] ?? 'en-us';
        $conceptIds = $this->fetchCatalogConceptIds($primaryRegion, $category, $maxPages);

        $created = 0;
        $updated = 0;
        $skipped = 0;
        $errors = [];
        $priceRecordsCreated = 0;

        // Step 2: For each game, fetch pricing from ALL regions
        foreach ($conceptIds as $conceptId) {
            try {
                $result = $this->ingestConceptWithMultiRegionPricing($conceptId);

                if ($result['created']) {
                    $created++;
                } elseif ($result['updated']) {
                    $updated++;
                } else {
                    $skipped++;
                }

                $priceRecordsCreated += $result['price_records_created'];

            } catch (\Throwable $e) {
                $errors[] = [
                    'concept_id' => $conceptId,
                    'error' => $e->getMessage(),
                ];
                $skipped++;
            }
        }

        return [
            'success' => true,
            'stats' => [
                'created' => $created,
                'updated' => $updated,
                'skipped' => $skipped,
                'errors' => \count($errors),
                'price_records_created' => $priceRecordsCreated,
                'regions_queried' => \count($this->regions),
            ],
            'errors' => $errors,
            'meta' => [
                'provider' => self::PROVIDER_KEY,
                'provider_name' => self::PROVIDER_NAME,
                'regions' => $this->regions,
                'category' => $category->value ?? 'unknown',
            ],
        ];
    }

    /**
     * Fetch concept IDs from catalog (catalog discovery from single region).
     *
     * @return array<string>
     */
    private function fetchCatalogConceptIds(string $region, CategoryEnum $category, int $maxPages): array
    {
        $conceptIds = [];

        try {
            $client = $this->createClient($region);
            $request = RequestProductList::createFromCategory($category);

            for ($page = 0; $page < $maxPages; $page++) {
                $response = $client->get($request);
                $grid = $response['data']['categoryGridRetrieve'] ?? [];
                $concepts = $grid['concepts'] ?? [];

                if (empty($concepts)) {
                    break;
                }

                foreach ($concepts as $concept) {
                    if ($id = $concept['id'] ?? null) {
                        $conceptIds[] = $id;
                    }
                }

                // Check for next page
                if ($page >= $maxPages - 1) {
                    break;
                }

                $request = $request->createNextPageRequest();
            }

        } catch (\Throwable $e) {
            Log::error('PlayStation Store catalog fetch failed', [
                'region' => $region,
                'error' => $e->getMessage(),
            ]);
        }

        return $conceptIds;
    }

    /**
     * Ingest a single concept with pricing from ALL configured regions.
     */
    private function ingestConceptWithMultiRegionPricing(string $conceptId): array
    {
        // Fetch game metadata from primary region
        $primaryRegion = $this->regions[0] ?? 'en-us';
        $gameData = $this->fetchConceptMetadata($conceptId, $primaryRegion);

        if (! $gameData) {
            return ['created' => false, 'updated' => false, 'price_records_created' => 0];
        }

        $title = $gameData['title'];
        $normalizedTitle = $this->normalizeTitle($title);

        // 1. Find or create Product
        $product = Product::query()->firstOrCreate(
            ['name' => $title],
            [
                'type' => 'video_game',
                'slug' => Str::slug($title),
                'title' => $title,
                'normalized_title' => $normalizedTitle,
            ]
        );

        $wasProductCreated = $product->wasRecentlyCreated;

        // 2. Find or create VideoGameTitle
        $videoGameTitle = VideoGameTitle::query()->firstOrCreate(
            [
                'product_id' => $product->id,
                'normalized_title' => $normalizedTitle,
            ],
            [
                'name' => $title,
                'slug' => $product->slug.'-'.$product->id,
            ]
        );

        $wasTitleCreated = $videoGameTitle->wasRecentlyCreated;

        // 3. Create VideoGameTitleSource mapping
        $conceptNumericId = $this->conceptNumericId($conceptId);

        VideoGameTitleSource::query()->updateOrCreate(
            [
                'video_game_title_id' => $videoGameTitle->id,
                'video_game_source_id' => $this->providerSource->id,
                'provider' => self::PROVIDER_KEY,
                'provider_item_id' => (string) $conceptId,
            ],
            [
                'external_id' => $conceptNumericId,
                'slug' => Str::slug($title),
                'name' => $title,
                'raw_payload' => $gameData['metadata']['raw_payload'] ?? [],
            ]
        );

        // 4. Create or update VideoGame (with media data)
        $platforms = $this->platformNormalizer->normalizeMany($gameData['platform'] ?? []);
        $genres = $this->normalizeGenres($gameData['metadata']['genres'] ?? []);
        $media = $gameData['media'] ?? [];
        $ratingPercent = $gameData['metadata']['rating_percent'] ?? null;
        $ratingCount = $gameData['metadata']['rating_count'] ?? null;

        $videoGame = VideoGame::query()->updateOrCreate(
            [
                'video_game_title_id' => $videoGameTitle->id,
            ],
            [
                'slug' => $videoGameTitle->slug,
                'name' => $title,
                'provider' => self::PROVIDER_KEY,
                'external_id' => $conceptNumericId,
                'description' => $gameData['metadata']['description'] ?? null,
                'summary' => $gameData['metadata']['summary'] ?? null,
                'platform' => $platforms === [] ? null : $platforms,
                'genre' => $genres === [] ? null : $genres,
                'developer' => $gameData['metadata']['developer'] ?? null,
                'publisher' => $gameData['metadata']['publisher'] ?? null,
                'release_date' => $gameData['metadata']['release_date'] ?? null,
                'rating' => $ratingPercent,
                'rating_count' => $ratingCount,
                'media' => empty($media) ? null : $media,
                'source_payload' => $gameData['metadata']['raw_payload'] ?? null,
            ]
        );

        $wasGameCreated = $videoGame->wasRecentlyCreated;

        $this->providerSource->recordVideoGameId($videoGame->id);

        // 5. Fetch pricing from ALL regions and create price records
        $priceRecordsCreated = 0;

        foreach ($this->regions as $region) {
            $priceData = $this->fetchConceptPricing($conceptId, $region);

            if ($priceData) {
                VideoGamePrice::create([
                    'video_game_id' => $videoGame->id,
                    'currency' => $priceData['currency'],
                    'amount_minor' => $priceData['amount_minor'],
                    'recorded_at' => now(),
                    'retailer' => self::PROVIDER_NAME.' '.strtoupper($region),
                    'tax_inclusive' => $priceData['is_discounted'] ?? false,
                ]);

                $priceRecordsCreated++;
            }
        }

        return [
            'created' => $wasProductCreated || $wasTitleCreated || $wasGameCreated,
            'updated' => ! ($wasProductCreated && $wasTitleCreated && $wasGameCreated),
            'price_records_created' => $priceRecordsCreated,
        ];
    }

    /**
     * Fetch game metadata (title, platform, genres, etc.) from a concept.
     */
    private function fetchConceptMetadata(string $conceptId, string $region): ?array
    {
        try {
            $client = $this->createClient($region);
            $response = $client->get(new RequestConceptById($conceptId));
            $conceptData = $response['data']['conceptRetrieve'] ?? null;

            if (! $conceptData) {
                return null;
            }

            $defaultProduct = $conceptData['defaultProduct'] ?? null;
            if (! $defaultProduct) {
                return null;
            }

            $productId = $defaultProduct['id'] ?? null;
            $name = $conceptData['name'] ?? $defaultProduct['invariantName'] ?? null;

            if (! $productId || ! $name) {
                return null;
            }

            $platforms = $this->resolvePlatforms($conceptData, $productId);

            // Extract and normalize media data
            $media = $this->extractMediaData($conceptData['media'] ?? []);

            $rawPayload = $conceptData;
            $releaseDate = $defaultProduct['releaseDate'] ?? null;
            $publisher = $defaultProduct['publisherName'] ?? null;
            $developer = $defaultProduct['developerName'] ?? null;
            $starRating = Arr::get($defaultProduct, 'starRating', []);
            $ratingPercent = $this->ratingPercentHelper->extractPercentage($starRating, [
                ['path' => 'score', 'scale' => 5],
            ]);
            $ratingCount = Arr::get($starRating, 'count');

            return [
                'title' => $name,
                'platform' => $platforms,
                'media' => $media,
                'metadata' => [
                    'concept_id' => $conceptId,
                    'product_id' => $productId,
                    'raw_media' => $conceptData['media'] ?? [],
                    'genres' => $conceptData['genres'] ?? [],
                    'release_date' => $releaseDate ? date('Y-m-d', strtotime($releaseDate)) : null,
                    'publisher' => $publisher,
                    'developer' => $developer,
                    'rating_percent' => $ratingPercent,
                    'rating_count' => is_numeric($ratingCount) ? (int) $ratingCount : null,
                    'description' => Arr::get($defaultProduct, 'longDescription'),
                    'summary' => Arr::get($defaultProduct, 'shortDescription'),
                    'raw_payload' => $rawPayload,
                ],
            ];

        } catch (\Throwable $e) {
            Log::warning('PlayStation Store concept metadata fetch failed', [
                'concept_id' => $conceptId,
                'region' => $region,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Extract and normalize media data from PlayStation Store API response.
     *
     * @param  array<mixed>  $mediaData
     * @return array{images: array<array{url: string, type: string, role: string}>, videos: array<array{url: string, type: string, role: string}>}
     */
    private function extractMediaData(array $mediaData): array
    {
        $images = [];
        $videos = [];

        foreach ($mediaData as $item) {
            $typename = $item['__typename'] ?? null;
            $type = $item['type'] ?? null;
            $role = $item['role'] ?? 'unknown';
            $url = $item['url'] ?? null;

            if (! $url) {
                continue;
            }

            $normalized = [
                'url' => $url,
                'type' => $type ?? 'unknown',
                'role' => $role,
            ];

            // Categorize by __typename (Media can be ImageMedia or VideoMedia)
            if ($typename === 'ImageMedia' || $type === 'image') {
                $images[] = $normalized;
            } elseif ($typename === 'VideoMedia' || $type === 'video') {
                $videos[] = $normalized;
            }
        }

        return [
            'images' => $images,
            'videos' => $videos,
        ];
    }

    /**
     * Fetch pricing for a specific concept in a specific region.
     */
    private function fetchConceptPricing(string $conceptId, string $region): ?array
    {
        try {
            $client = $this->createClient($region);
            $response = $client->get(new RequestConceptById($conceptId));
            $conceptData = $response['data']['conceptRetrieve'] ?? null;

            if (! $conceptData) {
                return null;
            }

            $defaultProduct = $conceptData['defaultProduct'] ?? null;
            if (! $defaultProduct) {
                return null;
            }

            $price = $defaultProduct['price'] ?? null;
            if (! $price) {
                return null;
            }

            $currencyCode = $price['currencyCode'] ?? null;
            $discountedValue = $price['discountedValue'] ?? null;
            $basePriceValue = $price['basePriceValue'] ?? null;

            if (! $currencyCode || ($discountedValue === null && $basePriceValue === null)) {
                return null;
            }

            return [
                'currency' => $currencyCode,
                'amount_minor' => (int) ($discountedValue ?? $basePriceValue),
                'base_price' => (int) ($basePriceValue ?? 0),
                'is_discounted' => $discountedValue !== null && $discountedValue < $basePriceValue,
            ];

        } catch (\Throwable $e) {
            Log::debug('PlayStation Store pricing not available', [
                'concept_id' => $conceptId,
                'region' => $region,
            ]);

            return null;
        }
    }

    /**
     * Create a new PlayStation Store API client for a specific region.
     */
    private function createClient(string $region): Client
    {
        return new Client(
            $this->resolveRegionEnum($region),
            new HTTPClient([
                'base_uri' => self::API_URL,
                'timeout' => $this->timeout,
            ])
        );
    }

    /**
     * Extract platform from PlayStation product ID.
     */
    private function resolvePlatforms(array $conceptData, string $productId): array
    {
        $platforms = collect($conceptData['platforms'] ?? [])
            ->map(fn ($platform) => Arr::get($platform, 'name'))
            ->filter()
            ->values()
            ->all();

        if ($platforms === []) {
            $platforms = [$this->guessPlatformFromProductId($productId)];
        }

        return $platforms;
    }

    private function guessPlatformFromProductId(string $productId): string
    {
        if (str_contains($productId, 'PPSA')) {
            return 'PlayStation 5';
        }

        if (str_contains($productId, 'CUSA')) {
            return 'PlayStation 4';
        }

        return 'PlayStation';
    }

    private function normalizeGenres(array $genres): array
    {
        return collect($genres)
            ->map(fn ($genre) => is_array($genre) ? Arr::get($genre, 'name') : $genre)
            ->filter()
            ->unique()
            ->values()
            ->all();
    }

    private function conceptNumericId(string $conceptId): int
    {
        if (is_numeric($conceptId)) {
            return (int) $conceptId;
        }

        return abs(crc32($conceptId));
    }

    /**
     * Normalize title for matching (simple implementation).
     */
    private function normalizeTitle(string $title): string
    {
        return Str::lower(
            preg_replace('/[^a-z0-9]+/', '', Str::ascii($title)) ?? ''
        );
    }

    /**
     * Resolve region string to RegionEnum.
     */
    private function resolveRegionEnum(string $region): RegionEnum
    {
        $regionMap = [
            'en-us' => RegionEnum::UNITED_STATES,
            'us' => RegionEnum::UNITED_STATES,
            'en-gb' => RegionEnum::UNITED_KINGDOM,
            'uk' => RegionEnum::UNITED_KINGDOM,
            'gb' => RegionEnum::UNITED_KINGDOM,
            'ja-jp' => RegionEnum::JAPAN,
            'jp' => RegionEnum::JAPAN,
        ];

        return $regionMap[strtolower($region)]
            ?? RegionEnum::tryFrom($region)
            ?? RegionEnum::UNITED_STATES;
    }
}
