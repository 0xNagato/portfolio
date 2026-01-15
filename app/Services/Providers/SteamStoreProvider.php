<?php

declare(strict_types=1);

namespace App\Services\Providers;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class SteamStoreProvider
{
    private const PROVIDER_KEY = 'steam_store';

    private const PROVIDER_NAME = 'Steam Store';

    private const API_URL = 'https://store.steampowered.com/api';

    private const RATE_LIMIT_DELAY_MS = 1500; // 200 requests per 5 minutes = ~1.5s delay

    private VideoGameSource $providerSource;

    /**
     * @param  array<string>  $countryCodes  List of country codes for pricing (e.g., ['us', 'gb', 'jp'])
     */
    public function __construct(
        private readonly array $countryCodes = ['us'],
        private readonly int $timeout = 15
    ) {
        // Ensure provider source exists in database
        $this->providerSource = VideoGameSource::query()->firstOrCreate(
            ['provider_key' => self::PROVIDER_KEY],
            [
                'display_name' => self::PROVIDER_NAME,
                'category' => 'store',
            ]
        );
    }

    /**
     * Ingest products with multi-region pricing (avoids duplicates).
     *
     * @param  array{app_ids?: array<int>}  $options
     */
    public function ingestProducts(array $options = []): array
    {
        $appIds = Arr::get($options, 'app_ids', []);

        if (empty($appIds)) {
            return [
                'success' => false,
                'message' => 'No Steam app IDs provided for ingestion',
                'stats' => [
                    'created' => 0,
                    'updated' => 0,
                    'skipped' => 0,
                    'errors' => 0,
                    'price_records_created' => 0,
                    'regions_queried' => 0,
                ],
            ];
        }

        $created = 0;
        $updated = 0;
        $skipped = 0;
        $errors = [];
        $priceRecordsCreated = 0;

        // Process each app ID
        foreach ($appIds as $appId) {
            try {
                $result = $this->ingestAppWithMultiRegionPricing((int) $appId);

                if ($result['created']) {
                    $created++;
                } elseif ($result['updated']) {
                    $updated++;
                } else {
                    $skipped++;
                }

                $priceRecordsCreated += $result['price_records_created'];

                // Rate limiting: 200 requests per 5 minutes
                usleep(self::RATE_LIMIT_DELAY_MS * 1000);

            } catch (\Throwable $e) {
                $errors[] = [
                    'app_id' => $appId,
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
                'regions_queried' => \count($this->countryCodes),
            ],
            'errors' => $errors,
            'meta' => [
                'provider' => self::PROVIDER_KEY,
                'provider_name' => self::PROVIDER_NAME,
                'country_codes' => $this->countryCodes,
                'total_app_ids' => \count($appIds),
            ],
        ];
    }

    /**
     * Ingest a single Steam app with pricing from ALL configured regions.
     */
    private function ingestAppWithMultiRegionPricing(int $appId): array
    {
        // Fetch game metadata from primary region
        $primaryCountry = $this->countryCodes[0] ?? 'us';
        $gameData = $this->fetchAppDetails($appId, $primaryCountry);

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
                'synopsis' => $gameData['metadata']['short_description'] ?? null,
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
        VideoGameTitleSource::query()->updateOrCreate(
            [
                'video_game_title_id' => $videoGameTitle->id,
                'video_game_source_id' => $this->providerSource->id,
            ],
            [
                'provider' => self::PROVIDER_KEY,
                'external_id' => (int) $appId,
                'provider_item_id' => (string) $appId,
                'raw_payload' => $gameData['metadata'] ?? [],
            ]
        );

        // 4. Create or update VideoGame (with media data)
        $videoGame = VideoGame::query()->updateOrCreate(
            ['video_game_title_id' => $videoGameTitle->id],
            [
                'slug' => $videoGameTitle->slug,
                'name' => $title,
                'provider' => self::PROVIDER_KEY,
                'external_id' => (int) $appId,
                'description' => $gameData['metadata']['detailed_description'] ?? null,
                'platform' => json_encode($gameData['platforms'] ?? ['PC']),
                'genre' => json_encode($gameData['metadata']['genres'] ?? []),
                'developer' => $gameData['metadata']['developers'][0] ?? null,
                'publisher' => $gameData['metadata']['publishers'][0] ?? null,
                'release_date' => $gameData['metadata']['release_date'] ?? null,
                'media' => json_encode([
                    'steam_store' => $gameData['media'] ?? [],
                ]),
            ]
        );

        $wasGameCreated = $videoGame->wasRecentlyCreated;

        // 5. Fetch pricing from ALL regions and create price records
        $priceRecordsCreated = 0;

        foreach ($this->countryCodes as $countryCode) {
            $priceData = $this->fetchAppPricing($appId, $countryCode);

            if ($priceData) {
                VideoGamePrice::create([
                    'video_game_id' => $videoGame->id,
                    'currency' => $priceData['currency'],
                    'amount_minor' => $priceData['amount_minor'],
                    'recorded_at' => now(),
                    'retailer' => self::PROVIDER_NAME.' '.strtoupper($countryCode),
                ]);

                $priceRecordsCreated++;
            }

            // Rate limiting between region requests
            usleep(self::RATE_LIMIT_DELAY_MS * 1000);
        }

        return [
            'created' => $wasProductCreated || $wasTitleCreated || $wasGameCreated,
            'updated' => ! ($wasProductCreated && $wasTitleCreated && $wasGameCreated),
            'price_records_created' => $priceRecordsCreated,
        ];
    }

    /**
     * Fetch app details and metadata from Steam Store API.
     */
    private function fetchAppDetails(int $appId, string $countryCode): ?array
    {
        try {
            $response = Http::timeout($this->timeout)
                ->get(self::API_URL.'/appdetails', [
                    'appids' => $appId,
                    'cc' => strtoupper($countryCode),
                    'l' => 'english',
                ])
                ->throw()
                ->json();

            $appData = $response[(string) $appId] ?? null;

            if (! $appData || ! ($appData['success'] ?? false)) {
                return null;
            }

            $data = $appData['data'] ?? null;
            if (! $data) {
                return null;
            }

            $name = $data['name'] ?? null;
            if (! $name) {
                return null;
            }

            // Extract platforms
            $platforms = [];
            if ($data['platforms']['windows'] ?? false) {
                $platforms[] = 'Windows';
            }
            if ($data['platforms']['mac'] ?? false) {
                $platforms[] = 'Mac';
            }
            if ($data['platforms']['linux'] ?? false) {
                $platforms[] = 'Linux';
            }

            // Extract media
            $media = $this->extractMediaData($data);

            // Extract genres
            $genres = array_map(fn ($genre) => $genre['description'] ?? '', $data['genres'] ?? []);

            return [
                'title' => $name,
                'platforms' => $platforms ?: ['PC'],
                'media' => $media,
                'metadata' => [
                    'app_id' => $appId,
                    'type' => $data['type'] ?? 'game',
                    'short_description' => $data['short_description'] ?? null,
                    'detailed_description' => $data['detailed_description'] ?? null,
                    'developers' => $data['developers'] ?? [],
                    'publishers' => $data['publishers'] ?? [],
                    'genres' => $genres,
                    'release_date' => isset($data['release_date']['date'])
                        ? date('Y-m-d', strtotime($data['release_date']['date']))
                        : null,
                    'steam_appid' => $data['steam_appid'] ?? $appId,
                ],
            ];

        } catch (\Throwable $e) {
            Log::warning('Steam Store app details fetch failed', [
                'app_id' => $appId,
                'country_code' => $countryCode,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Fetch pricing for a specific app in a specific region.
     */
    private function fetchAppPricing(int $appId, string $countryCode): ?array
    {
        try {
            $response = Http::timeout($this->timeout)
                ->get(self::API_URL.'/appdetails', [
                    'appids' => $appId,
                    'cc' => strtoupper($countryCode),
                    'filters' => 'price_overview',
                ])
                ->throw()
                ->json();

            $appData = $response[(string) $appId] ?? null;

            if (! $appData || ! ($appData['success'] ?? false)) {
                return null;
            }

            $data = $appData['data'] ?? null;
            $priceOverview = $data['price_overview'] ?? null;

            if (! $priceOverview) {
                // Free-to-play games don't have price_overview
                return null;
            }

            $currency = $priceOverview['currency'] ?? null;
            $finalPrice = $priceOverview['final'] ?? null;

            if (! $currency || $finalPrice === null) {
                return null;
            }

            return [
                'currency' => $currency,
                'amount_minor' => $finalPrice, // Already in cents
                'initial_price' => $priceOverview['initial'] ?? $finalPrice,
                'discount_percent' => $priceOverview['discount_percent'] ?? 0,
            ];

        } catch (\Throwable $e) {
            Log::debug('Steam Store pricing not available', [
                'app_id' => $appId,
                'country_code' => $countryCode,
            ]);

            return null;
        }
    }

    /**
     * Extract and normalize media data from Steam API response.
     */
    private function extractMediaData(array $data): array
    {
        $images = [];
        $videos = [];

        // Header image (main cover)
        if (isset($data['header_image'])) {
            $images[] = [
                'url' => $data['header_image'],
                'type' => 'image',
                'role' => 'cover',
            ];
        }

        // Screenshots
        foreach ($data['screenshots'] ?? [] as $screenshot) {
            if (isset($screenshot['path_full'])) {
                $images[] = [
                    'url' => $screenshot['path_full'],
                    'type' => 'image',
                    'role' => 'screenshot',
                ];
            }
        }

        // Movies/Trailers
        foreach ($data['movies'] ?? [] as $movie) {
            if (isset($movie['mp4']['max'])) {
                $videos[] = [
                    'url' => $movie['mp4']['max'],
                    'type' => 'video',
                    'role' => 'trailer',
                    'thumbnail' => $movie['thumbnail'] ?? null,
                ];
            }
        }

        return [
            'images' => $images,
            'videos' => $videos,
        ];
    }

    /**
     * Normalize title for matching.
     */
    private function normalizeTitle(string $title): string
    {
        return Str::lower(
            preg_replace('/[^a-z0-9]+/', '', Str::ascii($title)) ?? ''
        );
    }
}
