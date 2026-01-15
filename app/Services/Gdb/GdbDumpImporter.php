<?php

declare(strict_types=1);

namespace App\Services\Gdb;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\IgdbRatingHelper;
use App\Services\Normalization\PlatformNormalizer;
use App\Services\Normalization\RatingNormalizer;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class GdbDumpImporter
{
    public function __construct(
        public PlatformNormalizer $platformNormalizer,
        public RatingNormalizer $ratingNormalizer,
        public IgdbRatingHelper $ratingHelper,
    ) {}

    /**
     * @return array{products:int,sources:int,titles:int,video_games:int}
     */
    public function importRecord(array $record, string $provider = 'giantbomb'): array
    {
        $externalId = (string) Arr::get($record, 'id', '');
        $name = (string) (Arr::get($record, 'name') ?? Arr::get($record, 'title') ?? '');

        if ($externalId === '' || $name === '') {
            return ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0];
        }

        $normalizedTitle = Str::of($name)->lower()->replaceMatches('/[^a-z0-9\s]+/i', '')->squish()->toString();
        $normalizedTitle = $normalizedTitle !== '' ? Str::slug($normalizedTitle) : null;

        $productSlug = Str::slug($name);

        return DB::transaction(function () use ($record, $provider, $externalId, $name, $normalizedTitle, $productSlug): array {
            $productsCreated = 0;
            $sourcesCreated = 0;
            $titlesCreated = 0;
            $gamesCreated = 0;

            $product = Product::query()->firstOrCreate(
                [
                    'slug' => $productSlug,
                ],
                [
                    'name' => $name,
                    'type' => 'video_game',
                    'title' => $name,
                    'normalized_title' => $normalizedTitle,
                ]
            );

            if ($product->wasRecentlyCreated) {
                $productsCreated++;
            }

            $source = VideoGameSource::query()->firstOrCreate(
                [
                    'provider' => $provider,
                ]
            );

            if ($source->wasRecentlyCreated) {
                $sourcesCreated++;
            }

            $title = VideoGameTitle::query()->firstOrCreate(
                [
                    'product_id' => $product->id,
                ],
                [
                    'name' => $name,
                    'normalized_title' => $normalizedTitle,
                    'slug' => $productSlug,
                    'providers' => [$provider],
                ]
            );

            if ($title->wasRecentlyCreated) {
                $titlesCreated++;
            } else {
                $existingProviders = is_array($title->providers) ? $title->providers : [];
                if (! in_array($provider, $existingProviders, true)) {
                    $title->forceFill([
                        'providers' => array_values(array_unique(array_merge($existingProviders, [$provider]))),
                    ])->save();
                }
            }

            VideoGameTitleSource::query()->updateOrCreate(
                [
                    'video_game_source_id' => $source->id,
                    'provider_item_id' => $externalId,
                ],
                [
                    'video_game_title_id' => $title->id,
                    'provider' => $provider,
                    'external_id' => (int) $externalId,
                    'raw_payload' => $record,
                ]
            );

            $platformNames = collect(Arr::get($record, 'platforms', []))
                ->map(fn ($p) => is_array($p) ? (string) Arr::get($p, 'name', '') : (string) $p)
                ->filter(fn (string $p) => $p !== '')
                ->all();

            $platformNames = $this->platformNormalizer->normalizeMany($platformNames);

            $media = $this->extractMedia($record);
            $rating = $this->ratingHelper->extractPercentage($record);
            $ratingCount = $this->ratingNormalizer->extractRatingCount($record);

            $description = Arr::get($record, 'deck')
                ?? Arr::get($record, 'description')
                ?? Arr::get($record, 'summary');

            $releaseDate = Arr::get($record, 'original_release_date')
                ?? Arr::get($record, 'release_date');

            $developer = Arr::get($record, 'developers.0.name')
                ?? Arr::get($record, 'developer');

            $publisher = Arr::get($record, 'publishers.0.name')
                ?? Arr::get($record, 'publisher');

            $genres = collect((array) Arr::get($record, 'genres', []))
                ->map(fn ($g) => is_array($g) ? (string) Arr::get($g, 'name', '') : (string) $g)
                ->filter(fn (string $g) => $g !== '')
                ->unique()
                ->values()
                ->all();

            $game = VideoGame::query()->updateOrCreate(
                [
                    'video_game_title_id' => $title->id,
                ],
                [
                    'slug' => $title->slug,
                    'name' => $name,
                    'provider' => $provider,
                    'external_id' => (int) $externalId,
                    'description' => is_string($description) ? $description : null,
                    'release_date' => is_string($releaseDate) ? $releaseDate : null,
                    'platform' => $platformNames === [] ? null : $platformNames,
                    'rating' => $rating,
                    'rating_count' => $ratingCount,
                    'developer' => is_string($developer) ? $developer : null,
                    'publisher' => is_string($publisher) ? $publisher : null,
                    'genre' => $genres === [] ? null : $genres,
                    'media' => $media,
                    'source_payload' => null,
                ]
            );

            if ($game->wasRecentlyCreated) {
                $gamesCreated++;
            }

            $source->forceFill([
                'items_count' => VideoGameTitleSource::query()
                    ->where('video_game_source_id', $source->id)
                    ->distinct()
                    ->count('provider_item_id'),
            ])->save();

            return [
                'products' => $productsCreated,
                'sources' => $sourcesCreated,
                'titles' => $titlesCreated,
                'video_games' => $gamesCreated,
            ];
        });
    }

    private function extractMedia(array $record): ?array
    {
        $image = Arr::get($record, 'image');

        if (! is_array($image)) {
            return null;
        }

        $urls = Arr::only($image, [
            'icon_url',
            'medium_url',
            'screen_url',
            'screen_large_url',
            'small_url',
            'super_url',
            'thumb_url',
            'tiny_url',
            'original_url',
        ]);

        $urls = array_filter($urls, fn ($v) => is_string($v) && $v !== '');

        return $urls === [] ? null : ['image' => $urls];
    }
}
