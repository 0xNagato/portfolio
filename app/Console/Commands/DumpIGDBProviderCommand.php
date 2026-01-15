<?php

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Services\Media\Providers\IGDBProvider;
use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

class DumpIGDBProviderCommand extends Command
{
    protected $signature = 'media:igdb:dump {slug?}
        {--query= : Override the search query when resolving the game}
    {--igdb-id= : Resolve by a specific IGDB game id}
    {--limit=5 : Number of media samples to print}
    {--persist=true : Allow the provider to write data to the database instead of running in dry-run mode}';

    protected $description = 'Execute the IGDB media provider for a video game and dump the payload to the console.';

    public function handle(): int
    {
        $slug = $this->argument('slug');
        $query = trim((string) $this->option('query')) ?: null;
        $igdbIdOption = $this->option('igdb-id');
        $igdbId = is_numeric($igdbIdOption) ? (int) $igdbIdOption : null;
        $limit = max(1, (int) $this->option('limit'));

        if (! $slug && ! $query && $igdbId === null) {
            $this->error('Provide either a video game slug, a --query, or an --igdb-id option.');

            return self::FAILURE;
        }

        $videoGame = $slug
            ? VideoGame::query()->where('slug', $slug)->first()
            : null;

        if (! $videoGame) {
            $seedName = $query ?: 'IGDB Sample Game';
            $seedSlug = $slug ?: Str::slug($seedName.'-'.Str::random(6));

            // Create a minimal VideoGame; the model hook will create a backing Product if needed.
            $videoGame = VideoGame::query()->firstOrCreate(
                ['slug' => $seedSlug],
                [
                    'title' => Str::headline($seedName),
                    'metadata' => [],
                    'external_ids' => [],
                ]
            );

            $this->info(sprintf('Created placeholder video game with slug [%s] for the run.', $videoGame->slug));
        }

        $context = array_filter([
            'query' => $query,
            'igdb_id' => $igdbId,
        ], fn ($value) => $value !== null && $value !== '');

        $dryRun = ! (bool) $this->option('persist');

        $options = array_merge(
            config('media.providers.igdb.options'),
            [
                'cache_lifetime' => 0,
                'dry_run' => $dryRun,
            ],
        );

        /** @var IGDBProvider $provider */
        $provider = app(VideoGameSource::class, ['options' => $options]);

        $this->info('Executing IGDB provider fetch…');
        if ($dryRun) {
            $this->line('Running in dry-run mode (no database writes).');
        }

        $media = $provider->fetch($videoGame, $context);

        $videoGame->refresh();

        $igdbMetadata = Arr::get($videoGame->metadata, 'igdb', []);

        $firstMedia = $media->first();
        $firstMediaMetadata = $firstMedia ? $firstMedia->metadata : [];

        $resolvedIgdbId = $igdbId
            ?? (is_numeric(Arr::get($igdbMetadata, 'id')) ? (int) Arr::get($igdbMetadata, 'id') : null)
            ?? (is_numeric(Arr::get($firstMediaMetadata, 'igdb_game_id'))
                ? (int) Arr::get($firstMediaMetadata, 'igdb_game_id')
                : null);

        $this->line(json_encode([
            'input' => [
                'video_game_slug' => $videoGame->slug,
                'context' => $context,
            ],
            'media' => [
                'count' => $media->count(),
                'samples' => $media->take($limit)->map(fn ($item) => $item->toArray())->values(),
            ],
            'video_game' => [
                'id' => $videoGame?->id,
                'title' => $videoGame?->title,
                'release_date' => $videoGame?->release_date,
                'metadata' => $igdbMetadata,
            ],
            'videoGame' => [
                'id' => $videoGame->id,
                'external_ids' => $videoGame->external_ids,
                'metadata_sources' => Arr::get($videoGame->metadata, 'sources.igdb'),
            ],
            'igdb_payload' => $this->resolveIgdbPayload($resolvedIgdbId),
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }

    private function resolveIgdbPayload(?int $igdbId): mixed
    {
        if (! $igdbId) {
            return null;
        }

        try {
            $game = VideoGame::with([
                'cover',
                'artworks',
                'screenshots',
                'genres',
                'videos',
                'companies',
                'total_rating',
                'total_rating_count',
                'user_average_rating',
                'platforms',
                'alternative_names',
                'websites',
                'user_ratings',
            ])->find($igdbId);

            return $game?->toArray();
        } catch (\Throwable $exception) {
            $this->warn(sprintf('Unable to fetch raw IGDB payload: %s', $exception->getMessage()));

            return null;
        }
    }
}
