<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class IgdbFetchFilteredDumpCommand extends Command
{
    protected $signature = 'igdb:fetch:filtered
                           {--from-date=2015-01-01 : Start date (Y-m-d)}
                           {--to-date=2026-01-13 : End date (Y-m-d)}
                           {--limit=10000 : Maximum games to fetch (IGDB API allows max 500 per request)}
                           {--output-dir=igdb-dumps : Directory to save filtered dumps}
                           {--min-rating=0 : Minimum IGDB rating (0-100)}
                           {--min-hypes=0 : Minimum hypes (pre-release follows)}
                           {--sort-by=hypes : Sort field (hypes, rating, first_release_date)}';

    protected $description = 'Fetch filtered IGDB games by date range with popularity metrics (uses live API - dumps don\'t support filtering)';

    private string $baseUrl = 'https://api.igdb.com/v4';

    private ?string $accessToken = null;

    private int $requestCount = 0;

    private int $maxRequestsPerSecond = 4; // IGDB rate limit

    public function handle(): int
    {
        if (! $this->obtainAccessToken()) {
            return self::FAILURE;
        }

        $fromDate = strtotime($this->option('from-date'));
        $toDate = strtotime($this->option('to-date'));
        $limit = (int) $this->option('limit');
        $minRating = (int) $this->option('min-rating');
        $minHypes = (int) $this->option('min-hypes');
        $sortBy = $this->option('sort-by');

        if (! $fromDate || ! $toDate) {
            $this->error('Invalid date format. Use Y-m-d (e.g., 2015-01-01)');

            return self::FAILURE;
        }

        $this->info('🎮 Fetching IGDB games released between '.date('Y-m-d', $fromDate).' and '.date('Y-m-d', $toDate));
        if ($minRating > 0) {
            $this->info("   Minimum rating: {$minRating}");
        }
        if ($minHypes > 0) {
            $this->info("   Minimum hypes: {$minHypes}");
        }
        $this->info("   Sorted by: {$sortBy} desc");
        $this->newLine();

        // Prepare output directory
        $outputDir = storage_path($this->option('output-dir'));
        if (! is_dir($outputDir)) {
            mkdir($outputDir, 0755, true);
        }

        $timestamp = time();
        $gamesFile = "{$outputDir}/{$timestamp}_games.csv";
        $coversFile = "{$outputDir}/{$timestamp}_covers.csv";
        $screenshotsFile = "{$outputDir}/{$timestamp}_screenshots.csv";
        $artworksFile = "{$outputDir}/{$timestamp}_artworks.csv";
        $videosFile = "{$outputDir}/{$timestamp}_game_videos.csv";
        $websitesFile = "{$outputDir}/{$timestamp}_websites.csv";

        // Fetch games (includes nested covers, screenshots, videos, websites)
        $games = $this->fetchGames($fromDate, $toDate, $limit, $minRating, $minHypes, $sortBy);

        if (empty($games)) {
            $this->error('No games found for the specified date range and filters.');

            return self::FAILURE;
        }

        $this->info("✓ Fetched {$games->count()} games");
        $this->newLine();

        // Export games to CSV
        $this->exportGamesToCsv($games, $gamesFile);

        // Extract nested media from games response (already included!)
        $this->info('📸 Extracting nested media from games response...');
        $this->extractAndExportNestedMedia($games, $coversFile, $screenshotsFile, $artworksFile, $videosFile, $websitesFile);

        $this->newLine();
        $this->info('✅ Filtered dump created successfully!');
        $this->newLine();
        $this->info('Next steps:');
        $this->line("  Import: php artisan gc:import-igdb --path={$this->option('output-dir')}");

        return self::SUCCESS;
    }

    private function obtainAccessToken(): bool
    {
        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            $this->error('IGDB_CLIENT_ID and IGDB_CLIENT_SECRET must be set in .env');

            return false;
        }

        $this->info('🔑 Obtaining OAuth token from Twitch...');

        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            $this->error('Failed to obtain OAuth token: '.$response->body());

            return false;
        }

        $this->accessToken = $response->json('access_token');
        $this->info('✓ OAuth token obtained');
        $this->newLine();

        return true;
    }

    private function fetchGames(int $fromDate, int $toDate, int $limit, int $minRating, int $minHypes, string $sortBy): \Illuminate\Support\Collection
    {
        $allGames = collect();
        $offset = 0;
        $perPage = 500; // IGDB max per request
        $bar = $this->output->createProgressBar($limit);

        while ($offset < $limit) {
            $this->rateLimit();

            $fields = implode(',', [
                'id', 'name', 'slug', 'url', 'created_at', 'updated_at',
                'summary', 'storyline', 'collection', 'franchise', 'franchises',
                'hypes', 'follows', 'rating', 'aggregated_rating', 'aggregated_rating_count',
                'total_rating', 'total_rating_count', 'rating_count', 'parent_game',
                'version_parent', 'version_title', 'similar_games', 'tags', 'game_engines',
                'category', 'player_perspectives', 'game_modes', 'keywords', 'themes',
                'genres', 'expansions', 'dlcs', 'bundles', 'standalone_expansions',
                'first_release_date', 'status', 'platforms', 'release_dates',
                'alternative_names',
                // Nested media (returned as arrays in single request!)
                'screenshots.*',  // All screenshot data
                'videos.*',       // All video data
                'cover.*',        // Cover details
                'websites.*',     // Website URLs
                'artworks.*',     // Artwork data
                'external_games', 'multiplayer_modes', 'involved_companies', 'age_ratings',
                'checksum', 'remakes', 'remasters', 'expanded_games', 'ports',
                'forks', 'language_supports', 'game_localizations', 'collections',
            ]);

            // Build WHERE clause with filters
            $whereConditions = [
                "first_release_date >= {$fromDate}",
                "first_release_date <= {$toDate}",
            ];

            if ($minRating > 0) {
                $whereConditions[] = "rating >= {$minRating}";
            }

            if ($minHypes > 0) {
                $whereConditions[] = "hypes >= {$minHypes}";
            }

            $whereClause = implode(' & ', $whereConditions);

            $query = "fields {$fields}; ".
                     "where {$whereClause}; ".
                     "sort {$sortBy} desc; ".
                     "limit {$perPage}; ".
                     "offset {$offset};";

            $response = Http::withHeaders([
                'Client-ID' => config('services.igdb.client_id'),
                'Authorization' => 'Bearer '.$this->accessToken,
            ])->withBody($query, 'text/plain')->post("{$this->baseUrl}/games");

            if (! $response->successful()) {
                $this->error("API request failed at offset {$offset}: ".$response->body());
                break;
            }

            $games = collect($response->json());

            if ($games->isEmpty()) {
                break;
            }

            $allGames = $allGames->concat($games);
            $bar->advance($games->count());

            $offset += $perPage;

            if ($games->count() < $perPage) {
                break; // Last page
            }
        }

        $bar->finish();
        $this->newLine();

        return $allGames;
    }

    /**
     * Extract nested media from games response (no additional API calls needed!)
     * Games endpoint returns: cover (object), screenshots (array), videos (array),
     * websites (array), artworks (array)
     */
    private function extractAndExportNestedMedia(
        \Illuminate\Support\Collection $games,
        string $coversFile,
        string $screenshotsFile,
        string $artworksFile,
        string $videosFile,
        string $websitesFile
    ): void {
        $covers = collect();
        $screenshots = collect();
        $artworks = collect();
        $videos = collect();
        $websites = collect();

        foreach ($games as $game) {
            $gameId = $game['id'];

            // Extract cover (single object)
            if (isset($game['cover']) && is_array($game['cover'])) {
                $cover = $game['cover'];
                $cover['game'] = $gameId; // Add game reference
                $covers->push($cover);
            }

            // Extract screenshots (array - can be multiple per game!)
            if (isset($game['screenshots']) && is_array($game['screenshots'])) {
                foreach ($game['screenshots'] as $screenshot) {
                    $screenshot['game'] = $gameId;
                    $screenshots->push($screenshot);
                }
            }

            // Extract artworks (array)
            if (isset($game['artworks']) && is_array($game['artworks'])) {
                foreach ($game['artworks'] as $artwork) {
                    $artwork['game'] = $gameId;
                    $artworks->push($artwork);
                }
            }

            // Extract videos (array)
            if (isset($game['videos']) && is_array($game['videos'])) {
                foreach ($game['videos'] as $video) {
                    $video['game'] = $gameId;
                    $videos->push($video);
                }
            }

            // Extract websites (array) - useful for Steam links, official sites, etc.
            if (isset($game['websites']) && is_array($game['websites'])) {
                foreach ($game['websites'] as $website) {
                    $website['game'] = $gameId;
                    $websites->push($website);
                }
            }
        }

        // Export to CSV files
        if ($covers->isNotEmpty()) {
            $this->exportMediaToCsv($covers, $coversFile, 'covers');
            $this->info("  ✓ Extracted {$covers->count()} covers");
        }

        if ($screenshots->isNotEmpty()) {
            $this->exportMediaToCsv($screenshots, $screenshotsFile, 'screenshots');
            $this->info("  ✓ Extracted {$screenshots->count()} screenshots");
        }

        if ($artworks->isNotEmpty()) {
            $this->exportMediaToCsv($artworks, $artworksFile, 'artworks');
            $this->info("  ✓ Extracted {$artworks->count()} artworks");
        }

        if ($videos->isNotEmpty()) {
            $this->exportMediaToCsv($videos, $videosFile, 'game_videos');
            $this->info("  ✓ Extracted {$videos->count()} videos");
        }

        if ($websites->isNotEmpty()) {
            $this->exportMediaToCsv($websites, $websitesFile, 'websites');
            $this->info("  ✓ Extracted {$websites->count()}, websites");
            'artworks' => 'id,game,image_id,url,height,width,checksum',
            'game_videos' => 'id,game,video_id,name,checksum',
            default => '*',
        };
    }

    private function exportGamesToCsv(\Illuminate\Support\Collection $games, string $outputFile): void
    {
        $fp = fopen($outputFile, 'w');

        // Write headers
        $headers = [
            'id', 'name', 'slug', 'url', 'created_at', 'updated_at',
            'summary', 'storyline', 'collection', 'franchise', 'franchises',
            'hypes', 'follows', 'rating', 'aggregated_rating', 'aggregated_rating_count',
            'total_rating', 'total_rating_count', 'rating_count', 'parent_game',
            'version_parent', 'version_title', 'similar_games', 'tags', 'game_engines',
            'category', 'player_perspectives', 'game_modes', 'keywords', 'themes',
            'genres', 'expansions', 'dlcs', 'bundles', 'standalone_expansions',
            'first_release_date', 'status', 'platforms', 'release_dates',
            'alternative_names', 'screenshots', 'videos', 'cover', 'websites',
            'external_games', 'multiplayer_modes', 'involved_companies', 'age_ratings',
            'artworks', 'checksum', 'remakes', 'remasters', 'expanded_games', 'ports',
            'forks', 'language_supports', 'game_localizations', 'collections', 'game_status', 'game_type',
        ];
        fputcsv($fp, $headers);

        // Write rows
        foreach ($games as $game) {
            $row = [];
            foreach ($headers as $header) {
                $value = $game[$header] ?? '';

                // Convert arrays to PostgreSQL array format
                if (is_array($value)) {
                    $value = '{'.implode(',', $value).'}';
                }

                // Convert timestamps
                if ($header === 'first_release_date' && is_numeric($value)) {
                    $value = date('Y-m-d H:i:s', $value);
                }

                $row[] = $value;
            }
            fputcsv($fp, $row);
        }

        fclose($fp);
        $this->info("✓ Exported games to: {$outputFile}");
    }

    private function exportMediaToCsv(\Illuminate\Support\Collection $media, string $outputFile, string $type): void
    {
        $fp = fopen($outputFile, 'w');

        // Write headers based on media type
        $headers = match ($type) {
            'game_videos' => ['id', 'game', 'video_id', 'name', 'checksum'],
            'websites' => ['id', 'game', 'category', 'url', 'checksum', 'trusted'],
            default => ['id', 'game', 'image_id', 'url', 'height', 'width', 'checksum'],
        };

        fputcsv($fp, $headers);

        // Write rows
        foreach ($media as $item) {
            $row = [];
            foreach ($headers as $header) {
                $row[] = $item[$header] ?? '';
            }
            fputcsv($fp, $row);
        }

        fclose($fp);
    }

    private function rateLimit(): void
    {
        $this->requestCount++;

        if ($this->requestCount % $this->maxRequestsPerSecond === 0) {
            usleep(250000); // 250ms delay every 4 requests = 4 req/sec
        }
    }
}
