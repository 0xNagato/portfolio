<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Str;

class FetchIgdbPlatformsCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'gc:fetch-igdb-platforms
        {--limit=500 : IGDB page size (max 500)}
        {--out=storage/igdb-dumps/platforms.csv : Output CSV path}
    ';

    /**
     * The console command description.
     */
    protected $description = 'Fetch IGDB platform metadata via v4 API and export to CSV';

    public function handle(): int
    {
        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (empty($clientId) || empty($clientSecret)) {
            $this->error('Missing IGDB credentials. Set services.igdb.client_id and client_secret (env: IGDB_CLIENT_ID, IGDB_CLIENT_SECRET).');

            return static::FAILURE;
        }

        $limit = (int) $this->option('limit');
        $limit = max(1, min($limit, 500));
        $outPath = $this->option('out');

        $token = $this->fetchToken($clientId, $clientSecret);
        if (! $token) {
            $this->error('Unable to obtain IGDB access token.');

            return static::FAILURE;
        }

        $this->info('Fetched IGDB token. Downloading platforms...');

        $rows = $this->fetchPlatforms($token, $clientId, $limit);

        if (empty($rows)) {
            $this->warn('No platform rows returned.');

            return static::FAILURE;
        }

        $written = $this->writeCsv($outPath, $rows);
        $this->info("Wrote {$written} platform rows to {$outPath}");

        return static::SUCCESS;
    }

    protected function fetchToken(string $clientId, string $clientSecret): ?string
    {
        $response = Http::asForm()
            ->timeout(15)
            ->retry(3, 500)
            ->post('https://id.twitch.tv/oauth2/token', [
                'client_id' => $clientId,
                'client_secret' => $clientSecret,
                'grant_type' => 'client_credentials',
            ]);

        if ($response->status() >= 400) {
            $this->error('Token request failed: '.$response->body());

            return null;
        }

        return $response->json('access_token');
    }

    /**
     * @return array<int, array{id:int,name:?string,abbreviation:?string,logo_url:?string}>
     */
    protected function fetchPlatforms(string $token, string $clientId, int $limit): array
    {
        $results = [];
        $offset = 0;
        $maxPages = 50; // safety cap

        while ($maxPages-- > 0) {
            $body = sprintf(
                'fields id,name,abbreviation,platform_logo.url; sort id asc; limit %d; offset %d;',
                $limit,
                $offset
            );

            $response = Http::withHeaders([
                'Client-ID' => $clientId,
                'Authorization' => "Bearer {$token}",
            ])
                ->timeout(20)
                ->retry(3, 500)
                ->withBody($body, 'text/plain')
                ->post('https://api.igdb.com/v4/platforms');

            if (! $response->successful()) {
                $this->error('Platform request failed: '.$response->body());

                return [];
            }

            $batch = $response->json();
            $count = is_array($batch) ? count($batch) : 0;
            $this->info(sprintf('Fetched %d platform rows (offset %d)', $count, $offset));

            if ($count === 0) {
                break;
            }

            foreach ($batch as $item) {
                $results[] = [
                    'id' => (int) ($item['id'] ?? 0),
                    'name' => $item['name'] ?? null,
                    'abbreviation' => $item['abbreviation'] ?? null,
                    'logo_url' => $item['platform_logo']['url'] ?? null,
                ];
            }

            if ($count < $limit) {
                break;
            }

            $offset += $limit;
        }

        return $results;
    }

    /**
     * @param  array<int, array{id:int,name:?string,abbreviation:?string,logo_url:?string}>  $rows
     */
    protected function writeCsv(string $path, array $rows): int
    {
        $directory = dirname($path);
        if (! is_dir($directory)) {
            mkdir($directory, 0755, true);
        }

        $handle = fopen($path, 'w');
        if ($handle === false) {
            throw new \RuntimeException('Unable to open output file: '.$path);
        }

        fputcsv($handle, ['id', 'name', 'abbreviation', 'logo_url']);

        $count = 0;
        foreach ($rows as $row) {
            fputcsv($handle, [
                $row['id'],
                $row['name'] ?? '',
                $row['abbreviation'] ?? '',
                $this->expandLogoUrl($row['logo_url'] ?? null),
            ]);
            $count++;
        }

        fclose($handle);

        return $count;
    }

    protected function expandLogoUrl(?string $url): ?string
    {
        if (empty($url)) {
            return null;
        }

        // IGDB returns //images.igdb.com/... normalize to https
        if (Str::startsWith($url, '//')) {
            return 'https:'.$url;
        }

        return $url;
    }
}
