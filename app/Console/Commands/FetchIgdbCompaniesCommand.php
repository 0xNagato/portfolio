<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Http;

class FetchIgdbCompaniesCommand extends Command
{
    protected $signature = 'gc:fetch-igdb-companies
        {--limit=500 : IGDB page size (max 500)}
        {--out=storage/igdb-dumps/companies.csv : Output CSV path}
    ';

    protected $description = 'Fetch IGDB company metadata via v4 API and export to CSV';

    public function handle(): int
    {
        $clientId = (string) config('services.igdb.client_id');
        $clientSecret = (string) config('services.igdb.client_secret');

        if ($clientId === '' || $clientSecret === '') {
            $this->error('Missing IGDB credentials. Set services.igdb.client_id and client_secret (env: IGDB_CLIENT_ID, IGDB_CLIENT_SECRET).');

            return static::FAILURE;
        }

        $limit = (int) $this->option('limit');
        $limit = max(1, min($limit, 500));
        $outPath = (string) $this->option('out');

        $token = $this->fetchToken($clientId, $clientSecret);
        if ($token === null) {
            $this->error('Unable to obtain IGDB access token.');

            return static::FAILURE;
        }

        $this->info('Fetched IGDB token. Downloading companies...');

        $rows = $this->fetchCompanies($token, $clientId, $limit);

        if ($rows === []) {
            $this->warn('No company rows returned.');

            return static::FAILURE;
        }

        $written = $this->writeCsv($outPath, $rows);
        $this->info("Wrote {$written} company rows to {$outPath}");

        return static::SUCCESS;
    }

    protected function fetchToken(string $clientId, string $clientSecret): ?string
    {
        /** @var Response $response */
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
     * @return array<int, array{id:int,name:?string}>
     */
    protected function fetchCompanies(string $token, string $clientId, int $limit): array
    {
        $results = [];
        $offset = 0;
        $maxPages = 50; // safety cap

        while ($maxPages-- > 0) {
            $body = sprintf('fields id,name; sort id asc; limit %d; offset %d;', $limit, $offset);

            /** @var Response $response */
            $response = Http::withHeaders([
                'Client-ID' => $clientId,
                'Authorization' => "Bearer {$token}",
                'User-Agent' => 'game-comapre/igdb-fetch (+local dev)',
            ])
                ->timeout(20)
                ->retry(3, 500)
                ->withBody($body, 'text/plain')
                ->post('https://api.igdb.com/v4/companies');

            if (! $response->successful()) {
                $this->error('Companies request failed: '.$response->body());

                return [];
            }

            $batch = $response->json();
            $count = is_array($batch) ? count($batch) : 0;
            $this->info(sprintf('Fetched %d company rows (offset %d)', $count, $offset));

            if ($count === 0) {
                break;
            }

            foreach ($batch as $item) {
                $results[] = [
                    'id' => (int) ($item['id'] ?? 0),
                    'name' => $item['name'] ?? null,
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
     * @param  array<int, array{id:int,name:?string}>  $rows
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

        fputcsv($handle, ['id', 'name']);

        $count = 0;
        foreach ($rows as $row) {
            fputcsv($handle, [$row['id'], $row['name'] ?? '']);
            $count++;
        }

        fclose($handle);

        return $count;
    }
}
