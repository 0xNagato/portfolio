<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class IgdbDownloadDumpCommand extends Command
{
    protected $signature = 'igdb:dump:download
                           {endpoint? : The IGDB endpoint to download (e.g., games, platforms, covers)}
                           {--list : List all available dumps}
                           {--output-dir=igdb-dumps : Directory to save dumps}';

    protected $description = 'Download IGDB CSV data dumps';

    private string $baseUrl = 'https://api.igdb.com/v4';

    private ?string $accessToken = null;

    private function clientId(): string
    {
        return (string) config('services.igdb.client_id');
    }

    private function clientSecret(): string
    {
        return (string) config('services.igdb.client_secret');
    }

    public function handle(): int
    {
        // Get OAuth token
        if (! $this->obtainAccessToken()) {
            return self::FAILURE;
        }

        // List dumps if requested
        if ($this->option('list')) {
            return $this->listDumps();
        }

        // Download specific endpoint
        $endpoint = $this->argument('endpoint');
        if (! $endpoint) {
            $this->error('Please specify an endpoint or use --list to see available dumps.');

            return self::FAILURE;
        }

        return $this->downloadDump($endpoint);
    }

    private function obtainAccessToken(): bool
    {
        $clientId = $this->clientId();
        $clientSecret = $this->clientSecret();

        if (! $clientId || ! $clientSecret) {
            $this->error('IGDB_CLIENT_ID and IGDB_CLIENT_SECRET must be set in .env');

            return false;
        }

        $this->info('Obtaining OAuth token from Twitch...');

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

        return true;
    }

    private function listDumps(): int
    {
        $this->info('Fetching available dumps from IGDB...');

        $clientId = $this->clientId();
        if ($clientId === '') {
            $this->error('IGDB_CLIENT_ID must be set in .env');

            return self::FAILURE;
        }

        $response = Http::withHeaders([
            'Client-ID' => $clientId,
            'Authorization' => 'Bearer '.$this->accessToken,
        ])->get("{$this->baseUrl}/dumps");

        if (! $response->successful()) {
            $this->error('Failed to fetch dumps: '.$response->body());

            return self::FAILURE;
        }

        $dumps = $response->json();

        if (empty($dumps)) {
            $this->warn('No dumps available or you do not have dump access.');

            return self::FAILURE;
        }

        $this->newLine();
        $this->info('Available IGDB Dumps:');
        $this->table(
            ['Endpoint', 'Filename', 'Updated At'],
            collect($dumps)->map(fn ($dump) => [
                $dump['endpoint'],
                $dump['file_name'],
                date('Y-m-d H:i:s', $dump['updated_at']),
            ])->toArray()
        );

        $this->newLine();
        $this->info('To download a dump, run:');
        $this->line('  php artisan igdb:dump:download <endpoint>');
        $this->newLine();

        return self::SUCCESS;
    }

    private function downloadDump(string $endpoint): int
    {
        $this->info("Fetching dump metadata for '{$endpoint}'...");

        $clientId = $this->clientId();
        if ($clientId === '') {
            $this->error('IGDB_CLIENT_ID must be set in .env');

            return self::FAILURE;
        }

        $response = Http::withHeaders([
            'Client-ID' => $clientId,
            'Authorization' => 'Bearer '.$this->accessToken,
        ])->get("{$this->baseUrl}/dumps/{$endpoint}");

        if (! $response->successful()) {
            $this->error("Failed to fetch dump for '{$endpoint}': ".$response->body());

            return self::FAILURE;
        }

        $dump = $response->json();

        $this->info('Dump Information:');
        $this->line("  Endpoint: {$dump['endpoint']}");
        $this->line("  Filename: {$dump['file_name']}");
        $this->line('  Size: '.$this->formatBytes($dump['size_bytes']));
        $this->line('  Updated: '.date('Y-m-d H:i:s', $dump['updated_at']));
        $this->line("  Schema Version: {$dump['schema_version']}");
        $this->newLine();

        // Create output directory
        $outputDir = $this->option('output-dir');
        if (! is_dir(storage_path($outputDir))) {
            mkdir(storage_path($outputDir), 0755, true);
        }

        $outputPath = storage_path("{$outputDir}/{$dump['file_name']}");

        // Download from S3
        $this->info('Downloading CSV dump from S3...');
        $this->warn('Note: S3 URL is valid for 5 minutes only');
        $this->newLine();

        $bar = $this->output->createProgressBar();
        $bar->start();

        $response = Http::timeout(300)
            ->withOptions(['sink' => $outputPath])
            ->get($dump['s3_url']);

        $bar->finish();
        $this->newLine();

        if (! $response->successful()) {
            $this->error('Failed to download dump from S3');

            return self::FAILURE;
        }

        // When Http::fake() is active, Laravel doesn't actually stream to `sink`.
        // To keep this command testable without performing real network I/O,
        // we fall back to writing the fake response body if the sink file is empty.
        if ((! file_exists($outputPath) || filesize($outputPath) === 0) && $response->body() !== '') {
            file_put_contents($outputPath, $response->body());
        }

        $this->info('✓ Dump downloaded successfully!');
        $this->line("  Location: {$outputPath}");
        $this->newLine();

        // Save schema
        $schemaPath = storage_path("{$outputDir}/{$endpoint}_schema.json");
        file_put_contents($schemaPath, json_encode($dump['schema'], JSON_PRETTY_PRINT));
        $this->info("✓ Schema saved: {$schemaPath}");
        $this->newLine();

        $this->info('Next steps:');
        $this->line("  1. Review the CSV: head -20 {$outputPath}");
        $this->line('  2. Import games dump into canonical provider staging (video_game_titles):');
        $this->line('     php artisan igdb:dump:import games');

        return self::SUCCESS;
    }

    private function formatBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $power = $bytes > 0 ? floor(log($bytes, 1024)) : 0;

        return round($bytes / pow(1024, $power), 2).' '.$units[$power];
    }
}
