<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\Providers\PlayStationStoreProvider;
use Illuminate\Console\Command;
use PlaystationStoreApi\Enum\CategoryEnum;

class IngestPlayStationStore extends Command
{
    protected $signature = 'ingest:playstation
        {--regions=en-us : Comma-separated regions (e.g., en-us,en-gb,ja-jp)}
        {--category=ps5-games : Category to fetch (ps5-games, ps4-games, etc.)}
        {--max-pages=1 : Maximum pages to fetch from catalog}';

    protected $description = 'Ingest game prices from PlayStation Store across multiple regions';

    public function handle(): int
    {
        $regionsInput = $this->option('regions');
        $regions = array_map('trim', explode(',', $regionsInput));
        $category = $this->resolveCategoryEnum($this->option('category'));
        $maxPages = (int) $this->option('max-pages');

        $this->info('Starting PlayStation Store ingestion...');
        $this->info('Regions: '.implode(', ', $regions));
        $this->info("Category: {$this->option('category')}");
        $this->info("Max pages: {$maxPages}");
        $this->newLine();

        try {
            $provider = new PlayStationStoreProvider($regions);

            $this->info('Fetching catalog from primary region ('.$regions[0].')...');
            $this->info('Will fetch pricing from '.count($regions).' region(s)...');
            $this->newLine();

            $result = $provider->ingestProducts([
                'category' => $category,
                'max_pages' => $maxPages,
            ]);

            $this->newLine();
            $this->info('Ingestion completed successfully!');
            $this->newLine();

            $this->table(
                ['Metric', 'Count'],
                [
                    ['Games Created', $result['stats']['created']],
                    ['Games Updated', $result['stats']['updated']],
                    ['Games Skipped', $result['stats']['skipped']],
                    ['Price Records Created', $result['stats']['price_records_created']],
                    ['Regions Queried', $result['stats']['regions_queried']],
                    ['Errors', $result['stats']['errors']],
                ]
            );

            if (! empty($result['errors'])) {
                $this->newLine();
                $this->warn('Errors encountered:');
                foreach ($result['errors'] as $error) {
                    $this->error("Concept {$error['concept_id']}: {$error['error']}");
                }
            }

            return self::SUCCESS;

        } catch (\Throwable $e) {
            $this->error('Ingestion failed: '.$e->getMessage());
            $this->error($e->getTraceAsString());

            return self::FAILURE;
        }
    }

    private function resolveCategoryEnum(string $category): CategoryEnum
    {
        return match (strtolower($category)) {
            'ps5-games', 'ps5_games' => CategoryEnum::PS5_GAMES,
            'ps4-games', 'ps4_games' => CategoryEnum::PS4_GAMES,
            default => CategoryEnum::PS5_GAMES,
        };
    }
}
