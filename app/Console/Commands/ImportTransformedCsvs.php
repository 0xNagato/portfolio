<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;

class ImportTransformedCsvs extends Command
{
    protected $signature = 'import:transformed-csvs
                            {--batch=500 : Batch size for inserts}
                            {--skip-truncate : Skip truncating tables before import}
                            {--table= : Import only specific table}';

    protected $description = 'Import transformed CSV files with batching (respects FK order)';

    private int $batchSize;

    private array $importSequence = [
        'products' => 'products_TRANSFORMED.csv',
        'video_game_titles' => 'video_game_titles_TRANSFORMED.csv',
        'video_games' => 'video_games_TRANSFORMED.csv',
        'video_game_title_sources' => 'video_game_title_sources_TRANSFORMED.csv',
        'videos' => 'videos_TRANSFORMED.csv',
        'images' => 'images_TRANSFORMED.csv',
    ];

    public function handle(): int
    {
        $this->batchSize = (int) $this->option('batch');
        $basePath = storage_path('sqlite_exports');

        $this->info('╔═══════════════════════════════════════════════════════════════╗');
        $this->info('║   CSV Import - Batch Processing (FK-Safe Order)              ║');
        $this->info('╚═══════════════════════════════════════════════════════════════╝');
        $this->newLine();

        // Validate files exist
        $this->validateFiles($basePath);

        // Optionally truncate tables
        if (! $this->option('skip-truncate')) {
            if ($this->confirm('⚠️  Truncate existing data before import?', true)) {
                $this->truncateTables();
            }
        }

        $this->newLine();

        // Import in FK-safe order
        $specificTable = $this->option('table');

        foreach ($this->importSequence as $table => $csvFile) {
            if ($specificTable && $table !== $specificTable) {
                continue;
            }

            $this->importTable($table, $basePath.'/'.$csvFile);
        }

        $this->newLine();
        $this->info('✅ Import complete!');

        return self::SUCCESS;
    }

    private function validateFiles(string $basePath): void
    {
        $this->info('📂 Validating CSV files...');

        $missing = [];
        foreach ($this->importSequence as $table => $csvFile) {
            $fullPath = $basePath.'/'.$csvFile;
            if (! File::exists($fullPath)) {
                $missing[] = $csvFile;
            }
        }

        if (! empty($missing)) {
            $this->error('❌ Missing CSV files:');
            foreach ($missing as $file) {
                $this->line("   - $file");
            }
            exit(1);
        }

        $this->info('✅ All CSV files found');
        $this->newLine();
    }

    private function truncateTables(): void
    {
        $this->warn('⚠️  Truncating tables (reverse FK order)...');

        DB::transaction(function () {
            // Reverse order to respect FK constraints
            $tables = array_reverse(array_keys($this->importSequence));

            foreach ($tables as $table) {
                DB::table($table)->truncate();
                $this->line("   ✓ Truncated {$table}");
            }
        });

        $this->info('✅ Tables truncated');
    }

    private function importTable(string $table, string $csvPath): void
    {
        $this->info("📥 Importing {$table}...");

        $startTime = microtime(true);
        $totalRows = 0;
        $batchCount = 0;

        // Use memory-efficient line-by-line reading
        $handle = fopen($csvPath, 'r');
        if ($handle === false) {
            $this->error("   ❌ Failed to open {$csvPath}");

            return;
        }

        // Read headers
        $headers = fgetcsv($handle, 0, ',', '"', '\\');
        if ($headers === false) {
            $this->error("   ❌ Failed to read headers from {$csvPath}");
            fclose($handle);

            return;
        }

        $batch = [];

        while (($row = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
            // Handle mismatched column counts
            if (count($row) !== count($headers)) {
                $row = array_pad(array_slice($row, 0, count($headers)), count($headers), '');
            }

            // Build associative array
            $data = array_combine($headers, $row);

            // Convert empty strings to null for nullable fields
            $data = $this->normalizeRow($data);

            $batch[] = $data;

            // Insert batch when size reached
            if (count($batch) >= $this->batchSize) {
                $this->insertBatch($table, $batch);
                $batchCount++;
                $totalRows += count($batch);
                $batch = [];

                // Progress indicator
                $this->output->write("\r   → Processed {$totalRows} rows ({$batchCount} batches)");
            }
        }

        // Insert remaining rows
        if (! empty($batch)) {
            $this->insertBatch($table, $batch);
            $batchCount++;
            $totalRows += count($batch);
        }

        fclose($handle);

        $duration = round(microtime(true) - $startTime, 2);
        $this->newLine();
        $this->info("   ✅ Imported {$totalRows} rows in {$batchCount} batches ({$duration}s)");
        $this->newLine();
    }

    private function insertBatch(string $table, array $batch): void
    {
        // Use insertOrIgnore for safety (skip duplicates)
        DB::table($table)->insertOrIgnore($batch);
    }

    private function normalizeRow(array $row): array
    {
        foreach ($row as $key => $value) {
            // Convert empty strings to null
            if ($value === '' || $value === null) {
                $row[$key] = null;
                continue;
            }

            // Handle JSON fields (detect by column name) - keep as string
            if (in_array($key, ['metadata', 'providers', 'platform', 'genre', 'media', 'source_payload', 'raw_payload', 'urls'])) {
                // Keep as string, DB will cast to JSON
                continue;
            }

            // Handle boolean fields
            if (in_array($key, ['is_thumbnail'])) {
                $row[$key] = (int) filter_var($value, FILTER_VALIDATE_BOOLEAN);
                continue;
            }

            // Handle numeric fields - cast to int
            if (in_array($key, ['id', 'product_id', 'video_game_title_id', 'video_game_source_id', 'videoable_id', 'imageable_id', 'video_game_id', 'media_id', 'external_id', 'provider_item_id', 'width', 'height', 'duration', 'rating_count'])) {
                $row[$key] = $value !== null && $value !== '' ? (int) $value : null;
                continue;
            }

            // Handle decimal fields - cast to float
            if (in_array($key, ['rating'])) {
                $row[$key] = $value !== null && $value !== '' ? (float) $value : null;
                continue;
            }

            // Handle date/datetime fields - convert to Carbon instances
            if (in_array($key, ['created_at', 'updated_at', 'release_date', 'published_at', 'fetched_at'])) {
                try {
                    $row[$key] = $value ? \Carbon\Carbon::parse($value) : null;
                } catch (\Exception $e) {
                    $row[$key] = null;
                }
                continue;
            }

            // All other fields stay as strings (text, slugs, etc.)
            // Laravel Query Builder will properly quote them
        }

        return $row;
    }
}
