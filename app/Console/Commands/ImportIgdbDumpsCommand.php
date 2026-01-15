<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Image;
use App\Models\Product;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\PlatformNormalizer;
use App\Services\Normalization\IgdbRatingHelper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Symfony\Component\Console\Helper\ProgressBar;

class ImportIgdbDumpsCommand extends Command
{
    protected $signature = 'gc:import-igdb {--path= : Directory containing IGDB dump files, or a specific dump file path (e.g. *_games.csv)} {--provider=igdb : Provider key for video_game_sources.provider} {--limit=0 : Optional record limit for games} {--resume=1 : Resume from the last saved checkpoint (1/0)} {--reset-checkpoint : Ignore and delete any existing checkpoint for this import target}';

    protected $description = 'Import IGDB dump CSV/JSON files into products, sources, titles, and video games (streamed to avoid high memory).';

    /**
     * In-memory caches to reduce database lookups.
     */
    private array $productCache = [];

    private array $sourceCache = [];

    private array $titleCache = [];

    /**
     * Batch queue for bulk inserts.
     */
    private array $videoGameBatch = [];

    /**
     * Batch queue for provider-item mappings (video_game_title_sources).
     *
     * IMPORTANT: This table stores per-provider per-item IDs + payloads.
     * `video_game_sources` remains provider-level only (one row per provider).
     */
    private array $videoGameTitleSourceBatch = [];

    /**
     * Batch queue for aggregating image media per video game.
     * Structure: ['video_game_id' => ['urls' => [...], 'metadata' => [...]]]
     *
     * @var array<int, array{urls: array<int, string>, metadata: array<string, mixed>}>
     */
    private array $imageBatch = [];

    /**
     * Batch queue for aggregating video media per video game.
     * Structure: ['video_game_id' => ['urls' => [...], 'provider' => string, 'metadata' => [...]]]
     *
     * @var array<int, array{urls: array<int, string>, provider: string, metadata: array<int, mixed>}>
     */
    private array $videoBatch = [];

    private const BATCH_SIZE = 1000;

    private const MEDIA_BATCH_SIZE = 2000;

    /**
     * Maximum safe parameter count for PostgreSQL bulk operations.
     * PostgreSQL can handle ~65k parameters. We use a high but safe value.
     *
     * This limit applies to the number of columns × rows in a single batch.
     */
    private const MAX_SAFE_PARAMS = 65000;

    /**
     * Number of parsed game records to buffer before running a set-based write.
     */
    private const RECORD_BUFFER_SIZE = 500;

    private const CHECKPOINT_ROWS_INTERVAL = 100;

    private const CHECKPOINT_SECONDS_INTERVAL = 2.0;

    private ?PlatformNormalizer $platformNormalizer = null;

    private ?IgdbRatingHelper $igdbRatingHelper = null;

    /**
     * Map IGDB platform IDs to names (loaded from the platforms dump if available).
     *
     * @var array<int, string>
     */
    private array $platformIdToName = [];

    /**
     * Map IGDB genre IDs to names (loaded from the genres dump if available).
     *
     * @var array<int, string>
     */
    private array $genreIdToName = [];

    /**
     * Map IGDB company IDs to company names (loaded from the companies dump if available).
     *
     * @var array<int, string>
     */
    private array $companyIdToName = [];

    /**
     * Map IGDB involved_company IDs to company/role flags.
     *
     * @var array<int, array{company_id:int, developer:bool, publisher:bool}>
     */
    private array $involvedCompanyIdToCompanyRole = [];

    public function getName(): ?string
    {
        return 'gc:import-igdb';
    }

    public function handle(): int
    {
        // Disable query logging for performance
        DB::disableQueryLog();

        // Validate batch size configuration
        $this->validateBatchConfiguration();

        $startTime = microtime(true);
        $inputPath = (string) ($this->option('path') ?: base_path('storage/igdb-dumps'));
        $provider = (string) ($this->option('provider') ?: 'igdb');
        $limit = (int) $this->option('limit');

        $this->info('=== IGDB Import Started ===');
        $this->info("Path: {$inputPath}");
        $this->info("Provider: {$provider}");
        $this->info('Limit: '.($limit > 0 ? $limit : 'unlimited'));
        $this->newLine();

        if (! File::exists($inputPath)) {
            $this->error("Path does not exist: {$inputPath}");

            return self::FAILURE;
        }

        $directory = $inputPath;
        $explicitGamesFile = null;

        if (File::isFile($inputPath)) {
            $directory = dirname($inputPath);

            $basename = strtolower(basename($inputPath));
            $ext = strtolower(pathinfo($inputPath, PATHINFO_EXTENSION));
            $isSchemaArtifact = str_ends_with($basename, '_schema.json') || str_ends_with($basename, 'schema.json');

            if (str_contains($basename, 'games') && in_array($ext, ['csv', 'json', 'ndjson', 'jsonl'], true) && ! $isSchemaArtifact) {
                $explicitGamesFile = $inputPath;
            }
        }

        if (! File::isDirectory($directory)) {
            $this->error("Directory does not exist: {$directory}");

            return self::FAILURE;
        }

        if ($directory !== $inputPath) {
            $this->info("Resolved dump directory: {$directory}");
            $this->newLine();
        }

        $gamesFile = $explicitGamesFile ?: $this->findFile($directory, 'games');
        if (! $gamesFile) {
            $this->error('No games CSV/JSON file found.');

            return self::FAILURE;
        }

        // Load reference dumps first so games can be cross-referenced on insert.
        // Media runs last because it depends on the `video_games` rows being present.
        $this->loadPlatformIdToNameMap($directory);
        $this->loadGenreIdToNameMap($directory);
        $this->loadCompanyAndInvolvedCompanyMaps($directory);

        $this->info('📥 Importing games (streaming)...');
        $this->info("File: {$gamesFile}");
        $this->newLine();

        $processed = $this->processGamesStreaming($gamesFile, $provider, $limit);

        $this->newLine();
        $this->info("✅ Processed {$processed} game rows.");

        if ($limit > 0) {
            $this->warn('NOTE: This was a limited run (--limit set). Only that many rows were imported.');
            $this->line('Tip: omit --limit (or set --limit=0) for a full import. Use --reset-checkpoint to restart from the beginning.');
        }

        // Flush remaining batches.
        $this->flushBatches();

        $this->updateProviderItemsCount($provider);

        $this->newLine();
        $this->info('📸 Processing media files...');

        $mediaStats = [
            'covers' => $this->processMediaIfPresent($directory, 'covers', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'cover_images', true);
            }, $provider),
            'screenshots' => $this->processMediaIfPresent($directory, 'screenshots', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'screenshots', false);
            }, $provider),
            'artworks' => $this->processMediaIfPresent($directory, 'artworks', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'artworks', false);
            }, $provider),
            'videos' => $this->processMediaIfPresent($directory, 'videos', function ($g, array $row): void {
                $this->addVideoMedia($g, $row);
            }, $provider),
        ];

        $duration = round(microtime(true) - $startTime, 2);

        $this->newLine(2);
        $this->info('=== Import Complete ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Games Processed', $processed],
                ['Covers', $mediaStats['covers']],
                ['Screenshots', $mediaStats['screenshots']],
                ['Artworks', $mediaStats['artworks']],
                ['Videos', $mediaStats['videos']],
                ['Duration', "{$duration}s"],
            ]
        );

        $this->newLine();
        $this->info('=== Post-Import Steps ===');

        $this->info('🚀 Running OpenCritic import (Limit: 5)...');
        $this->call('gc:import-opencritic', ['--limit' => 5]);

        $this->info('🚀 Running Retailer Extraction...');
        $this->call('app:extract-retailers');

        return self::SUCCESS;
    }

    /**
     * Chunk an array of associative rows to stay under conservative bind/parameter limits.
     *
     * SQLite has a hard parameter limit (commonly 999). Even on MySQL/Postgres,
     * smaller chunks reduce risk during large imports.
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @return array<int, array<int, array<string, mixed>>>
     */
    private function chunkRowsForSafeParams(array $rows, int $preferredChunkSize): array
    {
        if ($rows === []) {
            return [];
        }

        $first = reset($rows);
        if (! is_array($first) || $first === []) {
            return array_chunk($rows, max(1, $preferredChunkSize));
        }

        $columnCount = count($first);
        $safeMax = (int) floor(self::MAX_SAFE_PARAMS / max(1, $columnCount));

        $chunkSize = max(1, min($preferredChunkSize, $safeMax));

        return array_chunk($rows, $chunkSize);
    }

    /**
     * Validates that BATCH_SIZE is safe for database parameter limits.
     *
     * Warns if the batch size might exceed SQLite's 999 parameter limit.
     */
    private function validateBatchConfiguration(): void
    {
        // Estimate: video_games table has ~14 columns
        // products table has ~6 columns
        // video_game_title_sources has ~5 columns
        $estimatedMaxColumns = 14;

        $paramsPerBatch = self::BATCH_SIZE * $estimatedMaxColumns;

        if ($paramsPerBatch > self::MAX_SAFE_PARAMS) {
            $this->warn(
                'BATCH_SIZE ('.self::BATCH_SIZE.") × max columns ({$estimatedMaxColumns}) = {$paramsPerBatch} params, ".
                'which exceeds the safe limit of '.self::MAX_SAFE_PARAMS.'.'
            );
            $this->warn('Consider reducing BATCH_SIZE to avoid SQLite parameter errors.');
            $this->newLine();
        }
    }

    private function updateProviderItemsCount(string $provider): void
    {
        $source = VideoGameSource::query()->where('provider', $provider)->first();
        if (! $source) {
            return;
        }

        $count = VideoGameTitleSource::query()
            ->where('video_game_source_id', $source->id)
            ->distinct()
            ->count('provider_item_id');

        $source->forceFill([
            'items_count' => $count,
        ])->save();
    }

    private function findFile(string $directory, string $basename): ?string
    {
        $candidates = [];
        foreach (File::files($directory) as $file) {
            $filename = $file->getFilename();
            $name = strtolower($filename);
            $ext = strtolower($file->getExtension());

            if (! str_contains($name, $basename)) {
                continue;
            }

            if (! in_array($ext, ['csv', 'json', 'ndjson', 'jsonl'], true)) {
                continue;
            }

            // Never treat schema artifacts as import payloads.
            if (str_ends_with($name, '_schema.json') || str_ends_with($name, 'schema.json')) {
                continue;
            }

            $candidates[] = $file;
        }

        if ($candidates === []) {
            return null;
        }

        // Prefer CSV payloads when both CSV and JSON variants exist.
        usort($candidates, function ($a, $b): int {
            $aExt = strtolower($a->getExtension());
            $bExt = strtolower($b->getExtension());

            if ($aExt !== $bExt) {
                if ($aExt === 'csv') {
                    return -1;
                }
                if ($bExt === 'csv') {
                    return 1;
                }
            }

            // Prefer "timestamped" dumps (e.g. 1767852000_games.csv) over generic names.
            $aName = strtolower($a->getFilename());
            $bName = strtolower($b->getFilename());

            $aTs = preg_match('/^(\d+)_/', $aName, $m1) === 1 ? (int) $m1[1] : 0;
            $bTs = preg_match('/^(\d+)_/', $bName, $m2) === 1 ? (int) $m2[1] : 0;
            if ($aTs !== $bTs) {
                return $bTs <=> $aTs;
            }

            // Finally, prefer larger files (likely the real payload).
            return $b->getSize() <=> $a->getSize();
        });

        return $candidates[0]->getPathname();
    }

    private function processGamesStreaming(string $file, string $provider, int $limit): int
    {
        $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));

        if ($ext === 'csv') {
            return $this->processGamesCsv($file, $provider, $limit);
        }

        return $this->processGamesJson($file, $provider, $limit);
    }

    private function processGamesCsv(string $file, string $provider, int $limit): int
    {
        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn("Could not open games file: {$file}");

            return 0;
        }

        $headers = fgetcsv($handle) ?: [];

        $resumeEnabled = (int) $this->option('resume') !== 0;
        $resetCheckpoint = (bool) $this->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        if ($limit <= 0 && $resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        $processed = 0;
        $errors = 0;
        $skipped = 0;

        $fileSize = max(1, (int) (filesize($file) ?: 1));
        $maxSteps = $limit > 0 ? $limit : $fileSize;

        $progressBar = $this->output->createProgressBar($maxSteps);
        $this->configureProgressBar($progressBar, true);
        $progressBar->setMessage('0', 'errors');
        $progressBar->setMessage('0', 'skipped');

        /** @var array<int, array<string, mixed>> $recordBuffer */
        $recordBuffer = [];

        $flushRecordBuffer = function () use (&$recordBuffer, $provider, &$errors, $progressBar): void {
            if ($recordBuffer === []) {
                return;
            }

            try {
                $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
            } catch (\Throwable $e) {
                Log::error('Failed to import game batch; falling back to per-record handling', [
                    'error' => $e->getMessage(),
                    'provider' => $provider,
                    'batch_size' => count($recordBuffer),
                ]);

                foreach ($recordBuffer as $record) {
                    try {
                        $this->processGameRecord($record, $provider);
                    } catch (\Throwable $inner) {
                        $errors++;
                        Log::error('Failed to import game (fallback path)', [
                            'record' => $record,
                            'error' => $inner->getMessage(),
                        ]);
                    }
                }
            } finally {
                $recordBuffer = [];
                $this->flushBatches();
                $progressBar->setMessage((string) $errors, 'errors');
            }
        };

        $advanceProgress = function () use ($progressBar, $limit, $handle, $fileSize): void {
            if ($limit > 0) {
                $progressBar->advance();

                return;
            }

            $pos = ftell($handle);
            if (is_int($pos) && $pos >= 0) {
                $progressBar->setProgress(min($pos, $fileSize));
            }
        };

        $read = 0;
        $lastCheckpointAt = microtime(true);
        $lastCheckpointRows = 0;

        $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $limit, $resumeEnabled, $flushRecordBuffer, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$skipped, &$errors): void {
            if (! $resumeEnabled) {
                return;
            }

            // When a limit is set, we expect the run to stop early; still persist a checkpoint.
            // When unlimited, checkpoint periodically without introducing per-row IO.
            if (! $force && $limit > 0) {
                return;
            }

            $rowsSince = ($processed + $skipped) - $lastCheckpointRows;
            $secondsSince = microtime(true) - $lastCheckpointAt;

            if (! $force && $rowsSince < self::CHECKPOINT_ROWS_INTERVAL && $secondsSince < self::CHECKPOINT_SECONDS_INTERVAL) {
                return;
            }

            // Ensure the checkpoint byte offset represents fully persisted rows.
            $flushRecordBuffer();

            $pos = ftell($handle);
            if (! is_int($pos) || $pos < 0) {
                return;
            }

            $this->storeCheckpoint($file, $provider, [
                'pos' => $pos,
                'processed' => $processed,
                'skipped' => $skipped,
                'errors' => $errors,
            ]);

            $lastCheckpointAt = microtime(true);
            $lastCheckpointRows = $processed + $skipped;
        };

        while (($row = fgetcsv($handle)) !== false) {
            $read++;
            $record = $this->combineCsvRow($headers, $row);
            if ($record === null) {
                $skipped++;
                $progressBar->setMessage((string) $skipped, 'skipped');
                $advanceProgress();

                $maybeCheckpoint();

                if ($limit > 0 && $read >= $limit) {
                    $maybeCheckpoint(true);
                    break;
                }

                continue;
            }

            try {
                $recordBuffer[] = $record;
                if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                    $flushRecordBuffer();
                }
            } catch (\Throwable $e) {
                $errors++;
                $progressBar->setMessage((string) $errors, 'errors');
                Log::error('Failed to buffer game record', [
                    'record' => $record,
                    'error' => $e->getMessage(),
                ]);
            }

            $processed++;
            $advanceProgress();

            $maybeCheckpoint();

            if ($limit > 0 && $read >= $limit) {
                $maybeCheckpoint(true);
                break;
            }
        }

        $flushRecordBuffer();
        $progressBar->finish();
        fclose($handle);

        // When we fully consume the file (unlimited run), remove the checkpoint.
        if ($limit <= 0 && $resumeEnabled && $read > 0) {
            $this->forgetCheckpoint($file, $provider);
        }

        return $processed;
    }

    private function processGamesJson(string $file, string $provider, int $limit): int
    {
        $processed = 0;
        $errors = 0;
        $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));

        if (in_array($ext, ['ndjson', 'jsonl'], true)) {
            $handle = fopen($file, 'rb');
            if (! $handle) {
                return 0;
            }

            $resumeEnabled = (int) $this->option('resume') !== 0;
            $resetCheckpoint = (bool) $this->option('reset-checkpoint');

            if ($resetCheckpoint) {
                $this->forgetCheckpoint($file, $provider);
            }

            if ($limit <= 0 && $resumeEnabled && ! $resetCheckpoint) {
                $this->maybeSeekToCheckpoint($handle, $file, $provider);
            }

            try {
                $fileSize = max(1, (int) (filesize($file) ?: 1));
                $maxSteps = $limit > 0 ? $limit : $fileSize;

                $progressBar = $this->output->createProgressBar($maxSteps);
                $this->configureProgressBar($progressBar, true);
                $progressBar->setMessage('0', 'errors');
                $progressBar->setMessage('0', 'skipped');

                $advanceProgress = function () use ($progressBar, $limit, $handle, $fileSize): void {
                    if ($limit > 0) {
                        $progressBar->advance();

                        return;
                    }

                    $pos = ftell($handle);
                    if (is_int($pos) && $pos >= 0) {
                        $progressBar->setProgress(min($pos, $fileSize));
                    }
                };

                $read = 0;
                $lastCheckpointAt = microtime(true);
                $lastCheckpointRows = 0;

                /** @var array<int, array<string, mixed>> $recordBuffer */
                $recordBuffer = [];

                $flushRecordBuffer = function () use (&$recordBuffer, $provider, &$errors, $progressBar): void {
                    if ($recordBuffer === []) {
                        return;
                    }

                    try {
                        $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
                    } catch (\Throwable $e) {
                        Log::error('Failed to import game batch; falling back to per-record handling', [
                            'error' => $e->getMessage(),
                            'provider' => $provider,
                            'batch_size' => count($recordBuffer),
                        ]);

                        foreach ($recordBuffer as $record) {
                            try {
                                $this->processGameRecord($record, $provider);
                            } catch (\Throwable $inner) {
                                $errors++;
                                Log::error('Failed to import game (fallback path)', [
                                    'record' => $record,
                                    'error' => $inner->getMessage(),
                                ]);
                            }
                        }
                    } finally {
                        $recordBuffer = [];
                        $this->flushBatches();
                        $progressBar->setMessage((string) $errors, 'errors');
                    }
                };

                $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $limit, $resumeEnabled, $flushRecordBuffer, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$errors): void {
                    if (! $resumeEnabled) {
                        return;
                    }

                    if (! $force && $limit > 0) {
                        return;
                    }

                    $rowsSince = $processed - $lastCheckpointRows;
                    $secondsSince = microtime(true) - $lastCheckpointAt;

                    if (! $force && $rowsSince < self::CHECKPOINT_ROWS_INTERVAL && $secondsSince < self::CHECKPOINT_SECONDS_INTERVAL) {
                        return;
                    }

                    // Ensure the checkpoint byte offset represents fully persisted rows.
                    $flushRecordBuffer();

                    $pos = ftell($handle);
                    if (! is_int($pos) || $pos < 0) {
                        return;
                    }

                    $this->storeCheckpoint($file, $provider, [
                        'pos' => $pos,
                        'processed' => $processed,
                        'errors' => $errors,
                    ]);

                    $lastCheckpointAt = microtime(true);
                    $lastCheckpointRows = $processed;
                };

                while (($line = fgets($handle)) !== false) {
                    $read++;
                    $line = trim($line);
                    if ($line === '') {
                        $advanceProgress();

                        $maybeCheckpoint();

                        if ($limit > 0 && $read >= $limit) {
                            $maybeCheckpoint(true);
                            break;
                        }

                        continue;
                    }

                    $decoded = json_decode($line, true);
                    if (is_array($decoded)) {
                        try {
                            $recordBuffer[] = $decoded;
                            if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                                $flushRecordBuffer();
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $progressBar->setMessage((string) $errors, 'errors');
                            Log::error('Failed to buffer game record', [
                                'record' => $decoded,
                                'error' => $e->getMessage(),
                            ]);
                        }
                        $processed++;
                        $advanceProgress();

                        $maybeCheckpoint();
                    } else {
                        $progressBar->setMessage((string) (++$errors), 'errors');
                        $advanceProgress();

                        $maybeCheckpoint();
                    }

                    if ($limit > 0 && $read >= $limit) {
                        $maybeCheckpoint(true);
                        break;
                    }
                }

                $flushRecordBuffer();
                $progressBar->finish();
            } finally {
                fclose($handle);
            }

            if ($limit <= 0 && $resumeEnabled) {
                $this->forgetCheckpoint($file, $provider);
            }

            $this->flushBatches();

            return $processed;
        }

        $raw = File::get($file);
        $decoded = json_decode($raw, true);

        $list = [];
        if (is_array($decoded) && array_is_list($decoded)) {
            $list = $decoded;
        } elseif (is_array($decoded)) {
            foreach (['results', 'games', 'data'] as $key) {
                if (isset($decoded[$key]) && is_array($decoded[$key]) && array_is_list($decoded[$key])) {
                    $list = $decoded[$key];
                    break;
                }
            }
        }

        $total = $limit > 0 ? min($limit, count($list)) : count($list);
        $progressBar = $this->output->createProgressBar($total);
        $this->configureProgressBar($progressBar, true);
        $progressBar->setMessage('0', 'errors');
        $progressBar->setMessage('0', 'skipped');
        $errors = 0;
        $skipped = 0;

        /** @var array<int, array<string, mixed>> $recordBuffer */
        $recordBuffer = [];

        foreach ($list as $row) {
            if (! is_array($row)) {
                $skipped++;
                $progressBar->setMessage((string) $skipped, 'skipped');
                $progressBar->advance();

                continue;
            }

            $recordBuffer[] = $row;
            if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                try {
                    $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
                } catch (\Throwable $e) {
                    Log::error('Failed to import game batch; falling back to per-record handling', [
                        'error' => $e->getMessage(),
                        'provider' => $provider,
                        'batch_size' => count($recordBuffer),
                    ]);

                    foreach ($recordBuffer as $record) {
                        try {
                            $this->processGameRecord($record, $provider);
                        } catch (\Throwable $inner) {
                            $errors++;
                            Log::error('Failed to import game (fallback path)', [
                                'record' => $record,
                                'error' => $inner->getMessage(),
                            ]);
                        }
                    }
                } finally {
                    $recordBuffer = [];
                    $this->flushBatches();
                    $progressBar->setMessage((string) $errors, 'errors');
                }
            }

            $processed++;
            $progressBar->advance();

            if ($limit > 0 && $processed >= $limit) {
                break;
            }
        }

        if ($recordBuffer !== []) {
            try {
                $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
            } catch (\Throwable $e) {
                Log::error('Failed to import game batch; falling back to per-record handling', [
                    'error' => $e->getMessage(),
                    'provider' => $provider,
                    'batch_size' => count($recordBuffer),
                ]);

                foreach ($recordBuffer as $record) {
                    try {
                        $this->processGameRecord($record, $provider);
                    } catch (\Throwable $inner) {
                        $errors++;
                        Log::error('Failed to import game (fallback path)', [
                            'record' => $record,
                            'error' => $inner->getMessage(),
                        ]);
                    }
                }
            } finally {
                $recordBuffer = [];
                $this->flushBatches();
                $progressBar->setMessage((string) $errors, 'errors');
            }
        }

        $progressBar->finish();

        return $processed;
    }

    /**
     * Persist a batch of game records using set-based writes.
     *
     * This is the main ingestion hot path. It avoids per-record Eloquent create/find
     * for Products and VideoGameTitles by using bulk insert-or-ignore + ID resolution.
     *
     * @param  array<int, array<string, mixed>>  $records
     * @return int Number of per-record errors encountered.
     */
    private function processGameRecordsBatch(array $records, string $provider): int
    {
        if ($records === []) {
            return 0;
        }

        $now = now();
        $errors = 0;

        // `video_game_sources` is provider-level aggregation: one row per provider.
        $source = $this->sourceCache[$provider] ?? null;
        if (! $source) {
            $source = VideoGameSource::query()->firstOrCreate([
                'provider' => $provider,
            ]);

            $this->sourceCache[$provider] = $source;
        }

        /** @var array<string, array{name:string, name:string, normalized_title:string}> $productRowsBySlug */
        $productRowsBySlug = [];

        foreach ($records as $record) {
            $gameName = $record['name'] ?? null;
            $gameName = is_string($gameName) && $gameName !== '' ? $gameName : 'Unknown Game';

            $slug = $record['slug'] ?? null;
            $slug = is_string($slug) && $slug !== '' ? $slug : Str::slug($gameName);
            if ($slug === '') {
                $slug = Str::slug('unknown-game');
            }

            if (! isset($productRowsBySlug[$slug])) {
                $productRowsBySlug[$slug] = [
                    'name' => $gameName,
                    'name' => $gameName,
                    'normalized_title' => Str::slug($gameName),
                    'synopsis' => $record['summary'] ?? $record['storyline'] ?? null,
                ];
            }
        }

        // Insert products in chunks (SQLite bind limits).
        $productRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productRows[] = [
                'slug' => $slug,
                'name' => $row['name'],
                'title' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'synopsis' => $row['synopsis'],
                'type' => 'video_game',
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach ($this->chunkRowsForSafeParams($productRows, self::BATCH_SIZE) as $chunk) {
            DB::table('products')->insertOrIgnore($chunk);
        }

        $slugs = array_keys($productRowsBySlug);
        /** @var array<string, int> $productIdBySlug */
        $productIdBySlug = Product::query()->whereIn('slug', $slugs)->pluck('id', 'slug')->all();

        // Insert titles (one per product slug) in chunks.
        $titleRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productId = $productIdBySlug[$slug] ?? null;
            if (! is_int($productId)) {
                continue;
            }

            $titleRows[] = [
                'product_id' => $productId,
                'name' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'slug' => $slug,
                'providers' => json_encode([$provider], JSON_THROW_ON_ERROR),
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach ($this->chunkRowsForSafeParams($titleRows, self::BATCH_SIZE) as $chunk) {
            DB::table('video_game_titles')->insertOrIgnore($chunk);
        }

        /** @var array<string, VideoGameTitle> $titleBySlug */
        $titleBySlug = VideoGameTitle::query()
            ->whereIn('slug', $slugs)
            ->get(['id', 'slug', 'product_id', 'providers'])
            ->keyBy('slug')
            ->all();

        // Ensure provider presence in the title's providers JSON array.
        foreach ($titleBySlug as $slug => $title) {
            $existingProviders = is_array($title->providers) ? $title->providers : [];
            if (! in_array($provider, $existingProviders, true)) {
                $merged = array_values(array_unique(array_merge($existingProviders, [$provider])));
                VideoGameTitle::query()->whereKey($title->id)->update([
                    'providers' => $merged,
                    'updated_at' => $now,
                ]);
                $title->providers = $merged;
            }

            $this->titleCache[(string) $title->product_id] = $title;
        }

        // Finally, enqueue mappings + video games per record.
        foreach ($records as $record) {
            try {
                $gameId = $record['id'] ?? null;
                if ($gameId === null || $gameId === '') {
                    $errors++;
                    Log::error('Failed to import game: missing id', [
                        'record' => $record,
                    ]);

                    continue;
                }

                $gameName = $record['name'] ?? null;
                $gameName = is_string($gameName) && $gameName !== '' ? $gameName : 'Unknown Game';

                $slug = $record['slug'] ?? null;
                $slug = is_string($slug) && $slug !== '' ? $slug : Str::slug($gameName);
                if ($slug === '') {
                    $slug = Str::slug('unknown-game');
                }

                $title = $titleBySlug[$slug] ?? null;
                if (! $title) {
                    $errors++;
                    Log::error('Failed to import game: missing title after upsert', [
                        'slug' => $slug,
                        'record' => $record,
                    ]);

                    continue;
                }

        $this->videoGameTitleSourceBatch[] = [
            'video_game_title_id' => $title->id,
            'video_game_source_id' => $source->id,
            'external_id' => (int) $gameId,
            'provider_item_id' => (string) $gameId,
            'raw_payload' => json_encode($record, JSON_THROW_ON_ERROR),
            'provider' => $provider,
            // Map explicitly for columns
            'slug' => $slug,
            'name' => $gameName,
            'description' => $record['summary'] ?? $record['storyline'] ?? null,
            'release_date' => $this->parseDate($record['first_release_date'] ?? null),
            'platform' => json_encode($this->extractPlatforms($record), JSON_THROW_ON_ERROR),
            'rating' => $this->igdbRatingHelper()->extractPercentage($record),
            'rating_count' => $this->igdbRatingHelper()->extractRatingCount($record),
            'developer' => $this->extractDeveloperAndPublisher($record)['developer'],
            'publisher' => $this->extractDeveloperAndPublisher($record)['publisher'],
            'genre' => $this->extractGenresAsJson($record),
            'created_at' => $now,
            'updated_at' => $now,
        ];

                $platforms = $this->extractPlatforms($record);
                $companyFields = $this->extractDeveloperAndPublisher($record);
                $rating = $this->igdbRatingHelper()->extractPercentage($record);
                $ratingCount = $this->igdbRatingHelper()->extractRatingCount($record);

        $this->videoGameBatch[] = [
            'video_game_title_id' => $title->id,
            'provider' => $provider,
            'external_id' => (int) $gameId,
            'name' => $gameName, // New column
            'rating' => $rating, // New column
            'release_date' => $this->parseDate($record['first_release_date'] ?? null), // New column
            'attributes' => json_encode([
                'platform' => $platforms,
                'slug' => $title->slug,
                'name' => $gameName,
                'summary' => $record['summary'] ?? null,
                'storyline' => $record['storyline'] ?? null,
                'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                'rating' => $rating,
                'rating_count' => $ratingCount,
                'developer' => $companyFields['developer'],
                'publisher' => $companyFields['publisher'],
                // Always persist a JSON array ("[]" when unknown) to match application casts.
                'genre' => $this->extractGenresAsJson($record),
                'media' => null,
                // Provider-specific payloads are mirrored on `video_game_title_sources`.
                'source_payload' => null,
            ], JSON_THROW_ON_ERROR),
            'created_at' => $now,
            'updated_at' => $now,
        ];

                if (count($this->videoGameBatch) >= self::BATCH_SIZE || count($this->videoGameTitleSourceBatch) >= self::BATCH_SIZE) {
                    $this->flushBatches();
                }
            } catch (\Throwable $e) {
                $errors++;
                Log::error('Failed to import game', [
                    'record' => $record,
                    'error' => $e->getMessage(),
                ]);
            }
        }

        return $errors;
    }

    private function checkpointPath(string $file, string $provider): string
    {
        $dir = rtrim(dirname($file), '/').'/'.'.checkpoints';
        File::ensureDirectoryExists($dir);

        $real = realpath($file) ?: $file;
        $key = sha1($provider.'|'.$real);

        return $dir.'/gc-import-igdb-'.$key.'.json';
    }

    /**
     * @return array{pos:int, file_size:int, file_mtime:int}|null
     */
    private function loadCheckpoint(string $file, string $provider): ?array
    {
        $path = $this->checkpointPath($file, $provider);
        if (! File::exists($path)) {
            return null;
        }

        $raw = File::get($path);
        $decoded = json_decode($raw, true);
        if (! is_array($decoded)) {
            return null;
        }

        $pos = $decoded['pos'] ?? null;
        $fileSize = $decoded['file_size'] ?? null;
        $fileMtime = $decoded['file_mtime'] ?? null;

        if (! is_int($pos) || $pos < 0 || ! is_int($fileSize) || $fileSize < 1 || ! is_int($fileMtime) || $fileMtime < 0) {
            return null;
        }

        // Guard against seeking into the wrong file if the dump changed.
        $currentSize = (int) (filesize($file) ?: 0);
        $currentMtime = (int) (filemtime($file) ?: 0);
        if ($currentSize !== $fileSize || $currentMtime !== $fileMtime) {
            return null;
        }

        return [
            'pos' => $pos,
            'file_size' => $fileSize,
            'file_mtime' => $fileMtime,
        ];
    }

    /**
     * @param  array{pos:int, processed?:int, skipped?:int, errors?:int}  $data
     */
    private function storeCheckpoint(string $file, string $provider, array $data): void
    {
        $path = $this->checkpointPath($file, $provider);

        $payload = array_merge($data, [
            'file_size' => (int) (filesize($file) ?: 0),
            'file_mtime' => (int) (filemtime($file) ?: 0),
            'updated_at' => now()->toISOString(),
        ]);

        try {
            File::put($path, json_encode($payload, JSON_THROW_ON_ERROR));
        } catch (\Throwable $e) {
            Log::warning('Failed to store import checkpoint', [
                'file' => $file,
                'provider' => $provider,
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function forgetCheckpoint(string $file, string $provider): void
    {
        $path = $this->checkpointPath($file, $provider);
        if (File::exists($path)) {
            File::delete($path);
        }
    }

    /**
     * @param  resource  $handle
     */
    private function maybeSeekToCheckpoint(mixed $handle, string $file, string $provider): void
    {
        $checkpoint = $this->loadCheckpoint($file, $provider);
        if ($checkpoint === null) {
            return;
        }

        $pos = $checkpoint['pos'];
        $current = ftell($handle);
        if (! is_int($current) || $current < 0) {
            $current = 0;
        }

        // Never seek backwards before the current pointer (e.g., before CSV header).
        if ($pos <= $current) {
            return;
        }

        $seekResult = fseek($handle, $pos);
        if ($seekResult === 0) {
            $this->warn("Resuming import from checkpoint at byte offset {$pos}...");
            $this->newLine();
        }
    }

    /**
     * Fallback path for processing a single game record.
     *
     * This method is INTENTIONALLY per-row (using firstOrCreate) and only executes
     * when batch processing fails. It trades performance for resilience, allowing
     * individual records to be saved even if the batch encounters errors.
     *
     * Do not convert this to batching - it serves as an error recovery mechanism.
     */
    private function processGameRecord(array $record, string $provider): void
    {
        $gameId = $record['id'] ?? null;
        $gameName = $record['name'] ?? 'Unknown Game';
        $slug = $record['slug'] ?? Str::slug($gameName);

        // Check cache first before hitting database
        $cacheKey = $slug;
        $product = $this->productCache[$cacheKey] ?? null;

        if (! $product) {
            $product = Product::query()->firstOrCreate(
                ['slug' => $slug],
                [
                    'name' => $gameName,
                    'title' => $gameName,
                    'normalized_title' => Str::slug($gameName),
                    'type' => 'video_game',
                    'synopsis' => $record['summary'] ?? $record['storyline'] ?? null,
                ]
            );
            $this->productCache[$cacheKey] = $product;
        }

        // `video_game_sources` is provider-level aggregation: one row per provider.
        $source = $this->sourceCache[$provider] ?? null;
        if (! $source) {
            $source = VideoGameSource::query()->firstOrCreate([
                'provider' => $provider,
            ]);

            $this->sourceCache[$provider] = $source;
        }

        // Titles are canonical per Product; providers attach via `video_game_title_sources`.
        $titleKey = (string) $product->id;
        $title = $this->titleCache[$titleKey] ?? null;

        if (! $title) {
            $title = VideoGameTitle::query()->firstOrCreate(
                [
                    'product_id' => $product->id,
                ],
                [
                    'name' => $gameName,
                    'normalized_title' => Str::slug($gameName),
                    'slug' => $product->slug,
                    'providers' => [$provider],
                ]
            );

            $this->titleCache[$titleKey] = $title;
        }

        $existingProviders = is_array($title->providers) ? $title->providers : [];
        if (! in_array($provider, $existingProviders, true)) {
            $title->forceFill([
                'providers' => array_values(array_unique(array_merge($existingProviders, [$provider]))),
            ])->save();
        }

        // Persist provider-item mapping via bulk upsert for speed.
        $this->videoGameTitleSourceBatch[] = [
            'video_game_title_id' => $title->id,
            'video_game_source_id' => $source->id,
            'external_id' => (int) $gameId,
            'provider_item_id' => (string) $gameId,
            'raw_payload' => json_encode($record, JSON_THROW_ON_ERROR),
            'provider' => $provider,
            'slug' => $slug,
            'name' => $gameName,
            'description' => $record['summary'] ?? $record['storyline'] ?? null,
            'release_date' => $this->parseDate($record['first_release_date'] ?? null),
            'platform' => json_encode($this->extractPlatforms($record), JSON_THROW_ON_ERROR),
            'rating' => $this->igdbRatingHelper()->extractPercentage($record),
            'rating_count' => $this->igdbRatingHelper()->extractRatingCount($record),
            'developer' => $this->extractDeveloperAndPublisher($record)['developer'],
            'publisher' => $this->extractDeveloperAndPublisher($record)['publisher'],
            'genre' => $this->extractGenresAsJson($record),
            'created_at' => now(),
            'updated_at' => now(),
        ];

        $platforms = $this->extractPlatforms($record);
        $companyFields = $this->extractDeveloperAndPublisher($record);
        $rating = $this->igdbRatingHelper()->extractPercentage($record);
        $ratingCount = $this->igdbRatingHelper()->extractRatingCount($record);

        $this->videoGameBatch[] = [
            'video_game_title_id' => $title->id,
            'provider' => $provider,
            'external_id' => (int) $gameId,
            'name' => $gameName,
            'rating' => $rating,
            'release_date' => $this->parseDate($record['first_release_date'] ?? null),
            'attributes' => json_encode([
                'platform' => $platforms,
                'slug' => $title->slug,
                'name' => $gameName,
                'summary' => $record['summary'] ?? null,
                'storyline' => $record['storyline'] ?? null,
                'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                'rating' => $rating,
                'rating_count' => $ratingCount,
                'developer' => $companyFields['developer'],
                'publisher' => $companyFields['publisher'],
                // Always persist a JSON array ("[]" when unknown) to match application casts.
                'genre' => $this->extractGenresAsJson($record),
                'media' => null,
                // Provider-specific payloads are mirrored on `video_game_title_sources`.
                'source_payload' => null,
            ], JSON_THROW_ON_ERROR),
            'created_at' => now(),
            'updated_at' => now(),
        ];

        // Flush batches when they reach size limit.
        if (count($this->videoGameBatch) >= self::BATCH_SIZE || count($this->videoGameTitleSourceBatch) >= self::BATCH_SIZE) {
            $this->flushBatches();
        }
    }

    private function flushBatches(): void
    {
        $this->flushVideoGameTitleSourceBatch();
        $this->flushVideoGameBatch();
        $this->flushImageBatch();
        $this->flushVideoBatch();
    }

    private function flushVideoGameTitleSourceBatch(): void
    {
        if ($this->videoGameTitleSourceBatch === []) {
            return;
        }

        foreach ($this->chunkRowsForSafeParams($this->videoGameTitleSourceBatch, self::BATCH_SIZE) as $chunk) {
            DB::table('video_game_title_sources')->upsert(
                $chunk,
                ['video_game_title_id', 'video_game_source_id', 'provider_item_id'],
                [
                    'raw_payload', 'updated_at', 'provider', 'external_id',
                    // Add columns to update
                    'slug', 'name', 'description', 'release_date', 'platform',
                    'rating', 'rating_count', 'developer', 'publisher', 'genre'
                ]
            );
        }

        $this->videoGameTitleSourceBatch = [];
    }

    private function flushVideoGameBatch(): void
    {
        if (empty($this->videoGameBatch)) {
            return;
        }

        foreach ($this->chunkRowsForSafeParams($this->videoGameBatch, self::BATCH_SIZE) as $chunk) {
            DB::table('video_games')->upsert(
                $chunk,
                ['video_game_title_id'],
                [
                    'provider',
                    'external_id',
                    'attributes',
                    'updated_at',
                    'name', // Update these
                    'rating',
                    'release_date'
                ]
            );
        }

        $this->videoGameBatch = [];
    }

    private function flushImageBatch(): void
    {
        if (empty($this->imageBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->imageBatch)));
        $existingRows = DB::table('images')
            ->where('imageable_type', VideoGame::class)
            ->whereIn('imageable_id', $videoGameIds)
            ->get([
                'imageable_id',
                'video_game_id',
                'url',
                'source_url',
                'width',
                'height',
                'is_thumbnail',
                'alt_text',
                'caption',
                'urls',
                'metadata',
            ]);

        $existingByGameId = [];
        foreach ($existingRows as $row) {
            $existingByGameId[(int) ($row->imageable_id ?? $row->video_game_id)] = $row;
        }

        // Defensive: ensure we never pass duplicate `video_game_id` rows into a single bulk upsert.
        // Some databases can throw a unique constraint violation when the VALUES list contains duplicates,
        // even though we're using ON CONFLICT/UPSERT semantics.
        $upsertByGameId = [];
        foreach ($this->imageBatch as $videoGameId => $batch) {
            $videoGameId = (int) $videoGameId;
            if ($videoGameId <= 0) {
                continue;
            }

            $existing = $existingByGameId[(int) $videoGameId] ?? null;
            $existingUrls = $existing && is_string($existing->urls) ? json_decode($existing->urls, true) : [];
            if (! is_array($existingUrls)) {
                $existingUrls = [];
            }

            $existingMetadata = $existing && is_string($existing->metadata) ? json_decode($existing->metadata, true) : [];
            if (! is_array($existingMetadata)) {
                $existingMetadata = [];
            }

            $newUrls = $batch['urls'] ?? [];
            if (! is_array($newUrls)) {
                $newUrls = [];
            }

            $mergedUrls = $this->mergeUniqueStrings($existingUrls, $newUrls);

            $newMetadata = [
                'collections' => $batch['metadata']['collections'] ?? [],
                'all_details' => $batch['metadata']['details'] ?? [],
            ];
            $mergedMetadata = $this->mergeImageMetadata($existingMetadata, $newMetadata);

            $newPrimaryDetail = $this->pickImagePrimaryDetail($newMetadata['all_details'] ?? []);

            $existingIsThumbnail = $existing ? (bool) $existing->is_thumbnail : false;
            $newHasThumbnail = in_array(true, (array) ($batch['metadata']['thumbnails'] ?? []), true);

            $sourceUrl = $existingIsThumbnail
                ? $existing->source_url
                : ($newPrimaryDetail['url'] ?? ($mergedUrls[0] ?? null));

            $primaryUrl = $sourceUrl ?? ($mergedUrls[0] ?? null);
            if ($primaryUrl === null) {
                $primaryUrl = sprintf('igdb://video-game/%d/primary-image', $videoGameId);
                $mergedUrls = $this->mergeUniqueStrings($mergedUrls, [$primaryUrl]);
            }

            // Ensure integers are strictly typed or null (fixes "invalid input syntax for type integer: ''")
            $rawWidth = $existingIsThumbnail ? $existing->width : ($newPrimaryDetail['width'] ?? null);
            $width = ($rawWidth === null || $rawWidth === '') ? null : (int) $rawWidth;

            $rawHeight = $existingIsThumbnail ? $existing->height : ($newPrimaryDetail['height'] ?? null);
            $height = ($rawHeight === null || $rawHeight === '') ? null : (int) $rawHeight;

            $isThumbnail = $existingIsThumbnail || $newHasThumbnail;

            $altText = $existingIsThumbnail
                ? $existing->alt_text
                : ($newPrimaryDetail['alt_text'] ?? ($newPrimaryDetail['image_id'] ?? null));

            $caption = $existingIsThumbnail
                ? $existing->caption
                : ($newPrimaryDetail['caption'] ?? null);

            $metadata = $mergedMetadata;

            // Extract Spatie-compatible fields
            $collections = $mergedMetadata['collections'] ?? [];
            $primaryCollection = $collections[0] ?? 'cover_images';
            $externalId = $newPrimaryDetail['image_id'] ?? null;

            $row = [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'collection_names' => json_encode($collections),
                'primary_collection' => $primaryCollection,
                'url' => $primaryUrl,
                'external_id' => $externalId,
                'provider' => 'igdb',
                'source_url' => $sourceUrl,
                'width' => $width,
                'height' => $height,
                'is_thumbnail' => $isThumbnail,
                'order_column' => 0,
                'alt_text' => $altText,
                'caption' => $caption,
                'urls' => json_encode($mergedUrls),
                'metadata' => json_encode($metadata),
                'created_at' => now(),
                'updated_at' => now(),
            ];

            if (! isset($upsertByGameId[$videoGameId])) {
                $upsertByGameId[$videoGameId] = $row;

                continue;
            }

            // Merge duplicates (prefer thumbnails for primary fields, preserve all urls/details).
            $existingRow = $upsertByGameId[$videoGameId];

            $existingRowUrls = is_string($existingRow['urls'] ?? null) ? json_decode((string) $existingRow['urls'], true) : [];
            if (! is_array($existingRowUrls)) {
                $existingRowUrls = [];
            }
            $rowUrls = is_string($row['urls'] ?? null) ? json_decode((string) $row['urls'], true) : [];
            if (! is_array($rowUrls)) {
                $rowUrls = [];
            }
            $mergedRowUrls = $this->mergeUniqueStrings($existingRowUrls, $rowUrls);

            $existingRowMeta = is_string($existingRow['metadata'] ?? null) ? json_decode((string) $existingRow['metadata'], true) : [];
            if (! is_array($existingRowMeta)) {
                $existingRowMeta = [];
            }
            $rowMeta = is_string($row['metadata'] ?? null) ? json_decode((string) $row['metadata'], true) : [];
            if (! is_array($rowMeta)) {
                $rowMeta = [];
            }
            $mergedRowMeta = $this->mergeImageMetadata($existingRowMeta, $rowMeta);

            $existingRowIsThumbnail = (bool) ($existingRow['is_thumbnail'] ?? false);
            $rowIsThumbnail = (bool) ($row['is_thumbnail'] ?? false);

            $preferRowScalars = $rowIsThumbnail && ! $existingRowIsThumbnail;

            // Merge collection_names
            $existingCollections = is_string($existingRow['collection_names'] ?? null)
                ? json_decode((string) $existingRow['collection_names'], true) : [];
            $rowCollections = is_string($row['collection_names'] ?? null)
                ? json_decode((string) $row['collection_names'], true) : [];
            $mergedCollections = array_values(array_unique(array_merge(
                is_array($existingCollections) ? $existingCollections : [],
                is_array($rowCollections) ? $rowCollections : []
            )));

            $upsertByGameId[$videoGameId] = [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => $existingRow['uuid'] ?? $row['uuid'],
                'collection_names' => json_encode($mergedCollections),
                'primary_collection' => $existingRow['primary_collection'] ?? $row['primary_collection'],
                'url' => $preferRowScalars ? $row['url'] : ($existingRow['url'] ?? $row['url']),
                'external_id' => $preferRowScalars ? $row['external_id'] : ($existingRow['external_id'] ?? $row['external_id']),
                'provider' => 'igdb',
                'source_url' => $preferRowScalars ? $row['source_url'] : ($existingRow['source_url'] ?? $row['source_url']),
                'width' => $preferRowScalars ? $row['width'] : ($existingRow['width'] ?? $row['width']),
                'height' => $preferRowScalars ? $row['height'] : ($existingRow['height'] ?? $row['height']),
                'is_thumbnail' => $existingRowIsThumbnail || $rowIsThumbnail,
                'order_column' => $existingRow['order_column'] ?? $row['order_column'] ?? 0,
                'alt_text' => $preferRowScalars ? $row['alt_text'] : ($existingRow['alt_text'] ?? $row['alt_text']),
                'caption' => $preferRowScalars ? $row['caption'] : ($existingRow['caption'] ?? $row['caption']),
                'urls' => json_encode($mergedRowUrls),
                'metadata' => json_encode($mergedRowMeta),
                // Preserve earliest created_at if present.
                'created_at' => $existingRow['created_at'] ?? $row['created_at'],
                'updated_at' => now(),
            ];
        }

        $upsertData = array_values($upsertByGameId);

        // Bulk upsert - let database handle merging via upsert
        if ($upsertData !== []) {
            foreach ($this->chunkRowsForSafeParams($upsertData, self::MEDIA_BATCH_SIZE) as $chunk) {
                Log::debug('ImportIgdbDumpsCommand image upsert chunk', [
                    'rows' => count($chunk),
                    'sample' => $chunk[0] ?? null,
                ]);
                DB::table('images')->upsert(
                    $chunk,
                    ['imageable_type', 'imageable_id', 'url'],
                    [
                        'video_game_id',
                        'uuid',
                        'collection_names',
                        'primary_collection',
                        'external_id',
                        'provider',
                        'source_url',
                        'width',
                        'height',
                        'is_thumbnail',
                        'order_column',
                        'alt_text',
                        'caption',
                        'urls',
                        'metadata',
                        'url',
                        'updated_at',
                    ]
                );
            }
        }

        $this->imageBatch = [];
    }

    private function flushVideoBatch(): void
    {
        if (empty($this->videoBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->videoBatch)));
        $existingRows = DB::table('videos')
            ->whereIn('video_game_id', $videoGameIds)
            ->get(['video_game_id', 'urls', 'provider', 'metadata']);

        $existingByGameId = [];
        foreach ($existingRows as $row) {
            $existingByGameId[(int) $row->video_game_id] = $row;
        }

        $upsertData = [];
        foreach ($this->videoBatch as $videoGameId => $batch) {
            $existing = $existingByGameId[(int) $videoGameId] ?? null;
            $existingUrls = $existing && is_string($existing->urls) ? json_decode($existing->urls, true) : [];
            if (! is_array($existingUrls)) {
                $existingUrls = [];
            }

            $existingMetadata = $existing && is_string($existing->metadata) ? json_decode($existing->metadata, true) : [];
            if (! is_array($existingMetadata)) {
                $existingMetadata = [];
            }

            $newUrls = $batch['urls'] ?? [];
            if (! is_array($newUrls)) {
                $newUrls = [];
            }

            $mergedUrls = $this->mergeUniqueStrings($existingUrls, $newUrls);
            $mergedMetadata = $this->mergeVideoMetadata($existingMetadata, (array) ($batch['metadata'] ?? []));
            $provider = $existing && is_string($existing->provider) && $existing->provider !== ''
                ? $existing->provider
                : ($batch['provider'] ?? 'youtube');

            $primaryUrl = $mergedUrls[0] ?? sprintf('igdb://video-game/%d/primary-video', $videoGameId);

            // Extract first video_id as external_id
            $externalId = $mergedUrls[0] ?? null;

            $upsertData[] = [
                'videoable_type' => \App\Models\VideoGame::class,
                'videoable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'collection_names' => json_encode(['trailers']),
                'primary_collection' => 'trailers',
                'url' => $primaryUrl,
                'external_id' => $externalId,
                'video_id' => $externalId,
                'urls' => json_encode($mergedUrls),
                'provider' => $provider,
                'order_column' => 0,
                'metadata' => json_encode($mergedMetadata),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        // Bulk upsert - let database handle merging
        if ($upsertData !== []) {
            foreach ($this->chunkRowsForSafeParams($upsertData, self::MEDIA_BATCH_SIZE) as $chunk) {
                DB::table('videos')->upsert(
                    $chunk,
                    ['videoable_type', 'videoable_id', 'url'],
                    [
                        'video_game_id',
                        'uuid',
                        'collection_names',
                        'primary_collection',
                        'external_id',
                        'video_id',
                        'urls',
                        'provider',
                        'order_column',
                        'metadata',
                        'updated_at',
                    ]
                );
            }
        }

        $this->videoBatch = [];
    }

    private function extractPlatforms(array $record): array
    {
        if (isset($record['platforms']) && is_array($record['platforms'])) {
            $names = [];
            foreach ($record['platforms'] as $platform) {
                if (is_array($platform) && isset($platform['name'])) {
                    $names[] = (string) $platform['name'];
                }
            }

            $names = $this->platformNormalizer()->normalizeMany($names);

            return $names !== [] ? $names : ['PC'];
        }

        if (isset($record['platforms']) && is_string($record['platforms'])) {
            $raw = trim($record['platforms']);

            // IGDB CSV exports may contain platform IDs or a JSON-ish string.
            // Best-effort parsing: JSON array -> list, else split on common delimiters.
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                $values = array_values(array_filter(array_map(function ($v) {
                    if (is_int($v)) {
                        return $this->platformIdToName[$v] ?? (string) $v;
                    }

                    if (is_string($v) && ctype_digit($v)) {
                        $id = (int) $v;

                        return $this->platformIdToName[$id] ?? $v;
                    }

                    return is_scalar($v) ? (string) $v : '';
                }, $decoded), fn ($v) => $v !== ''));

                $values = $this->platformNormalizer()->normalizeMany($values);

                return $values !== [] ? $values : ['PC'];
            }

            // IGDB dumps commonly represent ID arrays as "{6,48}".
            $ids = $this->parseIgdbIdSetString($raw);
            if ($ids !== []) {
                $names = array_map(fn (int $id) => $this->platformIdToName[$id] ?? (string) $id, $ids);
                $names = $this->platformNormalizer()->normalizeMany($names);

                return $names !== [] ? $names : ['PC'];
            }

            $parts = preg_split('/[\s,|]+/', $raw) ?: [];
            $parts = array_values(array_filter(array_map('trim', $parts), fn ($v) => $v !== ''));

            $parts = $this->platformNormalizer()->normalizeMany($parts);

            return $parts !== [] ? $parts : ['PC'];
        }

        return ['PC'];
    }

    private function extractGenresAsJson(array $record): string
    {
        $genres = [];

        // IGDB JSON API can embed genre objects.
        if (isset($record['genres']) && is_array($record['genres'])) {
            foreach ($record['genres'] as $g) {
                if (is_array($g) && isset($g['name']) && is_string($g['name']) && $g['name'] !== '') {
                    $genres[] = $g['name'];
                } elseif (is_string($g) && $g !== '') {
                    $genres[] = $g;
                }
            }
        }

        // IGDB CSV exports typically represent genre IDs as "{5,12}".
        if ($genres === [] && isset($record['genres']) && is_string($record['genres'])) {
            $ids = $this->parseIgdbIdSetString($record['genres']);
            if ($ids !== [] && $this->genreIdToName !== []) {
                $genres = array_values(array_filter(array_map(fn (int $id) => $this->genreIdToName[$id] ?? null, $ids)));
            } else {
                $genres = $ids;
            }
        }

        $genres = array_values(array_unique(array_values(array_filter($genres, fn ($v) => $v !== '' && $v !== null))));

        return json_encode($genres, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array{developer:?string, publisher:?string}
     */
    private function extractDeveloperAndPublisher(array $record): array
    {
        // JSON payloads in this repo's sample already expose these fields.
        $developer = isset($record['developer']) && is_string($record['developer']) && $record['developer'] !== ''
            ? $record['developer']
            : null;
        $publisher = isset($record['publisher']) && is_string($record['publisher']) && $record['publisher'] !== ''
            ? $record['publisher']
            : null;

        if ($developer !== null || $publisher !== null) {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        $raw = $record['involved_companies'] ?? null;
        if (! is_string($raw) || $raw === '') {
            return [
                'developer' => null,
                'publisher' => null,
            ];
        }

        if ($this->involvedCompanyIdToCompanyRole === [] || $this->companyIdToName === []) {
            return [
                'developer' => null,
                'publisher' => null,
            ];
        }

        $ids = $this->parseIgdbIdSetString($raw);
        if ($ids === []) {
            return [
                'developer' => null,
                'publisher' => null,
            ];
        }

        $developerNames = [];
        $publisherNames = [];

        foreach ($ids as $involvedCompanyId) {
            $row = $this->involvedCompanyIdToCompanyRole[$involvedCompanyId] ?? null;
            if ($row === null) {
                continue;
            }

            $companyName = $this->companyIdToName[$row['company_id']] ?? null;
            if (! is_string($companyName) || $companyName === '') {
                continue;
            }

            if ($row['developer'] === true) {
                $developerNames[] = $companyName;
            }
            if ($row['publisher'] === true) {
                $publisherNames[] = $companyName;
            }
        }

        $developerNames = array_values(array_unique($developerNames));
        $publisherNames = array_values(array_unique($publisherNames));

        return [
            'developer' => $developerNames !== [] ? implode(', ', $developerNames) : null,
            'publisher' => $publisherNames !== [] ? implode(', ', $publisherNames) : null,
        ];
    }

    private function parseDate(?string $date): ?string
    {
        if (! $date) {
            return null;
        }

        if (is_numeric($date)) {
            return date('Y-m-d', (int) $date);
        }

        return $date;
    }

    private function processMediaIfPresent(string $path, string $basename, callable $attach, string $provider): int
    {
        $file = $this->findFile($path, $basename);
        if (! $file) {
            $this->line("  ⚠️  {$basename}: not found, skipping");

            return 0;
        }

        $this->info("  📥 {$basename}...");

        return $this->processMediaCsvStreaming($file, $provider, $attach);
    }

    private function addImageMedia(object $videoGame, array $data, string $collection, bool $isThumbnail): void
    {
        try {
            $url = $data['url'] ?? null;
            $imageId = $data['image_id'] ?? null;

            if (! $url && is_string($imageId) && $imageId !== '') {
                $url = "https://images.igdb.com/igdb/image/upload/t_1080p/{$imageId}.jpg";
            }

            if (! $url) {
                return;
            }

            if (str_starts_with($url, '//')) {
                $url = 'https:'.$url;
            }

            // Generate all size variants for IGDB images
            $allUrls = [$url]; // Start with original URL

            if (str_contains($url, 'images.igdb.com')) {
                // Extract base URL pattern
                $baseUrl = preg_replace('/\/t_[a-z_]+\//', '/', $url);

                if (! is_string($imageId) || $imageId === '') {
                    $imageId = $this->extractIgdbImageIdFromUrl($baseUrl);
                }

                if (is_string($imageId) && $imageId !== '') {
                    // Generate all available IGDB sizes
                    $sizes = match ($collection) {
                        'cover_images' => [
                            't_cover_small',      // 90×128
                            't_cover_big',        // 264×374
                            't_720p',             // 1280×720
                            't_1080p',            // 1920×1080 (primary)
                        ],
                        'screenshots' => [
                            't_thumb',            // 90×90
                            't_screenshot_med',   // 569×320
                            't_screenshot_big',   // 889×500
                            't_screenshot_huge',  // 1280×720 (primary)
                            't_1080p',            // 1920×1080
                        ],
                        'artworks' => [
                            't_thumb',            // 90×90
                            't_720p',             // 1280×720
                            't_1080p',            // 1920×1080 (primary)
                        ],
                        default => [
                            't_thumb',            // 90×90
                            't_720p',             // 1280×720 (primary)
                        ],
                    };

                    // Build URL for each size
                    $allUrls = array_map(
                        fn ($size) => "https://images.igdb.com/igdb/image/upload/{$size}/{$imageId}.jpg",
                        $sizes
                    );
                }
            }

            // Aggregate images in-memory per video_game_id
            $gameId = $videoGame->id;

            // Skip if invalid game ID (must be a positive integer)
            if (! $gameId || ! is_numeric($gameId) || $gameId <= 0) {
                return;
            }

            if (! isset($this->imageBatch[$gameId])) {
                $this->imageBatch[$gameId] = [
                    'urls' => [],
                    'metadata' => [
                        'collections' => [],
                        'thumbnails' => [],
                        'details' => [],
                    ],
                ];
            }

            // Add all size variants (avoid duplicates)
            foreach ($allUrls as $variantUrl) {
                if (! in_array($variantUrl, $this->imageBatch[$gameId]['urls'], true)) {
                    $this->imageBatch[$gameId]['urls'][] = $variantUrl;
                }
            }

            // Aggregate metadata
            if (! in_array($collection, $this->imageBatch[$gameId]['metadata']['collections'], true)) {
                $this->imageBatch[$gameId]['metadata']['collections'][] = $collection;
            }
            $this->imageBatch[$gameId]['metadata']['thumbnails'][] = $isThumbnail;

            // Store detail with all size variants
            $detailWithSizes = array_merge($data, ['size_variants' => $allUrls, 'collection' => $collection, 'is_thumbnail' => $isThumbnail]);
            $this->imageBatch[$gameId]['metadata']['details'][] = $detailWithSizes;

            // Flush when batch reaches size limit
            if (count($this->imageBatch) >= self::MEDIA_BATCH_SIZE) {
                $this->flushImageBatch();
            }
        } catch (\Exception) {
            // Skip silently
        }
    }

    private function addVideoMedia(object $videoGame, array $data): void
    {
        try {
            $videoId = $data['video_id'] ?? null;
            if (! $videoId) {
                return;
            }

            // Skip devlogs as requested
            $name = $data['name'] ?? '';
            if (is_string($name) && stripos($name, 'devlog') !== false) {
                return;
            }

            $provider = $data['provider'] ?? 'youtube';

            // Aggregate videos in-memory per video_game_id
            $gameId = $videoGame->id;

            // Skip if invalid game ID (must be a positive integer)
            if (! $gameId || ! is_numeric($gameId) || $gameId <= 0) {
                return;
            }

            if (! isset($this->videoBatch[$gameId])) {
                $this->videoBatch[$gameId] = [
                    'urls' => [],
                    'provider' => $provider,
                    'metadata' => [],
                ];
            }

            // Avoid duplicate video IDs
            if (! in_array($videoId, $this->videoBatch[$gameId]['urls'], true)) {
                $this->videoBatch[$gameId]['urls'][] = $videoId;
            }

            $this->videoBatch[$gameId]['metadata'][] = $data;

            // Flush when batch reaches size limit
            if (count($this->videoBatch) >= self::MEDIA_BATCH_SIZE) {
                $this->flushVideoBatch();
            }
        } catch (\Exception) {
            // Skip silently
        }
    }

    /**
     * @param  array<int, string>  $existing
     * @param  array<int, string>  $incoming
     * @return array<int, string>
     */
    private function mergeUniqueStrings(array $existing, array $incoming): array
    {
        $merged = [];
        foreach (array_merge($existing, $incoming) as $value) {
            if (! is_string($value) || $value === '') {
                continue;
            }
            if (! in_array($value, $merged, true)) {
                $merged[] = $value;
            }
        }

        return $merged;
    }

    /**
     * @param  array<string, mixed>  $existing
     * @param  array<string, mixed>  $incoming
     * @return array<string, mixed>
     */
    private function mergeImageMetadata(array $existing, array $incoming): array
    {
        $existingCollections = isset($existing['collections']) && is_array($existing['collections']) ? $existing['collections'] : [];
        $incomingCollections = isset($incoming['collections']) && is_array($incoming['collections']) ? $incoming['collections'] : [];

        $collections = array_values(array_unique(array_values(array_filter(array_merge($existingCollections, $incomingCollections), fn ($v) => is_string($v) && $v !== ''))));

        $existingDetails = isset($existing['all_details']) && is_array($existing['all_details']) ? $existing['all_details'] : [];
        $incomingDetails = isset($incoming['all_details']) && is_array($incoming['all_details']) ? $incoming['all_details'] : [];

        $details = $this->dedupeMediaDetails(array_merge($existingDetails, $incomingDetails));

        return [
            'collections' => $collections,
            'all_details' => $details,
        ];
    }

    /**
     * @param  array<int, mixed>  $details
     * @return array<int, mixed>
     */
    private function dedupeMediaDetails(array $details): array
    {
        $seen = [];
        $result = [];

        foreach ($details as $detail) {
            if (! is_array($detail)) {
                continue;
            }

            $key = null;
            foreach (['id', 'image_id', 'video_id', 'url', 'checksum'] as $candidate) {
                if (isset($detail[$candidate]) && is_scalar($detail[$candidate]) && (string) $detail[$candidate] !== '') {
                    $key = $candidate.':'.(string) $detail[$candidate];
                    break;
                }
            }

            if ($key === null) {
                $key = 'hash:'.md5(json_encode($detail));
            }

            if (isset($seen[$key])) {
                continue;
            }

            $seen[$key] = true;
            $result[] = $detail;
        }

        return $result;
    }

    /**
     * @param  array<int, mixed>  $details
     * @return array<string, mixed>
     */
    private function pickImagePrimaryDetail(array $details): array
    {
        $first = null;
        $best = null;

        foreach ($details as $detail) {
            if (! is_array($detail)) {
                continue;
            }

            $first ??= $detail;

            if (($detail['is_thumbnail'] ?? false) === true) {
                $best = $detail;
                break;
            }

            if (($detail['collection'] ?? null) === 'cover_images') {
                $best ??= $detail;
            }
        }

        return $best ?? $first ?? [];
    }

    /**
     * @param  array<string, mixed>  $existing
     * @param  array<int, mixed>  $incoming
     * @return array<int, mixed>
     */
    private function mergeVideoMetadata(array $existing, array $incoming): array
    {
        $existingItems = is_array($existing) ? $existing : [];
        $items = array_merge($existingItems, $incoming);

        return $this->dedupeMediaDetails($items);
    }

    private function extractIgdbImageIdFromUrl(string $url): ?string
    {
        if (preg_match('~/(?:t_[a-z_]+/)?([a-zA-Z0-9_]+)\.(?:jpg|png|gif|webp)~', $url, $m) === 1) {
            return $m[1];
        }

        return null;
    }

    private function processMediaCsvStreaming(string $file, string $provider, callable $attach): int
    {
        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn("Could not open media file: {$file}");

            return 0;
        }

        $headers = fgetcsv($handle);
        if (! $headers) {
            fclose($handle);

            return 0;
        }

        // PRE-LOAD all game ID mappings into memory to avoid N+1 queries
        $gameIdMap = $this->preloadGameIdMappings($provider);
        if (empty($gameIdMap)) {
            $this->warn("No game ID mappings found - skipping media import for {$file}");
            fclose($handle);

            return 0;
        }

        // Check for resume support
        // IMPORTANT: Option values are strings. Casting to bool is wrong because (bool) '0' === true.
        // We must cast to int to correctly treat --resume=0 as disabled.
        $resumeEnabled = (int) $this->option('resume') !== 0;
        $resetCheckpoint = (bool) $this->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        if ($resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        // Count total lines
        $totalLines = 0;
        while (fgets($handle) !== false) {
            $totalLines++;
        }
        rewind($handle);
        fgetcsv($handle); // Skip header again

        // If we have a checkpoint, seek to it again (counting above reset the pointer)
        if ($resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        $progressBar = $this->output->createProgressBar($totalLines);
        $this->configureProgressBar($progressBar, false);
        $progressBar->setMessage('0', 'processed');
        $progressBar->setMessage('0', 'skipped');
        $progressBar->setMessage('0', 'errors');

        $processed = 0;
        $skipped = 0;
        $errors = 0;

        // Checkpoint tracking
        $lastCheckpointAt = microtime(true);
        $lastCheckpointRows = 0;
        $lastProgressUpdate = 0;
        $progressUpdateInterval = 50; // Update progress every 50 rows for better visual feedback

        $flushMediaBatches = function (): void {
            $this->flushImageBatch();
            $this->flushVideoBatch();
        };

        $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $resumeEnabled, $flushMediaBatches, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$skipped, &$errors): void {
            if (! $resumeEnabled) {
                return;
            }

            $rowsSince = ($processed + $skipped) - $lastCheckpointRows;
            $secondsSince = microtime(true) - $lastCheckpointAt;

            if (! $force && $rowsSince < self::CHECKPOINT_ROWS_INTERVAL && $secondsSince < self::CHECKPOINT_SECONDS_INTERVAL) {
                return;
            }

            // Flush pending batches before checkpointing
            $flushMediaBatches();

            $pos = ftell($handle);
            if (! is_int($pos) || $pos < 0) {
                return;
            }

            $this->storeCheckpoint($file, $provider, [
                'pos' => $pos,
                'processed' => $processed,
                'skipped' => $skipped,
                'errors' => $errors,
            ]);

            $lastCheckpointAt = microtime(true);
            $lastCheckpointRows = $processed + $skipped;
        };

        $updateProgress = function () use ($progressBar, &$processed, &$skipped, &$errors, &$lastProgressUpdate, $progressUpdateInterval): void {
            $totalRows = $processed + $skipped + $errors;
            if ($totalRows - $lastProgressUpdate >= $progressUpdateInterval) {
                $progressBar->setProgress($totalRows);
                $progressBar->setMessage((string) $processed, 'processed');
                $progressBar->setMessage((string) $skipped, 'skipped');
                $progressBar->setMessage((string) $errors, 'errors');
                $lastProgressUpdate = $totalRows;
            }
        };

        while (($row = fgetcsv($handle)) !== false) {
            $record = $this->combineCsvRow($headers, $row);
            if ($record === null) {
                $skipped++;
                $updateProgress();
                $maybeCheckpoint();

                continue;
            }
            $gameId = $record['game'] ?? null;
            if (! $gameId) {
                $skipped++;
                $updateProgress();
                $maybeCheckpoint();

                continue;
            }

            // Fast in-memory lookup instead of database query
            $videoGameId = $gameIdMap[(string) $gameId] ?? null;

            // Skip if video_game_id is null, empty string, zero, or invalid
            if (! $videoGameId || ! is_numeric($videoGameId) || $videoGameId <= 0) {
                $skipped++;
                $updateProgress();
                $maybeCheckpoint();

                continue;
            }

            try {
                // Create a minimal VideoGame-like object with just the ID
                // The attach callback only needs the video_game_id
                $videoGame = new \stdClass;
                $videoGame->id = $videoGameId;

                $attach($videoGame, $record);
                $processed++;
                $updateProgress();
                $maybeCheckpoint();
            } catch (\Exception) {
                // Skip problematic rows without halting the stream
                $errors++;
                $updateProgress();
                $maybeCheckpoint();
            }
        }

        // Final checkpoint before closing
        $maybeCheckpoint(true);

        $progressBar->finish();
        fclose($handle);

        // Flush any remaining media batches
        $this->flushImageBatch();
        $this->flushVideoBatch();

        // Remove checkpoint when fully consumed
        $this->forgetCheckpoint($file, $provider);

        $this->newLine();

        return $processed;
    }

    /**
     * Pre-load all game ID mappings for a provider into memory.
     * Returns array mapping external_id => video_game_id.
     *
     * @return array<string, int>
     */
    private function preloadGameIdMappings(string $provider): array
    {
        $this->info("Pre-loading game ID mappings for provider: {$provider}...");

        // Get the video game source
        $source = $this->sourceCache[$provider]
            ?? VideoGameSource::query()->where('provider', $provider)->first();

        if (! $source) {
            $this->warn("No video game source found for provider: {$provider}");

            return [];
        }

        $this->sourceCache[$provider] = $source;

        // Fetch all mappings: provider_item_id => video_game_title_id
        $titleMappings = VideoGameTitleSource::query()
            ->where('video_game_source_id', $source->id)
            ->select(['provider_item_id', 'video_game_title_id'])
            ->get()
            ->pluck('video_game_title_id', 'provider_item_id')
            ->toArray();

        if (empty($titleMappings)) {
            $this->warn("No title mappings found for provider: {$provider}");

            return [];
        }

        // Get unique title IDs
        $titleIds = array_unique(array_values($titleMappings));

        $this->info('Fetching video games for '.count($titleIds).' titles...');

        // Fetch all video games for these titles: video_game_title_id => video_game_id
        // Use chunking to avoid parameter limits (PostgreSQL has limits on array sizes)
        $videoGameMappings = [];
        $chunkSize = 10000;

        foreach (array_chunk($titleIds, $chunkSize) as $chunk) {
            $results = VideoGame::query()
                ->whereIn('video_game_title_id', $chunk)
                ->select(['id', 'video_game_title_id'])
                ->get()
                ->pluck('id', 'video_game_title_id')
                ->toArray();

            $videoGameMappings = array_merge($videoGameMappings, $results);
        }

        // Combine: external_id => video_game_id
        $gameIdMap = [];
        foreach ($titleMappings as $externalId => $titleId) {
            if (isset($videoGameMappings[$titleId])) {
                $gameIdMap[$externalId] = $videoGameMappings[$titleId];
            }
        }

        $count = count($gameIdMap);
        $this->info("Loaded {$count} game ID mappings into memory.");

        return $gameIdMap;
    }

    private function findVideoGameByExternalId(string $provider, string $externalId): ?VideoGame
    {
        // Fast path for media imports: resolve provider -> source id once, then use the
        // composite unique index (video_game_source_id, provider_item_id) on the mapping table.
        $source = $this->sourceCache[$provider]
            ?? VideoGameSource::query()->where('provider', $provider)->first();

        if (! $source) {
            return null;
        }

        $this->sourceCache[$provider] = $source;

        $titleId = VideoGameTitleSource::query()
            ->where('video_game_source_id', $source->id)
            ->where('provider_item_id', $externalId)
            ->value('video_game_title_id');

        if (! $titleId) {
            return null;
        }

        return VideoGame::query()
            ->where('video_game_title_id', $titleId)
            ->first();
    }

    private function platformNormalizer(): PlatformNormalizer
    {
        return $this->platformNormalizer ??= app(PlatformNormalizer::class);
    }

    private function igdbRatingHelper(): IgdbRatingHelper
    {
        return $this->igdbRatingHelper ??= app(IgdbRatingHelper::class);
    }

    /**
     * @param  array<int, string>  $headers
     * @param  array<int, string>  $row
     * @return array<string, string>|null
     */
    private function combineCsvRow(array $headers, array $row): ?array
    {
        if ($headers === []) {
            return null;
        }

        $headerCount = count($headers);
        $rowCount = count($row);

        if ($rowCount < $headerCount) {
            $row = array_pad($row, $headerCount, '');
        } elseif ($rowCount > $headerCount) {
            $row = array_slice($row, 0, $headerCount);
        }

        $combined = array_combine($headers, $row);

        return is_array($combined) ? $combined : null;
    }

    /**
     * Parse IGDB "set" strings like "{6,48}" into an integer list.
     *
     * @return array<int>
     */
    private function parseIgdbIdSetString(string $value): array
    {
        $raw = trim($value);

        if ($raw === '' || $raw === '{}' || $raw === '{NULL}') {
            return [];
        }

        // Remove curly braces if present.
        if (str_starts_with($raw, '{') && str_ends_with($raw, '}')) {
            $raw = substr($raw, 1, -1);
        }

        $raw = trim($raw);
        if ($raw === '') {
            return [];
        }

        $parts = array_values(array_filter(array_map('trim', explode(',', $raw)), fn ($p) => $p !== ''));
        $ids = [];

        foreach ($parts as $p) {
            if (ctype_digit($p)) {
                $ids[] = (int) $p;
            }
        }

        return array_values(array_unique($ids));
    }

    private function csvHasDataRows(string $filePath): bool
    {
        if (! File::exists($filePath) || ! File::isFile($filePath)) {
            return false;
        }

        $handle = fopen($filePath, 'r');
        if ($handle === false) {
            return false;
        }

        try {
            // Read and ignore header.
            $header = fgetcsv($handle);
            if ($header === false) {
                return false;
            }

            // If there is at least one additional CSV row, it's not header-only.
            return fgetcsv($handle) !== false;
        } finally {
            fclose($handle);
        }
    }

    private function ensureReferenceCsv(string $directory, string $basename, string $fallbackFilename, string $fetchCommand): ?string
    {
        $fallback = rtrim($directory, '/').'/'.$fallbackFilename;

        $pickBest = function () use ($directory, $basename, $fallback): ?string {
            $primary = $basename === 'companies'
                ? $this->findCompaniesFile($directory)
                : $this->findFile($directory, $basename);

            if (is_string($primary) && strtolower(pathinfo($primary, PATHINFO_EXTENSION)) === 'csv' && $this->csvHasDataRows($primary)) {
                return $primary;
            }

            if ($this->csvHasDataRows($fallback)) {
                return $fallback;
            }

            return null;
        };

        $best = $pickBest();
        if ($best !== null) {
            return $best;
        }

        $primary = $this->findFile($directory, $basename);
        $hasCandidateFile = (is_string($primary) && strtolower(pathinfo($primary, PATHINFO_EXTENSION)) === 'csv')
            || File::exists($fallback);

        // Only auto-download when a reference file exists but is header-only.
        // If the dump is entirely missing, we avoid making network calls implicitly.
        if (! $hasCandidateFile) {
            return null;
        }

        $storageRoot = storage_path();
        $outputDir = Str::startsWith($directory, $storageRoot)
            ? ltrim(Str::replaceFirst($storageRoot, '', $directory), DIRECTORY_SEPARATOR)
            : 'igdb-dumps';

        $this->warn("{$basename} reference dump is empty or missing. Attempting to download the official IGDB dump...");
        $exitCode = $this->call('igdb:dump:download', ['endpoint' => $basename, '--output-dir' => $outputDir]);

        $best = $pickBest();
        if ($exitCode === self::SUCCESS && $best !== null) {
            return $best;
        }

        $this->warn("Unable to download an official dump for {$basename}. Falling back to a v4 API fetch (best-effort)...");
        $exitCode = $this->call($fetchCommand, ['--out' => $fallback]);

        return $exitCode === self::SUCCESS && $this->csvHasDataRows($fallback) ? $fallback : null;
    }

    private function loadPlatformIdToNameMap(string $path): void
    {
        if ($this->platformIdToName !== []) {
            return;
        }

        $platformsFile = $this->ensureReferenceCsv($path, 'platforms', 'platforms.csv', 'gc:fetch-igdb-platforms');
        if (! $platformsFile) {
            return;
        }

        $this->info('🧩 Loading platform reference map...');

        $handle = fopen($platformsFile, 'r');
        if (! $handle) {
            return;
        }

        try {
            $headers = fgetcsv($handle) ?: [];
            $idKey = null;
            $nameKey = null;

            foreach ($headers as $h) {
                $h = strtolower((string) $h);
                if ($h === 'id') {
                    $idKey = 'id';
                }
                if ($h === 'name') {
                    $nameKey = 'name';
                }
            }

            if ($idKey === null || $nameKey === null) {
                return;
            }

            // Count remaining lines for a progress bar.
            $totalLines = 0;
            while (fgets($handle) !== false) {
                $totalLines++;
            }
            rewind($handle);
            fgetcsv($handle); // Skip header again

            $progressBar = $this->output->createProgressBar($totalLines);
            $this->configureProgressBar($progressBar, false);
            $progressBar->setMessage('0', 'processed');
            $progressBar->setMessage('0', 'skipped');
            $progressBar->setMessage('0', 'errors');

            $processed = 0;
            $skipped = 0;

            while (($row = fgetcsv($handle)) !== false) {
                $record = $this->combineCsvRow($headers, $row);
                if (! $record) {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $idRaw = $record['id'] ?? null;
                $name = $record['name'] ?? null;

                if (! is_string($idRaw) || ! ctype_digit($idRaw) || ! is_string($name) || $name === '') {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $this->platformIdToName[(int) $idRaw] = $name;

                $processed++;
                $progressBar->setMessage((string) $processed, 'processed');
                $progressBar->advance();
            }

            $progressBar->finish();
            $this->newLine();
        } finally {
            fclose($handle);
        }
    }

    private function loadGenreIdToNameMap(string $path): void
    {
        if ($this->genreIdToName !== []) {
            return;
        }

        $genresFile = $this->ensureReferenceCsv($path, 'genres', 'genres.csv', 'gc:fetch-igdb-genres');
        if (! $genresFile) {
            return;
        }

        $this->info('🏷️  Loading genre reference map...');

        $handle = fopen($genresFile, 'r');
        if (! $handle) {
            return;
        }

        try {
            $headers = fgetcsv($handle) ?: [];

            if (! in_array('id', $headers, true) || ! in_array('name', $headers, true)) {
                return;
            }

            $totalLines = 0;
            while (fgets($handle) !== false) {
                $totalLines++;
            }
            rewind($handle);
            fgetcsv($handle);

            $progressBar = $this->output->createProgressBar($totalLines);
            $this->configureProgressBar($progressBar, false);
            $progressBar->setMessage('0', 'processed');
            $progressBar->setMessage('0', 'skipped');
            $progressBar->setMessage('0', 'errors');

            $processed = 0;
            $skipped = 0;

            while (($row = fgetcsv($handle)) !== false) {
                $record = $this->combineCsvRow($headers, $row);
                if (! $record) {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $idRaw = $record['id'] ?? null;
                $name = $record['name'] ?? null;

                if (! is_string($idRaw) || ! ctype_digit($idRaw) || ! is_string($name) || $name === '') {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $this->genreIdToName[(int) $idRaw] = $name;
                $processed++;
                $progressBar->setMessage((string) $processed, 'processed');
                $progressBar->advance();
            }

            $progressBar->finish();
            $this->newLine();
        } finally {
            fclose($handle);
        }
    }

    private function loadCompanyAndInvolvedCompanyMaps(string $path): void
    {
        if ($this->companyIdToName !== [] || $this->involvedCompanyIdToCompanyRole !== []) {
            return;
        }

        $companiesFile = $this->ensureReferenceCsv($path, 'companies', 'companies.csv', 'gc:fetch-igdb-companies');
        $involvedCompaniesFile = $this->ensureReferenceCsv($path, 'involved_companies', 'involved_companies.csv', 'gc:fetch-igdb-involved-companies');

        if (! $companiesFile || ! $involvedCompaniesFile) {
            return;
        }

        $this->info('🏢 Loading companies + involved companies maps...');

        $this->loadCompanyIdToNameMap($companiesFile);
        $this->loadInvolvedCompanyMap($involvedCompaniesFile);
    }

    private function findCompaniesFile(string $directory): ?string
    {
        $candidates = [];

        foreach (File::files($directory) as $file) {
            $filename = strtolower($file->getFilename());
            $ext = strtolower($file->getExtension());

            if ($ext !== 'csv') {
                continue;
            }

            if (! str_contains($filename, 'companies')) {
                continue;
            }

            if (str_contains($filename, 'involved_companies')) {
                continue;
            }

            if (str_ends_with($filename, '_schema.json') || str_ends_with($filename, 'schema.json')) {
                continue;
            }

            $candidates[] = $file;
        }

        if ($candidates === []) {
            return null;
        }

        usort($candidates, function ($a, $b): int {
            $aName = strtolower($a->getFilename());
            $bName = strtolower($b->getFilename());

            $aTs = preg_match('/^(\d+)_/', $aName, $m1) === 1 ? (int) $m1[1] : 0;
            $bTs = preg_match('/^(\d+)_/', $bName, $m2) === 1 ? (int) $m2[1] : 0;
            if ($aTs !== $bTs) {
                return $bTs <=> $aTs;
            }

            return $b->getSize() <=> $a->getSize();
        });

        return $candidates[0]->getPathname();
    }

    private function loadCompanyIdToNameMap(string $companiesFile): void
    {
        $handle = fopen($companiesFile, 'r');
        if (! $handle) {
            return;
        }

        try {
            $headers = fgetcsv($handle) ?: [];
            if (! in_array('id', $headers, true) || ! in_array('name', $headers, true)) {
                return;
            }

            $totalLines = 0;
            while (fgets($handle) !== false) {
                $totalLines++;
            }
            rewind($handle);
            fgetcsv($handle);

            $progressBar = $this->output->createProgressBar($totalLines);
            $this->configureProgressBar($progressBar, false);
            $progressBar->setMessage('0', 'processed');
            $progressBar->setMessage('0', 'skipped');
            $progressBar->setMessage('0', 'errors');

            $processed = 0;
            $skipped = 0;

            while (($row = fgetcsv($handle)) !== false) {
                $record = $this->combineCsvRow($headers, $row);
                if (! $record) {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $idRaw = $record['id'] ?? null;
                $name = $record['name'] ?? null;

                if (! is_string($idRaw) || ! ctype_digit($idRaw) || ! is_string($name) || $name === '') {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $this->companyIdToName[(int) $idRaw] = $name;
                $processed++;
                $progressBar->setMessage((string) $processed, 'processed');
                $progressBar->advance();
            }

            $progressBar->finish();
            $this->newLine();
        } finally {
            fclose($handle);
        }
    }

    private function loadInvolvedCompanyMap(string $involvedCompaniesFile): void
    {
        $handle = fopen($involvedCompaniesFile, 'r');
        if (! $handle) {
            return;
        }

        try {
            $headers = fgetcsv($handle) ?: [];
            if (! in_array('id', $headers, true) || ! in_array('company', $headers, true)) {
                return;
            }

            $totalLines = 0;
            while (fgets($handle) !== false) {
                $totalLines++;
            }
            rewind($handle);
            fgetcsv($handle);

            $progressBar = $this->output->createProgressBar($totalLines);
            $this->configureProgressBar($progressBar, false);
            $progressBar->setMessage('0', 'processed');
            $progressBar->setMessage('0', 'skipped');
            $progressBar->setMessage('0', 'errors');

            $processed = 0;
            $skipped = 0;

            while (($row = fgetcsv($handle)) !== false) {
                $record = $this->combineCsvRow($headers, $row);
                if (! $record) {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $idRaw = $record['id'] ?? null;
                $companyRaw = $record['company'] ?? null;
                if (! is_string($idRaw) || ! ctype_digit($idRaw) || ! is_string($companyRaw) || ! ctype_digit($companyRaw)) {
                    $skipped++;
                    $progressBar->setMessage((string) $skipped, 'skipped');
                    $progressBar->advance();

                    continue;
                }

                $developer = $this->parseBoolish($record['developer'] ?? null);
                $publisher = $this->parseBoolish($record['publisher'] ?? null);

                $this->involvedCompanyIdToCompanyRole[(int) $idRaw] = [
                    'company_id' => (int) $companyRaw,
                    'developer' => $developer,
                    'publisher' => $publisher,
                ];

                $processed++;
                $progressBar->setMessage((string) $processed, 'processed');
                $progressBar->advance();
            }

            $progressBar->finish();
            $this->newLine();
        } finally {
            fclose($handle);
        }
    }

    private function parseBoolish(mixed $value): bool
    {
        if (is_bool($value)) {
            return $value;
        }

        if (is_int($value)) {
            return $value === 1;
        }

        if (! is_string($value)) {
            return false;
        }

        $v = strtolower(trim($value));

        return in_array($v, ['1', 't', 'true', 'yes', 'y'], true);
    }

    private function configureProgressBar(ProgressBar $progressBar, bool $includeElapsed): void
    {
        if ($includeElapsed) {
            $progressBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | Elapsed: %elapsed:6s% | Errors: %errors% | Skipped: %skipped%');

            return;
        }

        $progressBar->setFormat('    %current%/%max% [%bar%] %percent:3s%% | Processed: %processed% | Skipped: %skipped% | Errors: %errors%');
    }

    private function parseNumeric(mixed $value): ?float
    {
        if ($value === null || $value === '') {
            return null;
        }

        if (! is_numeric($value)) {
            return null;
        }

        return (float) $value;
    }

    private function parseInt(mixed $value): ?int
    {
        if ($value === null || $value === '') {
            return null;
        }

        return is_numeric($value) ? (int) $value : null;
    }
}
