<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Symfony\Component\Stopwatch\Stopwatch;

class BenchmarkImportCommand extends Command
{
    protected $signature = 'gc:benchmark-import {--path= : Directory containing IGDB dump files} {--provider=igdb : Provider key} {--limit=100 : Number of records to import}';

    protected $description = 'Benchmark the import command with detailed timing metrics.';

    public function handle(): int
    {
        $path = (string) ($this->option('path') ?: base_path('storage/igdb-dumps'));
        $provider = (string) ($this->option('provider') ?: 'igdb');
        $limit = (int) ($this->option('limit') ?: 100);

        $stopwatch = new Stopwatch;

        // Clear previous test data
        $this->info('📊 Clearing previous test data...');
        DB::table('video_games')->delete();
        DB::table('video_game_titles')->delete();
        DB::table('video_game_sources')->delete();
        DB::table('products')->delete();

        // Measure import time
        $this->info('🚀 Starting benchmark import...');
        $stopwatch->start('import');

        $this->call('gc:import-igdb', [
            '--path' => $path,
            '--provider' => $provider,
            '--limit' => $limit,
        ]);

        $event = $stopwatch->stop('import');

        // Calculate metrics
        $duration = $event->getDuration() / 1000; // Convert to seconds
        $recordsPerSecond = $limit > 0 ? $limit / $duration : 0;
        $avgTimePerRecord = $limit > 0 ? ($duration * 1000) / $limit : 0; // in milliseconds

        // Database stats
        $productCount = DB::table('products')->count();
        $sourceCount = DB::table('video_game_sources')->count();
        $titleCount = DB::table('video_game_titles')->count();
        $gameCount = DB::table('video_games')->count();

        $this->newLine();
        $this->info('✅ Benchmark Complete!');
        $this->newLine();

        $this->table(
            ['Metric', 'Value'],
            [
                ['Duration', number_format($duration, 2).' seconds'],
                ['Records Processed', number_format($limit)],
                ['Records/Second', number_format($recordsPerSecond, 2)],
                ['Avg Time/Record', number_format($avgTimePerRecord, 2).' ms'],
                ['', ''],
                ['Products Created', number_format($productCount)],
                ['Sources Created', number_format($sourceCount)],
                ['Titles Created', number_format($titleCount)],
                ['Video Games Created', number_format($gameCount)],
            ]
        );

        // Memory info
        $peakMemory = memory_get_peak_usage(true) / 1024 / 1024;
        $this->newLine();
        $this->info("💾 Peak Memory Usage: {$peakMemory} MB");

        // Query count (if available)
        $queryCount = count(DB::getQueryLog());
        if ($queryCount > 0) {
            $this->info("📝 Queries Executed: {$queryCount}");
            $avgQueriesPerRecord = $queryCount > 0 ? $queryCount / $limit : 0;
            $this->info("📊 Avg Queries/Record: {$avgQueriesPerRecord}");
        }

        $this->newLine();
        $this->info('💡 Optimization Tips:');
        $this->info('- Batch size is set to 100 records per insert');
        $this->info('- Query logging is disabled for faster execution');
        $this->info('- In-memory caching reduces database lookups');
        $this->info('- Consider increasing batch size for very large imports');

        return self::SUCCESS;
    }
}
