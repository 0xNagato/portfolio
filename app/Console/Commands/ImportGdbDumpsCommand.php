<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\Gdb\GdbDumpImporter;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\File;

class ImportGdbDumpsCommand extends Command
{
    protected $signature = 'gc:import-gdb {--path= : Directory path containing gdb dump files} {--provider=giantbomb : Provider key to store on video_game_sources.provider}';

    protected $description = 'Import GiantBomb (GDB) dump files from storage/gdb-dumps into products/video_game_titles/video_games.';

    public function handle(GdbDumpImporter $importer): int
    {
        $path = (string) ($this->option('path') ?: base_path('storage/gdb-dumps'));
        $provider = (string) ($this->option('provider') ?: 'giantbomb');

        if (! File::exists($path)) {
            $this->error("Directory does not exist: {$path}");

            return self::FAILURE;
        }

        $files = collect(File::files($path))
            ->filter(fn ($f) => $f->isFile())
            ->values();

        if ($files->isEmpty()) {
            $this->warn("No files found in: {$path}");

            return self::SUCCESS;
        }

        $totals = ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0];

        foreach ($files as $file) {
            $this->info("Importing {$file->getFilename()}...");

            foreach ($this->iterateRecordsFromFile($file->getPathname()) as $record) {
                $delta = $importer->importRecord($record, $provider);

                foreach ($totals as $k => $v) {
                    $totals[$k] += $delta[$k] ?? 0;
                }
            }
        }

        $this->newLine();
        $this->table(['Metric', 'Count'], [
            ['Products created', $totals['products']],
            ['Sources created', $totals['sources']],
            ['Titles created', $totals['titles']],
            ['VideoGames created', $totals['video_games']],
        ]);

        return self::SUCCESS;
    }

    /**
     * @return \Generator<int, array>
     */
    private function iterateRecordsFromFile(string $filePath): \Generator
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));

        // NDJSON/JSONL support.
        if (in_array($ext, ['ndjson', 'jsonl'], true)) {
            $handle = fopen($filePath, 'rb');

            if ($handle === false) {
                return;
            }

            try {
                while (($line = fgets($handle)) !== false) {
                    $line = trim($line);

                    if ($line === '') {
                        continue;
                    }

                    $decoded = json_decode($line, true);

                    if (is_array($decoded)) {
                        yield $decoded;
                    }
                }
            } finally {
                fclose($handle);
            }

            return;
        }

        // Regular JSON (array, or an object with a list under common keys).
        $raw = File::get($filePath);
        $decoded = json_decode($raw, true);

        if (is_array($decoded) && array_is_list($decoded)) {
            foreach ($decoded as $row) {
                if (is_array($row)) {
                    yield $row;
                }
            }

            return;
        }

        if (is_array($decoded)) {
            foreach (['results', 'games', 'data'] as $key) {
                $maybe = $decoded[$key] ?? null;

                if (is_array($maybe) && array_is_list($maybe)) {
                    foreach ($maybe as $row) {
                        if (is_array($row)) {
                            yield $row;
                        }
                    }

                    return;
                }
            }
        }
    }
}
