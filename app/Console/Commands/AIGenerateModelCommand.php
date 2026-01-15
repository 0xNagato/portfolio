<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\AICodeGeneratorService;
use Illuminate\Console\Command;

class AIGenerateModelCommand extends Command
{
    protected $signature = 'ai:generate-model {table : The database table name}';

    protected $description = 'Use AI to generate an Eloquent model from database schema';

    public function handle(AICodeGeneratorService $aiService): int
    {
        $table = $this->argument('table');

        $this->info("🤖 Generating model for table: {$table}...");

        try {
            $modelCode = $aiService->generateModel($table);

            $this->line("\n" . $modelCode . "\n");

            if ($this->confirm('Save this model?')) {
                $modelName = str($table)->singular()->studly();
                $path = app_path("Models/{$modelName}.php");

                file_put_contents($path, $modelCode);

                $this->info("✅ Model saved to: {$path}");
            }

            return Command::SUCCESS;
        } catch (\Exception $e) {
            $this->error("❌ Failed: {$e->getMessage()}");
            return Command::FAILURE;
        }
    }
}
