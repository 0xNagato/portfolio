<?php

declare(strict_types=1);

namespace App\Mcp\Tools;

use Illuminate\Support\Facades\Process;
use Laravel\Mcp\Attributes\McpTool;
use Laravel\Mcp\Schema;

#[McpTool(
    name: 'analyze_code_quality',
    description: 'Run PHPStan static analysis on models, commands, or services'
)]
class AnalyzeCodeQuality
{
    #[Schema(enum: ['models', 'commands', 'services', 'all'])]
    public string $scope = 'all';

    #[Schema(minimum: 0, maximum: 9, description: 'PHPStan level (0-9, higher = stricter)')]
    public int $level = 6;

    public function __invoke(): array
    {
        $paths = match ($this->scope) {
            'models' => 'app/Models',
            'commands' => 'app/Console/Commands',
            'services' => 'app/Services',
            'all' => 'app/',
        };

        $result = Process::run([
            './vendor/bin/phpstan',
            'analyse',
            $paths,
            '--level=' . $this->level,
            '--error-format=json',
            '--no-progress',
        ]);

        $decoded = json_decode($result->output(), true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            return [
                'success' => false,
                'error' => 'Failed to parse PHPStan output',
                'raw_output' => $result->output(),
            ];
        }

        return [
            'success' => true,
            'scope' => $this->scope,
            'level' => $this->level,
            'total_errors' => $decoded['totals']['file_errors'] ?? 0,
            'files_analyzed' => count($decoded['files'] ?? []),
            'errors' => array_map(function ($file, $errors) {
                return [
                    'file' => $file,
                    'errors' => array_map(fn($e) => [
                        'line' => $e['line'],
                        'message' => $e['message'],
                    ], $errors['messages'] ?? []),
                ];
            }, array_keys($decoded['files'] ?? []), array_values($decoded['files'] ?? [])),
        ];
    }
}
