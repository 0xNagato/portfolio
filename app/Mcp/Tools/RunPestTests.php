<?php

declare(strict_types=1);

namespace App\Mcp\Tools;

use Illuminate\Support\Facades\Process;
use Laravel\Mcp\Attributes\McpTool;
use Laravel\Mcp\Schema;

#[McpTool(
    name: 'run_pest_tests',
    description: 'Run Pest tests with optional filters and scopes'
)]
class RunPestTests
{
    #[Schema(enum: ['unit', 'feature', 'all'])]
    public string $type = 'all';

    #[Schema(description: 'Filter tests by name pattern (e.g., SchemaInvariants)')]
    public string $filter = '';

    #[Schema(description: 'Run tests in parallel')]
    public bool $parallel = false;

    public function __invoke(): array
    {
        $args = ['php', 'artisan', 'test', '--json'];

        if ($this->filter) {
            $args[] = '--filter=' . $this->filter;
        }

        if ($this->type !== 'all') {
            $args[] = 'tests/' . ucfirst($this->type);
        }

        if ($this->parallel) {
            $args[] = '--parallel';
        }

        $result = Process::run($args);
        $output = $result->output();

        // Try to extract JSON from output
        if (preg_match('/{.*}/s', $output, $matches)) {
            $decoded = json_decode($matches[0], true);
        } else {
            $decoded = null;
        }

        if ($decoded === null) {
            // Fallback: parse text output
            preg_match('/Tests:\s+(\d+)\s+passed/', $output, $passedMatch);
            preg_match('/Tests:\s+\d+\s+passed.*?(\d+)\s+failed/', $output, $failedMatch);

            return [
                'success' => !str_contains($output, 'failed'),
                'type' => $this->type,
                'filter' => $this->filter ?: 'none',
                'passed' => (int)($passedMatch[1] ?? 0),
                'failed' => (int)($failedMatch[1] ?? 0),
                'raw_output' => $output,
            ];
        }

        return [
            'success' => ($decoded['failed'] ?? 0) === 0,
            'type' => $this->type,
            'filter' => $this->filter ?: 'none',
            'total' => $decoded['total'] ?? 0,
            'passed' => $decoded['passed'] ?? 0,
            'failed' => $decoded['failed'] ?? 0,
            'warnings' => $decoded['warnings'] ?? 0,
            'duration' => $decoded['duration'] ?? 0,
            'failures' => array_map(fn($f) => [
                'test' => $f['name'] ?? '',
                'message' => $f['message'] ?? '',
                'file' => $f['file'] ?? '',
                'line' => $f['line'] ?? 0,
            ], $decoded['failures'] ?? []),
        ];
    }
}
