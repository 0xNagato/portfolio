<?php

declare(strict_types=1);

namespace App\Services;

use Illuminate\Support\Facades\Process;
use Illuminate\Support\Facades\Http;

/**
 * AI Code Generator Service
 *
 * Uses MCP tools to generate code, analyze schemas, and automate development tasks
 */
class AICodeGeneratorService
{
    /**
     * Generate a Laravel model based on table schema
     */
    public function generateModel(string $tableName): string
    {
        // Use Laravel Boost MCP to get table schema
        $schema = $this->getTableSchema($tableName);

        // Use Claude via MCP Gateway to generate model code
        $prompt = "Generate a Laravel 12 Eloquent model for table '{$tableName}' with this schema: " . json_encode($schema);

        return $this->callAI($prompt, 'code-generation');
    }

    /**
     * Auto-fix PHPStan errors in a file
     */
    public function autoFixTypeErrors(string $filePath): array
    {
        // Run PHPStan analysis
        $result = Process::run([
            './vendor/bin/phpstan',
            'analyse',
            $filePath,
            '--error-format=json',
            '--no-progress',
        ]);

        $errors = json_decode($result->output(), true);

        if (empty($errors['files'])) {
            return ['success' => true, 'message' => 'No errors found'];
        }

        // Use AI to fix each error
        $fixes = [];
        foreach ($errors['files'] as $file => $fileErrors) {
            $fileContent = file_get_contents($file);

            $prompt = "Fix these PHPStan errors in the following code:\n\n";
            $prompt .= "Errors: " . json_encode($fileErrors['messages']) . "\n\n";
            $prompt .= "Code:\n" . $fileContent;

            $fixes[$file] = $this->callAI($prompt, 'code-fix');
        }

        return [
            'success' => true,
            'fixes' => $fixes,
        ];
    }

    /**
     * Generate migration from natural language description
     */
    public function generateMigration(string $description): string
    {
        $prompt = "Generate a Laravel 12 migration for: {$description}\n\n";
        $prompt .= "Follow Laravel conventions. Use proper column types, indexes, and foreign keys.";

        return $this->callAI($prompt, 'migration-generation');
    }

    /**
     * Validate and fix schema invariants
     */
    public function validateSchemaInvariants(): array
    {
        // Call our custom ValidateSchemaInvariants MCP tool
        $result = app(\App\Mcp\Tools\ValidateSchemaInvariants::class)();

        if (!$result['valid']) {
            // Use AI to suggest fixes
            $prompt = "The schema has these violations: " . json_encode($result['violations']) . "\n\n";
            $prompt .= "Generate migrations to fix these schema invariants.";

            $fixes = $this->callAI($prompt, 'schema-fix');
            $result['ai_suggested_fixes'] = $fixes;
        }

        return $result;
    }

    /**
     * Generate Pest tests for a class
     */
    public function generateTests(string $className): string
    {
        $reflection = new \ReflectionClass($className);
        $methods = $reflection->getMethods(\ReflectionMethod::IS_PUBLIC);

        $prompt = "Generate Pest tests for this class:\n\n";
        $prompt .= "Class: {$className}\n";
        $prompt .= "Methods: " . implode(', ', array_map(fn($m) => $m->getName(), $methods)) . "\n\n";
        $prompt .= "Include happy path, edge cases, and error cases.";

        return $this->callAI($prompt, 'test-generation');
    }

    /**
     * Optimize query performance
     */
    public function optimizeQuery(string $query): array
    {
        $prompt = "Analyze and optimize this database query:\n\n{$query}\n\n";
        $prompt .= "Suggest: indexes, query rewriting, eager loading, caching strategies.";

        $suggestions = $this->callAI($prompt, 'query-optimization');

        return [
            'original_query' => $query,
            'suggestions' => $suggestions,
        ];
    }

    /**
     * Auto-generate API documentation
     */
    public function generateApiDocs(string $controllerPath): string
    {
        $code = file_get_contents($controllerPath);

        $prompt = "Generate OpenAPI 3.0 documentation for this Laravel controller:\n\n{$code}";

        return $this->callAI($prompt, 'documentation');
    }

    /**
     * Call AI via MCP Gateway or direct Anthropic API
     */
    protected function callAI(string $prompt, string $category = 'general'): string
    {
        // Option 1: Use MCP Gateway (if running)
        try {
            $response = Http::post('http://localhost:8811/tools/advanced-reasoning/generate', [
                'prompt' => $prompt,
                'category' => $category,
            ]);

            if ($response->successful()) {
                return $response->json('result', '');
            }
        } catch (\Exception $e) {
            // Fall through to Option 2
        }

        // Option 2: Direct Anthropic API call
        $apiKey = config('services.anthropic.api_key');

        if (!$apiKey) {
            throw new \Exception('Anthropic API key not configured');
        }

        $response = Http::withHeaders([
            'x-api-key' => $apiKey,
            'anthropic-version' => '2023-06-01',
            'content-type' => 'application/json',
        ])->post('https://api.anthropic.com/v1/messages', [
            'model' => 'claude-opus-4-5',
            'max_tokens' => 4096,
            'messages' => [
                [
                    'role' => 'user',
                    'content' => $prompt,
                ],
            ],
        ]);

        if (!$response->successful()) {
            throw new \Exception('AI API call failed: ' . $response->body());
        }

        return $response->json('content.0.text', '');
    }

    /**
     * Get table schema using Laravel Boost MCP
     */
    protected function getTableSchema(string $tableName): array
    {
        $result = Process::run([
            'php',
            base_path('artisan'),
            'boost:mcp',
            '--tool=database-schema',
            '--table=' . $tableName,
        ]);

        return json_decode($result->output(), true) ?? [];
    }
}
