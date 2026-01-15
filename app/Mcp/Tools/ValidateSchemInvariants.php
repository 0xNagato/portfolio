<?php

declare(strict_types=1);

namespace App\Mcp\Tools;

use Illuminate\Support\Facades\Schema;
use Laravel\Mcp\Attributes\McpTool;

#[McpTool(
    name: 'validate_schema_invariants',
    description: 'Validates critical schema invariants for the game pricing domain'
)]
class ValidateSchemaInvariants
{
    public function __invoke(): array
    {
        $violations = [];
        $checks = [];

        // Check 1: Only video_game_titles should have product_id FK
        $checks['video_game_titles_has_product_id'] = Schema::hasColumn('video_game_titles', 'product_id');
        if (!$checks['video_game_titles_has_product_id']) {
            $violations[] = 'CRITICAL: video_game_titles missing product_id column';
        }

        // Check 2: video_games must NOT have product_id
        $checks['video_games_no_product_id'] = !Schema::hasColumn('video_games', 'product_id');
        if (!$checks['video_games_no_product_id']) {
            $violations[] = 'CRITICAL: video_games has product_id (violates schema invariant)';
        }

        // Check 3: video_game_sources must NOT have product_id
        $checks['video_game_sources_no_product_id'] = !Schema::hasColumn('video_game_sources', 'product_id');
        if (!$checks['video_game_sources_no_product_id']) {
            $violations[] = 'CRITICAL: video_game_sources has product_id (violates schema invariant)';
        }

        // Check 4: video_game_title_sources must NOT have product_id
        $checks['video_game_title_sources_no_product_id'] = !Schema::hasColumn('video_game_title_sources', 'product_id');
        if (!$checks['video_game_title_sources_no_product_id']) {
            $violations[] = 'CRITICAL: video_game_title_sources has product_id (violates schema invariant)';
        }

        // Check 5: video_games must point to video_game_titles
        $checks['video_games_has_title_fk'] = Schema::hasColumn('video_games', 'video_game_title_id');
        if (!$checks['video_games_has_title_fk']) {
            $violations[] = 'CRITICAL: video_games missing video_game_title_id column';
        }

        // Check 6: Provider mappings in video_game_title_sources
        $checks['title_sources_has_provider_id'] = Schema::hasColumn('video_game_title_sources', 'provider_item_id');
        if (!$checks['title_sources_has_provider_id']) {
            $violations[] = 'CRITICAL: video_game_title_sources missing provider_item_id';
        }

        $checks['title_sources_has_raw_payload'] = Schema::hasColumn('video_game_title_sources', 'raw_payload');
        if (!$checks['title_sources_has_raw_payload']) {
            $violations[] = 'CRITICAL: video_game_title_sources missing raw_payload';
        }

        return [
            'valid' => count($violations) === 0,
            'checks_performed' => count($checks),
            'checks_passed' => count(array_filter($checks)),
            'checks_failed' => count($checks) - count(array_filter($checks)),
            'violations' => $violations,
            'detailed_checks' => $checks,
            'invariant_summary' => [
                'products_to_titles_only' => $checks['video_game_titles_has_product_id'] && $checks['video_games_no_product_id'],
                'titles_to_games_traversal' => $checks['video_games_has_title_fk'],
                'provider_mappings_correct' => $checks['title_sources_has_provider_id'] && $checks['title_sources_has_raw_payload'],
            ],
        ];
    }
}
