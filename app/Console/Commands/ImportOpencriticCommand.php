<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use App\Models\VideoGame;
use Illuminate\Support\Facades\Log;

class ImportOpencriticCommand extends Command
{
    protected $signature = 'gc:import-opencritic {--limit=0 : Limit number of games to process} {--force : Force update existing scores}';
    protected $description = 'Fetch OpenCritic scores for games (using name matching)';

    private const OPENCRITIC_API_URL = 'https://api.opencritic.com/api/game/search';

    public function handle()
    {
        $this->info('Starting OpenCritic Import...');
        $limit = (int) $this->option('limit');
        $force = (bool) $this->option('force');

        $query = VideoGame::query()->whereNotNull('name');
        
        if (!$force) {
            $query->whereNull('opencritic_score');
        }

        if ($limit > 0) {
            $query->limit($limit);
        }

        $query->chunk(50, function ($games) {
            foreach ($games as $game) {
                try {
                    $this->fetchAndSaveScore($game);
                    // Rate limiting: OpenCritic is generous but let's be polite
                    usleep(200000); // 0.2s
                } catch (\Exception $e) {
                    Log::error("Failed to fetch OpenCritic score for game {$game->id}: " . $e->getMessage());
                    $this->error("Error processing {$game->name}");
                }
            }
        });

        $this->info('OpenCritic Import Complete.');
    }

    private function fetchAndSaveScore(VideoGame $game)
    {
        // 1. Search for the game
        $response = Http::get(self::OPENCRITIC_API_URL, [
            'criteria' => $game->name
        ]);

        if ($response->failed()) {
            $this->warn("API Error for {$game->name}");
            return;
        }

        $results = $response->json();
        if (empty($results)) {
            $this->line("No results found for: {$game->name}");
            return;
        }

        // 2. Find best match (simplified: just take the first one or exact name match)
        $bestMatch = null;
        foreach ($results as $result) {
            if (strcasecmp($result['name'], $game->name) === 0) {
                $bestMatch = $result;
                break;
            }
        }

        if (!$bestMatch && isset($results[0])) {
            $bestMatch = $results[0];
        }

        if ($bestMatch) {
            $ocId = $bestMatch['id'];
            $this->fetchGameDetails($game, $ocId);
        }
    }

    private function fetchGameDetails(VideoGame $game, int $openCriticId)
    {
        $detailUrl = "https://api.opencritic.com/api/game/{$openCriticId}";
        $response = Http::get($detailUrl);

        if ($response->successful()) {
            $data = $response->json();
            
            $game->update([
                'opencritic_id' => $openCriticId,
                'opencritic_score' => $data['topCriticScore'] ?? null,
                'opencritic_tier' => $data['tier'] ?? null,
                'opencritic_review_count' => $data['numReviews'] ?? null,
                'opencritic_percent_recommended' => $data['percentRecommended'] ?? null,
                'opencritic_updated_at' => now(),
            ]);

            $this->info("Updated: {$game->name} (Score: {$game->opencritic_score})");
        }
    }
}
