<?php

namespace App\Jobs;

use App\Models\Retailer;
use App\Models\VideoGame;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class ExtractRetailerPricesJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(public int $chunkSize = 100)
    {
    }

    public function handle(): void
    {
        $retailers = Retailer::where('is_active', true)->get();

        VideoGame::query()
            ->whereNotNull('attributes')
            ->chunk($this->chunkSize, function ($games) use ($retailers) {
                foreach ($games as $game) {
                    $this->processGame($game, $retailers);
                }
            });
    }

    private function processGame(VideoGame $game, $retailers)
    {
        $attributes = $game->attributes;
        if (is_string($attributes)) {
            $attributes = json_decode($attributes, true);
        }

        // IGDB often puts websites in 'websites' or inside 'attributes'
        $websites = $attributes['websites'] ?? $attributes['original_metadata']['websites'] ?? [];
        if (empty($websites)) {
            return;
        }

        foreach ($websites as $site) {
            $url = $site['url'] ?? null;
            if (!$url) continue;

            foreach ($retailers as $retailer) {
                if (Str::contains($url, $retailer->domain_matcher)) {
                    // Dispatch job to fetch actual price
                    FetchGamePriceJob::dispatch($game->id, $retailer->id, $url);
                    break; 
                }
            }
        }
    }
}
