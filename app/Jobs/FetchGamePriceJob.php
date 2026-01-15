<?php

namespace App\Jobs;

use App\Models\Retailer;
use App\Models\VideoGame;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class FetchGamePriceJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(
        public int $videoGameId,
        public int $retailerId,
        public string $url
    ) {}

    public function handle(SteamStoreService $steamService): void
    {
        $retailer = Retailer::find($this->retailerId);
        if (!$retailer) return;

        $priceData = null;

        if ($retailer->slug === 'steam') {
            if (preg_match('/app\/(\d+)/', $this->url, $matches)) {
                $appId = $matches[1];
                $priceData = $steamService->getPrice($appId);
            }
        }

        if ($priceData) {
            DB::table('video_game_prices')->updateOrInsert(
                [
                    'video_game_id' => $this->videoGameId,
                    'retailer' => $retailer->name,
                ],
                [
                    'url' => $this->url,
                    'currency' => $priceData['currency'],
                    'amount_minor' => $priceData['amount_minor'],
                    'recorded_at' => now(),
                    'updated_at' => now(),
                ]
            );
        }
    }
}
