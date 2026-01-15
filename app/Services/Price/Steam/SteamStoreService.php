<?php

namespace App\Services\Price\Steam;

use App\Models\Retailer;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SteamStoreService
{
    /**
     * Fetch price for a given Steam App ID.
     */
    public function getPrice(string $appId): ?array
    {
        // cc=us enforces USD prices for consistency in this MVP
        $apiUrl = "https://store.steampowered.com/api/appdetails?appids={$appId}&cc=us&filters=price_overview";
        
        try {
            $response = Http::get($apiUrl);
            
            if ($response->failed()) {
                Log::error("SteamStoreService: API request failed for App ID {$appId}");
                return null;
            }

            $data = $response->json();

            // Structure: {"12345": {"success": true, "data": {"price_overview": {"final": 1999, "currency": "USD"}}}}
            if (empty($data[$appId]['success'])) {
                return null;
            }

            $gameData = $data[$appId]['data'] ?? [];
            $priceOverview = $gameData['price_overview'] ?? null;

            if (!$priceOverview) {
                 // Check if it's free
                 if (!empty($gameData['is_free'])) {
                     return [
                         'amount_minor' => 0,
                         'currency' => 'USD'
                     ];
                 }
                 return null;
            }

            return [
                'amount_minor' => (int) $priceOverview['final'], // Steam returns value in cents
                'currency' => $priceOverview['currency'],
            ];

        } catch (\Exception $e) {
            Log::error("SteamStoreService: Exception for App ID {$appId}: " . $e->getMessage());
            return null;
        }
    }
}
