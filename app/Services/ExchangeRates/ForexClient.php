<?php

declare(strict_types=1);

namespace App\Services\ExchangeRates;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Exchange rate client for fiat-to-fiat currency conversions.
 *
 * Uses ExchangeRate-API as the primary source (free tier: 1,500 requests/month).
 * Falls back to manual forex data if API is unavailable.
 *
 * @see https://www.exchangerate-api.com/docs/free
 */
class ForexClient
{
    private const BASE_URL = 'https://api.exchangerate-api.com/v4';

    private const CACHE_TTL = 900; // 15 minutes (forex moves slower than crypto)

    private const TIMEOUT_SECONDS = 10;

    private PendingRequest $client;

    public function __construct()
    {
        $apiUrl = config('services.forex.base_url', self::BASE_URL);

        $this->client = Http::baseUrl($apiUrl)
            ->timeout(config('services.forex.timeout', self::TIMEOUT_SECONDS))
            ->retry(3, 1000, throw: false);
    }

    /**
     * Get the exchange rate from base fiat to quote fiat.
     *
     * @param  string  $base  Base currency code (e.g., 'USD')
     * @param  string  $quote  Quote currency code (e.g., 'EUR')
     * @return float Exchange rate (1 base = X quote)
     *
     * @throws \RuntimeException If rate cannot be fetched
     */
    public function getFiatToFiatRate(string $base, string $quote): float
    {
        if ($base === $quote) {
            return 1.0;
        }

        $cacheKey = "forex:rate:{$base}:{$quote}";

        return Cache::remember($cacheKey, self::CACHE_TTL, function () use ($base, $quote) {
            try {
                $response = $this->client->get("/latest/{$base}");

                if (! $response->successful()) {
                    throw new \RuntimeException("Forex API error: {$response->status()}");
                }

                $data = $response->json();

                if (! isset($data['rates'][$quote])) {
                    throw new \RuntimeException("No rate found for {$base}/{$quote}");
                }

                return (float) $data['rates'][$quote];

            } catch (\Exception $e) {
                Log::error('Forex rate fetch failed', [
                    'base' => $base,
                    'quote' => $quote,
                    'error' => $e->getMessage(),
                ]);

                // Fallback to static approximations if API fails
                return $this->getFallbackRate($base, $quote);
            }
        });
    }

    /**
     * Get multiple forex rates in a single batch.
     *
     * @param  array<array{base: string, quote: string}>  $pairs  Array of base/quote pairs
     * @return array<string, float> Rates keyed by "BASE_QUOTE" (e.g., "USD_EUR" => 0.92)
     */
    public function getBulkRates(array $pairs): array
    {
        $rates = [];

        // Group by base currency to minimize API calls
        $byBase = [];
        foreach ($pairs as $pair) {
            $byBase[$pair['base']][] = $pair['quote'];
        }

        foreach ($byBase as $base => $quotes) {
            try {
                $response = $this->client->get("/latest/{$base}");

                if (! $response->successful()) {
                    continue;
                }

                $data = $response->json();

                foreach ($quotes as $quote) {
                    if (isset($data['rates'][$quote])) {
                        $key = "{$base}_{$quote}";
                        $rates[$key] = (float) $data['rates'][$quote];
                    }
                }

            } catch (\Exception $e) {
                Log::warning("Bulk forex fetch failed for {$base}", ['error' => $e->getMessage()]);

                continue;
            }
        }

        return $rates;
    }

    /**
     * Fallback rates when API is unavailable (approximate values).
     *
     * @throws \RuntimeException If no fallback available
     */
    private function getFallbackRate(string $base, string $quote): float
    {
        // Approximate rates as of Jan 2026 (update periodically)
        $approximations = [
            'USD_EUR' => 0.92,
            'USD_GBP' => 0.79,
            'USD_JPY' => 149.50,
            'USD_AUD' => 1.52,
            'USD_CAD' => 1.35,
            'USD_BRL' => 5.85,
            'USD_RUB' => 92.50,
            'USD_ZAR' => 18.75,
            'EUR_GBP' => 0.86,
            'EUR_JPY' => 162.50,
        ];

        $key = "{$base}_{$quote}";

        if (isset($approximations[$key])) {
            Log::warning("Using fallback forex rate for {$key}");

            return $approximations[$key];
        }

        // Try inverse rate
        $inverseKey = "{$quote}_{$base}";
        if (isset($approximations[$inverseKey])) {
            Log::warning("Using inverse fallback forex rate for {$key}");

            return 1.0 / $approximations[$inverseKey];
        }

        throw new \RuntimeException("No fallback rate available for {$base}/{$quote}");
    }

    /**
     * Check if forex API is reachable.
     */
    public function healthCheck(): bool
    {
        try {
            $response = $this->client->get('/latest/USD');

            return $response->successful();
        } catch (\Exception $e) {
            return false;
        }
    }
}
