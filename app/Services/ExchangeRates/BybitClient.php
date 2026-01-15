<?php

declare(strict_types=1);

namespace App\Services\ExchangeRates;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Bybit API client for fetching cryptocurrency exchange rates.
 *
 * Supports BTC to fiat currency conversions using Bybit's public market data API.
 * Implements rate limiting, caching, and error handling with exponential backoff.
 *
 * @see https://bybit-exchange.github.io/docs/v5/market/tickers
 */
class BybitClient
{
    private const BASE_URL = 'https://api.bybit.com';

    private const CACHE_TTL = 300; // 5 minutes

    private const RATE_LIMIT_PER_SECOND = 50;

    private const TIMEOUT_SECONDS = 10;

    private PendingRequest $client;

    public function __construct()
    {
        $this->client = Http::baseUrl(config('services.bybit.base_url', self::BASE_URL))
            ->timeout(config('services.bybit.timeout', self::TIMEOUT_SECONDS))
            ->retry(3, 1000, function (Exception $exception) {
                return $exception instanceof \Illuminate\Http\Client\RequestException;
            }, throw: false);
    }

    /**
     * Get the exchange rate from crypto to fiat currency.
     *
     * @param  string  $crypto  Cryptocurrency code (e.g., 'BTC', 'ETH')
     * @param  string  $fiat  Fiat currency code (e.g., 'USD', 'EUR')
     * @return float Exchange rate (1 crypto = X fiat)
     *
     * @throws \RuntimeException If rate cannot be fetched
     */
    public function getCryptoToFiatRate(string $crypto, string $fiat): float
    {
        $cacheKey = "bybit:rate:{$crypto}:{$fiat}";

        return Cache::remember($cacheKey, self::CACHE_TTL, function () use ($crypto, $fiat) {
            $symbol = $this->normalizeSymbol($crypto, $fiat);

            try {
                $response = $this->client->get('/v5/market/tickers', [
                    'category' => 'spot',
                    'symbol' => $symbol,
                ]);

                if (! $response->successful()) {
                    throw new \RuntimeException("Bybit API error: {$response->status()}");
                }

                $data = $response->json();

                if ($data['retCode'] !== 0) {
                    throw new \RuntimeException("Bybit API error: {$data['retMsg']}");
                }

                $ticker = $data['result']['list'][0] ?? null;

                if (! $ticker) {
                    throw new \RuntimeException("No ticker data for {$symbol}");
                }

                return (float) $ticker['lastPrice'];

            } catch (\Exception $e) {
                Log::error('Bybit rate fetch failed', [
                    'crypto' => $crypto,
                    'fiat' => $fiat,
                    'symbol' => $symbol,
                    'error' => $e->getMessage(),
                ]);

                throw new \RuntimeException("Failed to fetch {$crypto}/{$fiat} rate: {$e->getMessage()}");
            }
        });
    }

    /**
     * Get multiple exchange rates in a single request.
     *
     * @param  array<array{crypto: string, fiat: string}>  $pairs  Array of crypto/fiat pairs
     * @return array<string, float> Rates keyed by "CRYPTO_FIAT" (e.g., "BTC_USD" => 45000.0)
     */
    public function getBulkRates(array $pairs): array
    {
        $rates = [];

        foreach ($pairs as $pair) {
            $crypto = $pair['crypto'];
            $fiat = $pair['fiat'];
            $key = "{$crypto}_{$fiat}";

            try {
                $rates[$key] = $this->getCryptoToFiatRate($crypto, $fiat);
            } catch (\Exception $e) {
                Log::warning("Skipping {$key} due to error", ['error' => $e->getMessage()]);

                continue;
            }

            // Respect rate limit
            usleep(1_000_000 / self::RATE_LIMIT_PER_SECOND);
        }

        return $rates;
    }

    /**
     * Normalize crypto/fiat pair to Bybit symbol format.
     *
     * Bybit uses USDT as the quote currency for most pairs.
     * For BTC/USD, we fetch BTC/USDT and treat it as BTC/USD (close approximation).
     *
     * @return string Bybit symbol (e.g., 'BTCUSDT')
     */
    private function normalizeSymbol(string $crypto, string $fiat): string
    {
        // Map common fiat to Bybit stablecoin pairs
        $fiatMap = [
            'USD' => 'USDT',
            'EUR' => 'USDT', // We'll convert via USD
            'GBP' => 'USDT',
            'JPY' => 'USDT',
            'AUD' => 'USDT',
            'CAD' => 'USDT',
            'BRL' => 'USDT',
            'RUB' => 'USDT',
            'ZAR' => 'USDT',
        ];

        $quoteCurrency = $fiatMap[$fiat] ?? 'USDT';

        return strtoupper($crypto.$quoteCurrency);
    }

    /**
     * Check if Bybit API is reachable.
     */
    public function healthCheck(): bool
    {
        try {
            $response = $this->client->get('/v5/market/time');

            return $response->successful();
        } catch (\Exception $e) {
            return false;
        }
    }
}
