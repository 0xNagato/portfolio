<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Models\Currency;
use App\Models\ExchangeRate;
use App\Services\ExchangeRates\BybitClient;
use App\Services\ExchangeRates\ForexClient;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

/**
 * Fetch and persist exchange rates from Bybit (crypto) and ExchangeRate-API (forex).
 *
 * Scheduled to run hourly to keep exchange rates fresh.
 */
class FetchExchangeRatesJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    public int $timeout = 120;

    public int $backoff = 60;

    public function __construct()
    {
        $this->onQueue('fetch');
    }

    public function handle(BybitClient $bybitClient, ForexClient $forexClient): void
    {
        $startTime = microtime(true);
        $fetchedAt = now();

        Log::info('Starting exchange rate fetch', ['timestamp' => $fetchedAt]);

        try {
            // Get all currencies from database
            $currencies = Currency::all()->keyBy('code');
            $crypto = $currencies->where('is_crypto', true)->pluck('code')->toArray();
            $fiat = $currencies->where('is_crypto', false)->pluck('code')->toArray();

            $totalRates = 0;
            $failedRates = 0;

            // 1. Fetch crypto-to-fiat rates (Bybit)
            if (! empty($crypto)) {
                Log::info('Fetching crypto rates', ['crypto' => $crypto, 'fiat_count' => count($fiat)]);

                foreach ($crypto as $cryptoCurrency) {
                    foreach ($fiat as $fiatCurrency) {
                        try {
                            $rate = $bybitClient->getCryptoToFiatRate($cryptoCurrency, $fiatCurrency);

                            ExchangeRate::updateOrCreate(
                                [
                                    'base_currency' => $cryptoCurrency,
                                    'quote_currency' => $fiatCurrency,
                                    'fetched_at' => $fetchedAt->toDateTimeString(),
                                ],
                                [
                                    'rate' => $rate,
                                    'provider' => 'bybit',
                                ]
                            );

                            $totalRates++;

                        } catch (\Exception $e) {
                            $failedRates++;
                            Log::warning("Failed to fetch {$cryptoCurrency}/{$fiatCurrency}", [
                                'error' => $e->getMessage(),
                            ]);
                        }
                    }
                }
            }

            // 2. Fetch fiat-to-fiat rates (ExchangeRate-API)
            Log::info('Fetching forex rates', ['base' => 'USD', 'quotes' => $fiat]);

            // Fetch USD as base to all other fiat currencies
            foreach ($fiat as $quoteCurrency) {
                if ($quoteCurrency === 'USD') {
                    continue;
                }

                try {
                    $rate = $forexClient->getFiatToFiatRate('USD', $quoteCurrency);

                    ExchangeRate::updateOrCreate(
                        [
                            'base_currency' => 'USD',
                            'quote_currency' => $quoteCurrency,
                            'fetched_at' => $fetchedAt->toDateTimeString(),
                        ],
                        [
                            'rate' => $rate,
                            'provider' => 'exchangerate-api',
                        ]
                    );

                    $totalRates++;

                } catch (\Exception $e) {
                    $failedRates++;
                    Log::warning("Failed to fetch USD/{$quoteCurrency}", [
                        'error' => $e->getMessage(),
                    ]);
                }
            }

            // 3. Fetch key cross-rates (EUR/GBP, EUR/JPY)
            $crossRates = [
                ['base' => 'EUR', 'quote' => 'GBP'],
                ['base' => 'EUR', 'quote' => 'JPY'],
            ];

            foreach ($crossRates as $pair) {
                try {
                    $rate = $forexClient->getFiatToFiatRate($pair['base'], $pair['quote']);

                    ExchangeRate::updateOrCreate(
                        [
                            'base_currency' => $pair['base'],
                            'quote_currency' => $pair['quote'],
                            'fetched_at' => $fetchedAt->toDateTimeString(),
                        ],
                        [
                            'rate' => $rate,
                            'provider' => 'exchangerate-api',
                        ]
                    );

                    $totalRates++;

                } catch (\Exception $e) {
                    $failedRates++;
                    Log::warning("Failed to fetch {$pair['base']}/{$pair['quote']}", [
                        'error' => $e->getMessage(),
                    ]);
                }
            }

            // 4. Clean up stale rates (older than 7 days)
            $deleted = ExchangeRate::where('fetched_at', '<', now()->subDays(7))->delete();

            $duration = round(microtime(true) - $startTime, 2);

            Log::info('Exchange rate fetch completed', [
                'duration_seconds' => $duration,
                'total_rates_fetched' => $totalRates,
                'failed_rates' => $failedRates,
                'stale_rates_deleted' => $deleted,
                'success_rate' => $totalRates > 0 ? round(($totalRates / ($totalRates + $failedRates)) * 100, 2).'%' : '0%',
            ]);

            // Alert if too many failures
            if ($failedRates > 0 && ($failedRates / ($totalRates + $failedRates)) > 0.2) {
                Log::error('High exchange rate fetch failure rate', [
                    'failed' => $failedRates,
                    'total' => $totalRates + $failedRates,
                    'failure_rate' => round(($failedRates / ($totalRates + $failedRates)) * 100, 2).'%',
                ]);
            }

        } catch (\Exception $e) {
            Log::error('Exchange rate fetch job failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            throw $e;
        }
    }
}
