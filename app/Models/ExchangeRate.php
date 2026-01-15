<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class ExchangeRate extends Model
{
    protected $fillable = [
        'base_currency',
        'quote_currency',
        'rate',
        'fetched_at',
        'provider',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'rate' => 'decimal:8',
            'fetched_at' => 'datetime',
            'metadata' => 'array',
        ];
    }

    /**
     * Get the base currency.
     */
    public function baseCurrency(): BelongsTo
    {
        return $this->belongsTo(Currency::class, 'base_currency', 'code');
    }

    /**
     * Get the quote currency.
     */
    public function quoteCurrency(): BelongsTo
    {
        return $this->belongsTo(Currency::class, 'quote_currency', 'code');
    }

    /**
     * Get the latest exchange rate for a currency pair.
     *
     * @param  string  $baseCurrency  Base currency code (e.g., 'USD')
     * @param  string  $quoteCurrency  Quote currency code (e.g., 'BTC')
     */
    public static function getLatestRate(string $baseCurrency, string $quoteCurrency): ?self
    {
        return self::where('base_currency', $baseCurrency)
            ->where('quote_currency', $quoteCurrency)
            ->orderByDesc('fetched_at')
            ->first();
    }

    /**
     * Get exchange rate at a specific point in time.
     *
     * @param  string  $baseCurrency  Base currency code
     * @param  string  $quoteCurrency  Quote currency code
     * @param  \DateTimeInterface  $dateTime  Point in time
     */
    public static function getRateAt(string $baseCurrency, string $quoteCurrency, \DateTimeInterface $dateTime): ?self
    {
        return self::where('base_currency', $baseCurrency)
            ->where('quote_currency', $quoteCurrency)
            ->where('fetched_at', '<=', $dateTime)
            ->orderByDesc('fetched_at')
            ->first();
    }

    /**
     * Convert an amount from base to quote currency using this rate.
     *
     * @param  float  $amount  Amount in base currency
     * @return float Amount in quote currency
     */
    public function convert(float $amount): float
    {
        return $amount * (float) $this->rate;
    }

    /**
     * Get the inverse rate (quote → base).
     */
    public function inverseRate(): float
    {
        return 1 / (float) $this->rate;
    }

    /**
     * Check if this rate is stale (older than 1 hour).
     */
    public function isStale(): bool
    {
        return $this->fetched_at->diffInHours(now()) > 1;
    }
}
