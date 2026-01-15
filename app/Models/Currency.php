<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class Currency extends Model
{
    protected $fillable = [
        'code',
        'name',
        'symbol',
        'decimals',
        'is_crypto',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'decimals' => 'integer',
            'is_crypto' => 'boolean',
            'metadata' => 'array',
        ];
    }

    /**
     * Get the primary key for the model.
     */
    public function getKeyName(): string
    {
        return 'code';
    }

    /**
     * Get the "key type" for the model.
     */
    public function getKeyType(): string
    {
        return 'string';
    }

    /**
     * Indicates if the IDs are auto-incrementing.
     */
    public function getIncrementing(): bool
    {
        return false;
    }

    /**
     * Exchange rates where this is the base currency.
     */
    public function baseExchangeRates(): HasMany
    {
        return $this->hasMany(ExchangeRate::class, 'base_currency', 'code');
    }

    /**
     * Exchange rates where this is the quote currency.
     */
    public function quoteExchangeRates(): HasMany
    {
        return $this->hasMany(ExchangeRate::class, 'quote_currency', 'code');
    }

    /**
     * Countries that use this currency.
     */
    public function countries(): HasMany
    {
        return $this->hasMany(Country::class);
    }

    /**
     * Format an amount in this currency's minor units to display value.
     *
     * @param  int  $amountMinor  Amount in minor units (cents, satoshis, etc.)
     * @return float Formatted amount
     */
    public function formatAmount(int $amountMinor): float
    {
        return $amountMinor / pow(10, $this->decimals);
    }

    /**
     * Convert a display amount to minor units.
     *
     * @param  float  $amount  Display amount
     * @return int Amount in minor units
     */
    public function toMinorUnits(float $amount): int
    {
        return (int) round($amount * pow(10, $this->decimals));
    }

    /**
     * Check if this is a cryptocurrency.
     */
    public function isCrypto(): bool
    {
        return $this->is_crypto;
    }

    /**
     * Check if this is a fiat currency.
     */
    public function isFiat(): bool
    {
        return ! $this->is_crypto;
    }
}
