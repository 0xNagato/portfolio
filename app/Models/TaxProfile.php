<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class TaxProfile extends Model
{
    protected $fillable = [
        'region_code',
        'vat_rate',
        'effective_from',
        'notes',
    ];

    protected function casts(): array
    {
        return [
            'vat_rate' => 'decimal:4',
            'effective_from' => 'datetime',
        ];
    }

    /**
     * Get the tax profile for a specific region.
     *
     * @param  string  $regionCode  ISO 3166-1 alpha-2 country code
     */
    public static function forRegion(string $regionCode): ?self
    {
        return self::where('region_code', $regionCode)
            ->orderByDesc('effective_from')
            ->first();
    }

    /**
     * Calculate tax amount for a price.
     *
     * @param  float  $price  Pre-tax price
     * @return float Tax amount
     */
    public function calculateTax(float $price): float
    {
        return $price * (float) $this->vat_rate;
    }

    /**
     * Calculate tax-inclusive price.
     *
     * @param  float  $price  Pre-tax price
     * @return float Price including tax
     */
    public function applyTax(float $price): float
    {
        return $price * (1 + (float) $this->vat_rate);
    }

    /**
     * Calculate pre-tax price from tax-inclusive price.
     *
     * @param  float  $priceWithTax  Tax-inclusive price
     * @return float Pre-tax price
     */
    public function removeTax(float $priceWithTax): float
    {
        return $priceWithTax / (1 + (float) $this->vat_rate);
    }

    /**
     * Get the tax rate as a percentage.
     *
     * @return float Tax rate percentage (e.g., 20.0 for 20%)
     */
    public function taxRatePercentage(): float
    {
        return (float) $this->vat_rate * 100;
    }

    /**
     * Check if this region has any tax.
     */
    public function hasTax(): bool
    {
        return (float) $this->vat_rate > 0;
    }
}
