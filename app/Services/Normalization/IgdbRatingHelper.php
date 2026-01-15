<?php

declare(strict_types=1);

namespace App\Services\Normalization;

use Illuminate\Support\Arr;

/**
 * IGDB-specific rating helper that preserves the native 0-100 percentage scale.
 *
 * Unlike RatingNormalizer (which converts to 0-5 stars), this helper keeps
 * IGDB ratings on their original 0-100 scale with 8 decimal places precision.
 */
class IgdbRatingHelper
{
    /**
     * Extract IGDB rating as a 0-100 percentage with 8 decimal places precision.
     *
     * Priority order: aggregated_rating > total_rating > rating
     */
    public function extractPercentage(array $record): ?float
    {
        $candidate = $this->firstNonNull($record, [
            'aggregated_rating',
            'total_rating',
            'rating',
        ]);

        if ($candidate === null || $candidate === '') {
            return null;
        }

        if (! is_numeric($candidate)) {
            return null;
        }

        $value = (float) $candidate;

        if (! is_finite($value)) {
            return null;
        }

        // Clamp to 0-100 and round to 8 decimal places
        return round(max(0.0, min(100.0, $value)), 8);
    }

    /**
     * Extract rating count from IGDB record.
     */
    public function extractRatingCount(array $record): ?int
    {
        $candidate = $this->firstNonNull($record, [
            'aggregated_rating_count',
            'total_rating_count',
            'rating_count',
        ]);

        if (is_numeric($candidate)) {
            $value = (int) $candidate;

            return $value >= 0 ? $value : null;
        }

        return null;
    }

    private function firstNonNull(array $record, array $keys): mixed
    {
        foreach ($keys as $key) {
            $value = Arr::get($record, $key);

            if ($value === null) {
                continue;
            }

            // CSV payloads commonly represent missing values as empty strings.
            if (is_string($value) && trim($value) === '') {
                continue;
            }

            return $value;
        }

        return null;
    }
}
