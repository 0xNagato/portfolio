<?php

declare(strict_types=1);

namespace App\Services\Normalization;

use Illuminate\Support\Arr;

class RatingNormalizer
{
    /**
     * Extract a rating on a unified 0–5 scale from heterogeneous provider payloads.
     *
     * Normalization rules:
     * - 0–5 inputs are treated as stars and left as-is (clamped to 0–5)
     * - 0–10 inputs are divided by 2
     * - 0–100 inputs are divided by 20
     */
    public function extractNormalizedRating(array $record): ?float
    {
        $candidate = $this->firstNonNull($record, [
            // Common fields
            'rating',
            'user_ratings',

            // 0–100 style
            'aggregated_rating',
            'total_rating',

            // String-ish star ratings
            'product_star_rating',
        ]);

        return $this->normalizeCandidate($candidate);
    }

    public function extractRatingCount(array $record): ?int
    {
        $candidate = $this->firstNonNull($record, [
            'rating_count',
            'total_rating_count',
            'aggregated_rating_count',
            'user_rating_count',
        ]);

        if (is_numeric($candidate)) {
            $value = (int) $candidate;

            return $value >= 0 ? $value : null;
        }

        return null;
    }

    private function normalizeCandidate(mixed $candidate): ?float
    {
        if ($candidate === null || $candidate === '') {
            return null;
        }

        // Numeric input.
        if (is_numeric($candidate)) {
            return $this->normalizeNumeric((float) $candidate);
        }

        // Strings like "4.2 stars" or "4/5 stars".
        if (is_string($candidate)) {
            if (preg_match('/(\d+(?:\.\d+)?)\s*\/?\s*(\d+(?:\.\d+)?)?/', $candidate, $m) === 1) {
                $numerator = (float) $m[1];

                if (isset($m[2]) && is_numeric($m[2]) && (float) $m[2] > 0.0) {
                    $denominator = (float) $m[2];

                    // Convert X/Y -> 0–5.
                    $value = ($numerator / $denominator) * 5.0;

                    return $this->clampToFive($value);
                }

                return $this->normalizeNumeric($numerator);
            }
        }

        return null;
    }

    private function normalizeNumeric(float $value): ?float
    {
        if (! is_finite($value)) {
            return null;
        }

        // Heuristics:
        // - 0–5: already star scale
        // - 0–10: divide by 2
        // - >10: assume 0–100-ish and divide by 20
        if ($value <= 5.0) {
            return $this->clampToFive($value);
        }

        if ($value <= 10.0) {
            return $this->clampToFive($value / 2.0);
        }

        return $this->clampToFive($value / 20.0);
    }

    private function clampToFive(float $value): float
    {
        return max(0.0, min(5.0, $value));
    }

    private function firstNonNull(array $record, array $keys): mixed
    {
        foreach ($keys as $key) {
            $value = Arr::get($record, $key);

            if ($value === null) {
                continue;
            }

            // CSV payloads commonly represent missing values as empty strings.
            // Treat those as "missing" so later aliases (e.g. total_rating) can be used.
            if (is_string($value) && trim($value) === '') {
                continue;
            }

            if ($value !== null) {
                return $value;
            }
        }

        return null;
    }
}
