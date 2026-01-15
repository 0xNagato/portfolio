<?php

declare(strict_types=1);

namespace App\Services\Normalization;

use Illuminate\Support\Str;

class PlatformNormalizer
{
    private const SIMILARITY_THRESHOLD = 0.80;

    /**
     * @return array<string>
     */
    public function normalizeMany(array $platforms): array
    {
        $out = [];

        foreach ($platforms as $platform) {
            if (! is_string($platform)) {
                continue;
            }

            $platform = trim($platform);

            if ($platform === '') {
                continue;
            }

            // Common composite values (e.g. "Xbox Series X|S").
            // Beware shorthand like "PlayStation 4/5" (second part becomes just "5").
            $parts = preg_split('/\s*[|\/]\s*/', $platform) ?: [$platform];

            if (count($parts) === 2) {
                $left = trim($parts[0]);
                $right = trim($parts[1]);

                // Expand suffix shorthands like "Xbox Series X|S" -> "Xbox Series X", "Xbox Series S".
                if ($right !== '' && strlen($right) <= 2) {
                    if (preg_match('/^(.*\bSeries)\s+X$/i', $left, $m) === 1 && preg_match('/^s$/i', $right) === 1) {
                        $parts = [trim($m[0]), trim($m[1]).' S'];
                    }
                }

                // Expand digit shorthands like "PlayStation 4/5" -> "PlayStation 4", "PlayStation 5".
                if (preg_match('/^\d+$/', $right) === 1 && preg_match('/^(.*?)(\d+)$/', $left, $m) === 1) {
                    $parts = [$left, $m[1].$right];
                }
            }

            foreach ($parts as $part) {
                $normalized = $this->normalizeOne($part);

                if ($normalized === '') {
                    continue;
                }

                if (! in_array($normalized, $out, true)) {
                    $out[] = $normalized;
                }
            }
        }

        return $out;
    }

    public function normalizeOne(string $platform): string
    {
        $raw = trim($platform);

        if ($raw === '') {
            return '';
        }

        $raw = $this->stripRegionalPrefixes($raw);

        // Fast paths for common buckets.
        if (preg_match('/\b(pc|windows|steam|epic)\b/i', $raw) === 1) {
            return 'PC';
        }

        $canonical = $this->bestCanonicalMatch($raw);

        return $canonical ?? $raw;
    }

    private function stripRegionalPrefixes(string $value): string
    {
        // Ignore common region prefixes like JPY-, PAL-, NTSC- (and similar)
        // when comparing platforms.
        return (string) preg_replace('/^(jpy|pal|ntsc)\s*[-_:]+\s*/i', '', trim($value));
    }

    private function bestCanonicalMatch(string $raw): ?string
    {
        $rawKey = $this->normalizeForCompare($raw);
        $rawGeneration = $this->generationToken($raw);

        if ($rawKey === '') {
            return null;
        }

        $best = null;
        $bestScore = 0.0;

        foreach ($this->canonicalPlatforms() as $canonical => $aliases) {
            $canonicalGeneration = $this->generationToken($canonical);

            if ($rawGeneration !== null && $canonicalGeneration !== null && $rawGeneration !== $canonicalGeneration) {
                continue;
            }

            // Compare against canonical + aliases and take the best similarity.
            $candidates = array_merge([$canonical], $aliases);

            foreach ($candidates as $candidate) {
                $candidateKey = $this->normalizeForCompare($candidate);

                if ($candidateKey === '') {
                    continue;
                }

                $score = $this->jaroWinkler($rawKey, $candidateKey);

                if ($score > $bestScore) {
                    $bestScore = $score;
                    $best = $canonical;
                }
            }
        }

        if ($best !== null && $bestScore >= self::SIMILARITY_THRESHOLD) {
            return $best;
        }

        return null;
    }

    /**
     * @return array<string, array<int, string>>
     */
    private function canonicalPlatforms(): array
    {
        return [
            'PlayStation 5' => [
                'PS5',
                'Sony PlayStation 5',
            ],
            'PlayStation 4' => [
                'PS4',
                'Sony PlayStation 4',
            ],
            'PlayStation 3' => [
                'PS3',
                'Sony PlayStation 3',
            ],
            'Xbox Series X' => [
                'Xbox Series X|S',
                'XSX',
                'Series X',
            ],
            'Xbox Series S' => [
                'XSS',
                'Series S',
            ],
            'Xbox One' => [
                'Xbox 1',
                'XBONE',
            ],
            'Nintendo Switch' => [
                'Switch',
            ],
            'PC' => [
                'Windows',
            ],
        ];
    }

    private function normalizeForCompare(string $value): string
    {
        // Ignore whitespace/punctuation and case.
        $normalized = Str::of($value)
            ->lower()
            ->replaceMatches('/[^a-z0-9]+/i', '')
            ->toString();

        return $normalized;
    }

    private function generationToken(string $value): ?string
    {
        $v = Str::of($value)->lower()->toString();

        // Prefer digits when present.
        if (preg_match('/\d+/', $v, $m) === 1) {
            return $m[0];
        }

        // Handle common spelled-out generations.
        $map = [
            'one' => '1',
            'two' => '2',
            'three' => '3',
            'four' => '4',
            'five' => '5',
        ];

        foreach ($map as $word => $digit) {
            if (preg_match('/\b'.$word.'\b/', $v) === 1) {
                return $digit;
            }
        }

        return null;
    }

    /**
     * Jaro-Winkler similarity for short strings and typos.
     * Returns a value in [0, 1].
     */
    private function jaroWinkler(string $s1, string $s2): float
    {
        if ($s1 === $s2) {
            return 1.0;
        }

        $len1 = strlen($s1);
        $len2 = strlen($s2);

        if ($len1 === 0 || $len2 === 0) {
            return 0.0;
        }

        $matchDistance = (int) (max($len1, $len2) / 2) - 1;
        $matchDistance = max(0, $matchDistance);

        $s1Matches = array_fill(0, $len1, false);
        $s2Matches = array_fill(0, $len2, false);

        $matches = 0;
        for ($i = 0; $i < $len1; $i++) {
            $start = max(0, $i - $matchDistance);
            $end = min($i + $matchDistance + 1, $len2);

            for ($j = $start; $j < $end; $j++) {
                if ($s2Matches[$j]) {
                    continue;
                }

                if ($s1[$i] !== $s2[$j]) {
                    continue;
                }

                $s1Matches[$i] = true;
                $s2Matches[$j] = true;
                $matches++;
                break;
            }
        }

        if ($matches === 0) {
            return 0.0;
        }

        $t = 0;
        $k = 0;

        for ($i = 0; $i < $len1; $i++) {
            if (! $s1Matches[$i]) {
                continue;
            }

            while (! $s2Matches[$k]) {
                $k++;
            }

            if ($s1[$i] !== $s2[$k]) {
                $t++;
            }

            $k++;
        }

        $transpositions = $t / 2.0;

        $jaro = (
            ($matches / $len1) +
            ($matches / $len2) +
            (($matches - $transpositions) / $matches)
        ) / 3.0;

        // Winkler boost for common prefix.
        $prefix = 0;
        $maxPrefix = 4;
        $bound = min([$maxPrefix, $len1, $len2]);

        for ($i = 0; $i < $bound; $i++) {
            if ($s1[$i] !== $s2[$i]) {
                break;
            }
            $prefix++;
        }

        $scalingFactor = 0.1;

        return $jaro + ($prefix * $scalingFactor * (1.0 - $jaro));
    }
}
