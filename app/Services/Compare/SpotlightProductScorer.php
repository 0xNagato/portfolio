<?php

namespace App\Services\Compare;

use App\DataTransferObjects\SpotlightScoreData;
use App\Models\Platform;
use App\Models\PriceSeriesAggregate;
use App\Models\Product;
use App\DataTransferObjects\Media\ProductMediaSet;
use App\Models\SkuRegion;
use App\Support\Media\ProductMediaResolver;
use Illuminate\Support\Collection;

class SpotlightProductScorer
{
    /**
     * Default weights are defined in `config/compare.php` under `spotlight.weights`.
     *
     * These are applied after candidate selection (toplists/external IDs) and before
     * the final blended sort used on landing + compare.
     *
     * @return array{media: float, retailers: float, price: float, platforms: float}
     */
    protected function weights(): array
    {
        $raw = (array) config('compare.spotlight.weights', []);

        $weights = [
            'media' => (float) ($raw['media'] ?? 0.30),
            'retailers' => (float) ($raw['retailers'] ?? 0.30),
            'price' => (float) ($raw['price'] ?? 0.30),
            'platforms' => (float) ($raw['platforms'] ?? 0.10),
        ];

        $sum = array_sum($weights);
        if ($sum <= 0.0) {
            return [
                'media' => 0.30,
                'retailers' => 0.30,
                'price' => 0.30,
                'platforms' => 0.10,
            ];
        }

        // Normalize so callers can safely treat these as proportions.
        foreach ($weights as $key => $value) {
            $weights[$key] = max(0.0, (float) $value) / $sum;
        }

        return $weights;
    }

    public function score(Product $product, ?Collection $aggregates = null): SpotlightScoreData
    {
        $weights = $this->weights();

        $mediaSet = $this->resolveMedia($product);
        $skuRegions = $this->resolveSkuRegions($product);
        $platforms = $this->resolvePlatforms($product);

        $mediaMetric = $this->scoreMedia($mediaSet, $weights['media']);
        $retailerMetric = $this->scoreRetailers($skuRegions, $weights['retailers']);
        $priceMetric = $this->scorePriceCoverage($skuRegions, $weights['price'], $aggregates);
        $platformMetric = $this->scorePlatforms($product, $platforms, $weights['platforms']);

        $metrics = collect([
            $mediaMetric,
            $retailerMetric,
            $priceMetric,
            $platformMetric,
        ]);

        $normalizedTotal = $metrics->sum(function (array $metric): float {
            return $metric['weight'] * $metric['score'];
        });

        $totalScore = round($normalizedTotal * 100, 1);

        $context = [
            'media_count' => $mediaMetric['context']['media_count'] ?? 0,
            'media_has_trailer' => $mediaMetric['context']['has_trailer'] ?? false,
            'retailer_count' => $retailerMetric['context']['retailer_count'] ?? 0,
            'retailer_names' => $retailerMetric['context']['retailer_names'] ?? [],
            'region_count' => $priceMetric['context']['region_count'] ?? 0,
            'currency_count' => $priceMetric['context']['currency_count'] ?? 0,
            'price_entries' => $priceMetric['context']['price_entries'] ?? 0,
            'platform_count' => $platformMetric['context']['platform_count'] ?? 0,
            'platform_labels' => $platformMetric['context']['platform_labels'] ?? [],
        ];

        $grade = $this->gradeFor($normalizedTotal);
        $verdict = $this->verdictFor($normalizedTotal);

        return new SpotlightScoreData(
            productId: $product->id,
            totalScore: $totalScore,
            normalizedTotal: round($normalizedTotal, 4),
            metrics: $metrics
                ->map(fn (array $metric): array => [
                    'key' => $metric['key'],
                    'label' => $metric['label'],
                    'weight' => $metric['weight'],
                    'weight_percentage' => (int) round($metric['weight'] * 100),
                    'score' => round($metric['score'] * 100, 1),
                    'normalized' => round($metric['score'], 4),
                    'summary' => $metric['summary'],
                    'context' => $metric['context'],
                ])
                ->values()
                ->all(),
            context: $context,
            grade: $grade,
            verdict: $verdict,
        );
    }

    protected function resolveMedia(Product $product): ProductMediaSet
    {
        return ProductMediaResolver::resolve($product);
    }

    /**
     * @return Collection<int, SkuRegion>
     */
    protected function resolveSkuRegions(Product $product): Collection
    {
        $regions = $product->getRelationValue('skuRegions');

        return $regions instanceof Collection
            ? $regions
            : collect();
    }

    /**
     * @return Collection<int, Platform>
     */
    protected function resolvePlatforms(Product $product): Collection
    {
        $platforms = $product->getRelationValue('platforms');

        return $platforms instanceof Collection
            ? $platforms
            : collect();
    }

    /**
     * @return array{
     *   key:string,
     *   label:string,
     *   weight:float,
     *   score:float,
     *   summary:string,
     *   context: array<string, mixed>
     * }
     */
    protected function scoreMedia(ProductMediaSet $mediaSet, float $weight): array
    {
        if ($mediaSet->totalAssets() === 0) {
            return [
                'key' => 'media',
                'label' => 'Media depth',
                'weight' => $weight,
                'score' => 0.0,
                'summary' => 'Awaiting gallery assets',
                'context' => [
                    'media_count' => 0,
                    'quality_average' => 0.0,
                    'has_trailer' => false,
                ],
            ];
        }

        $images = $mediaSet->gallery();
        $videos = $mediaSet->trailers();

        $qualityAverage = $images
            ->map(fn ($item) => is_numeric($item->quality) ? (float) $item->quality : ($item->isPrimary ? 0.75 : 0.6))
            ->average();

        $qualityAverage = $qualityAverage !== null ? (float) $qualityAverage : 0.6;
        $count = $mediaSet->totalAssets();
        $countNormalized = min($count / 10, 1);
        $hasTrailer = $videos->isNotEmpty();

        $normalized = ($qualityAverage * 0.6) + ($countNormalized * 0.4);
        $normalized = min(max($normalized, 0), 1);

        $captionParts = [sprintf('%d asset%s', $count, $count === 1 ? '' : 's')];
        if ($hasTrailer) {
            $captionParts[] = 'trailer ready';
        }

        return [
            'key' => 'media',
            'label' => 'Media depth',
            'weight' => $weight,
            'score' => $normalized,
            'summary' => implode(' · ', $captionParts),
            'context' => [
                'media_count' => $count,
                'quality_average' => round($qualityAverage, 3),
                'has_trailer' => $hasTrailer,
            ],
        ];
    }

    /**
     * @param  Collection<int, SkuRegion>  $skuRegions
     * @return array<string, mixed>
     */
    protected function scoreRetailers(Collection $skuRegions, float $weight): array
    {
        if ($skuRegions->isEmpty()) {
            return [
                'key' => 'retailers',
                'label' => 'Retailer footprint',
                'weight' => $weight,
                'score' => 0.0,
                'summary' => 'No active retailers indexed yet',
                'context' => [
                    'retailer_count' => 0,
                    'retailer_names' => [],
                ],
            ];
        }

        $active = $skuRegions
            ->filter(fn (SkuRegion $region): bool => (bool) ($region->is_active ?? true));

        $retailerNames = $active
            ->pluck('retailer')
            ->filter()
            ->map(static fn ($value) => trim((string) $value))
            ->filter()
            ->unique()
            ->values();

        $retailerCount = $retailerNames->count();
        $normalized = min($retailerCount / 6, 1);

        $summary = $retailerCount > 0
            ? sprintf('%d retailer%s indexed', $retailerCount, $retailerCount === 1 ? '' : 's')
            : 'Awaiting active store links';

        return [
            'key' => 'retailers',
            'label' => 'Retailer footprint',
            'weight' => $weight,
            'score' => $normalized,
            'summary' => $summary,
            'context' => [
                'retailer_count' => $retailerCount,
                'retailer_names' => $retailerNames->all(),
            ],
        ];
    }

    /**
     * @param  Collection<int, SkuRegion>  $skuRegions
     * @param  Collection<int, PriceSeriesAggregate>|null  $aggregates
     * @return array<string, mixed>
     */
    protected function scorePriceCoverage(Collection $skuRegions, float $weight, ?Collection $aggregates = null): array
    {
        if ($skuRegions->isEmpty()) {
            $entries = $aggregates?->sum('sample_count') ?? 0;

            return [
                'key' => 'coverage',
                'label' => 'Price coverage',
                'weight' => $weight,
                'score' => $entries > 0 ? min($entries / 50, 1) * 0.4 : 0.0,
                'summary' => $entries > 0 ? 'Historical samples cached' : 'Price pulls warming up',
                'context' => [
                    'region_count' => 0,
                    'currency_count' => 0,
                    'price_entries' => $entries,
                ],
            ];
        }

        $active = $skuRegions
            ->filter(fn (SkuRegion $region): bool => (bool) ($region->is_active ?? true));

        $regionCount = $active
            ->pluck('region_code')
            ->filter()
            ->map(static fn ($value) => strtoupper((string) $value))
            ->filter()
            ->unique()
            ->count();

        $currencyCount = $active
            ->pluck('currency')
            ->filter()
            ->map(static fn ($value) => strtoupper((string) $value))
            ->filter()
            ->unique()
            ->count();

        $entries = (int) ($aggregates?->sum('sample_count') ?? 0);

        $regionNormalized = min($regionCount / 6, 1);
        $currencyNormalized = min($currencyCount / 6, 1);
        $historyNormalized = min($entries / 75, 1);

        $normalized = ($regionNormalized * 0.55) + ($currencyNormalized * 0.25) + ($historyNormalized * 0.2);
        $normalized = min(max($normalized, 0), 1);

        $summaryParts = [];
        if ($regionCount > 0) {
            $summaryParts[] = sprintf('%d region%s', $regionCount, $regionCount === 1 ? '' : 's');
        }
        if ($currencyCount > 0) {
            $summaryParts[] = sprintf('%d currency%s', $currencyCount, $currencyCount === 1 ? '' : 's');
        }
        if ($entries > 0) {
            $summaryParts[] = sprintf('%d price sample%s', $entries, $entries === 1 ? '' : 's');
        }

        if ($summaryParts === []) {
            $summaryParts[] = 'Coverage warming up';
        }

        return [
            'key' => 'coverage',
            'label' => 'Price coverage',
            'weight' => $weight,
            'score' => $normalized,
            'summary' => implode(' · ', $summaryParts),
            'context' => [
                'region_count' => $regionCount,
                'currency_count' => $currencyCount,
                'price_entries' => $entries,
            ],
        ];
    }

    /**
     * @param  Collection<int, Platform>  $platforms
     * @return array<string, mixed>
     */
    protected function scorePlatforms(Product $product, Collection $platforms, float $weight): array
    {
        $labels = $platforms
            ->pluck('name')
            ->filter()
            ->map(static fn ($value) => trim((string) $value))
            ->filter()
            ->unique()
            ->values();

        if ($labels->isEmpty() && $product->platform) {
            $labels = collect([(string) $product->platform]);
        }

        $platformCount = $labels->count();
        $normalized = min($platformCount / 4, 1);

        $summary = $platformCount > 0
            ? sprintf('%d platform%s tracked', $platformCount, $platformCount === 1 ? '' : 's')
            : 'Primary platform pending';

        return [
            'key' => 'platforms',
            'label' => 'Platform breadth',
            'weight' => $weight,
            'score' => $normalized,
            'summary' => $summary,
            'context' => [
                'platform_count' => $platformCount,
                'platform_labels' => $labels->map(static fn ($value) => strtoupper($value))->all(),
            ],
        ];
    }

    protected function gradeFor(float $normalizedScore): string
    {
        if ($normalizedScore >= 0.9) {
            return 'S';
        }

        if ($normalizedScore >= 0.8) {
            return 'A';
        }

        if ($normalizedScore >= 0.7) {
            return 'B';
        }

        if ($normalizedScore >= 0.6) {
            return 'C';
        }

        if ($normalizedScore >= 0.5) {
            return 'D';
        }

        return 'E';
    }

    protected function verdictFor(float $normalizedScore): string
    {
        if ($normalizedScore >= 0.85) {
            return 'Benchmark ready';
        }

        if ($normalizedScore >= 0.7) {
            return 'High confidence coverage';
        }

        if ($normalizedScore >= 0.55) {
            return 'Good, with gaps to fill';
        }

        if ($normalizedScore >= 0.4) {
            return 'Still ingesting price feeds';
        }

        return 'Spotlight warming up';
    }
}
