<?php

namespace App\DataTransferObjects;

class ComparePageData
{
    /**
     * @param PresentedProductData $initialProduct
     * @param PresentedProductData[] $spotlight
     * @param string[] $regionOptions
     * @param array<string, mixed> $crossReferenceStats
     * @param array<string, string> $apiEndpoints
     * @param array<int, array<string, mixed>> $crossReferenceMatches
     * @param string[] $crossReferencePlatforms
     * @param string[] $crossReferenceCurrencies
     */
    public function __construct(
        private readonly PresentedProductData $initialProduct,
        private readonly array $spotlight,
        private readonly array $regionOptions,
        private readonly array $crossReferenceStats,
        private readonly array $apiEndpoints,
        private readonly array $crossReferenceMatches = [],
        private readonly array $crossReferencePlatforms = [],
        private readonly array $crossReferenceCurrencies = [],
    ) {}

    public function initialProduct(): PresentedProductData
    {
        return $this->initialProduct;
    }

    /**
     * @return PresentedProductData[]
     */
    public function spotlight(): array
    {
        return $this->spotlight;
    }

    /**
     * @return string[]
     */
    public function regionOptions(): array
    {
        return $this->regionOptions;
    }

    /**
     * @return array<string, mixed>
     */
    public function crossReferenceStats(): array
    {
        return $this->crossReferenceStats;
    }

    /**
     * @return array<string, string>
     */
    public function apiEndpoints(): array
    {
        return $this->apiEndpoints;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function crossReferenceMatches(): array
    {
        return $this->crossReferenceMatches;
    }

    /**
     * @return string[]
     */
    public function crossReferencePlatforms(): array
    {
        return $this->crossReferencePlatforms;
    }

    /**
     * @return string[]
     */
    public function crossReferenceCurrencies(): array
    {
        return $this->crossReferenceCurrencies;
    }

    /**
     * @return array<string, mixed>
     */
    public function toViewData(): array
    {
        $spotlight = array_map(static fn (PresentedProductData $product) => $product->toArray(), $this->spotlight);
        $spotlightCollection = collect($spotlight);

        $initialProduct = $this->initialProduct->toArray();
        $firstSpotlight = $spotlightCollection->first();

        if (! is_array($firstSpotlight)) {
            $firstSpotlight = $initialProduct;
        }

        $placeholderImage = asset('images/placeholders/game-cover.svg');

        $firstImage = (string) (data_get($firstSpotlight, 'image') ?: $placeholderImage);
        $firstName = (string) (data_get($firstSpotlight, 'name') ?: 'Spotlight warming up');
        $firstImageAlt = trim($firstName) !== '' ? ($firstName.' artwork') : 'Spotlight artwork';

        $firstScoreTotal = (float) data_get($firstSpotlight, 'spotlight_score.total', 0);
        $firstScoreValue = $firstScoreTotal > 0 ? number_format($firstScoreTotal, 1) : '—';
        $firstVerdict = (string) data_get($firstSpotlight, 'spotlight_score.verdict', 'Scoring in progress');
        $firstGrade = (string) data_get($firstSpotlight, 'spotlight_score.grade', '—');

        $firstMetrics = data_get($firstSpotlight, 'spotlight_score.metrics', []);
        $firstMetrics = is_array($firstMetrics) ? $firstMetrics : [];

        $firstMediaCaption = '';
        foreach ($firstMetrics as $metric) {
            if (is_array($metric) && ($metric['key'] ?? null) === 'media') {
                $firstMediaCaption = (string) ($metric['summary'] ?? 'Spotlight feed');
                break;
            }
        }
        if (trim($firstMediaCaption) === '') {
            $firstMediaCaption = 'Spotlight feed';
        }

        $platformLabels = data_get($firstSpotlight, 'platform_labels', []);
        $platformLabels = is_array($platformLabels) ? array_values(array_filter($platformLabels, fn ($v) => is_string($v) && trim($v) !== '')) : [];
        if ($platformLabels === []) {
            $platform = data_get($firstSpotlight, 'platform');
            if (is_string($platform) && trim($platform) !== '') {
                $platformLabels = [trim($platform)];
            }
        }

        $regionCount = (int) data_get($firstSpotlight, 'spotlight_score.context.region_count', 0);
        if ($regionCount <= 0) {
            $regionCodes = data_get($firstSpotlight, 'region_codes', []);
            $regionCount = is_array($regionCodes) ? count($regionCodes) : 0;
        }

        $retailerCount = (int) data_get($firstSpotlight, 'spotlight_score.context.retailer_count', 0);
        $mediaCount = (int) data_get($firstSpotlight, 'spotlight_score.context.media_count', 0);
        $currencyCount = (int) data_get($firstSpotlight, 'spotlight_score.context.currency_count', 0);

        $subtitleParts = [];
        if (! empty($platformLabels)) {
            $subtitleParts[] = strtoupper((string) $platformLabels[0]);
        }
        if ($regionCount > 0) {
            $subtitleParts[] = $regionCount.' '.($regionCount === 1 ? 'region' : 'regions');
        }
        if ($currencyCount > 0) {
            $subtitleParts[] = $currencyCount.' '.($currencyCount === 1 ? 'currency' : 'currencies');
        }
        if ($retailerCount > 0) {
            $subtitleParts[] = $retailerCount.' '.($retailerCount === 1 ? 'retailer' : 'retailers');
        }
        $firstSubtitle = $subtitleParts !== [] ? implode(' · ', $subtitleParts) : 'Coverage warming up';

        $firstContextTokens = [];
        if ($mediaCount > 0) {
            $firstContextTokens[] = $mediaCount.' media asset'.($mediaCount === 1 ? '' : 's');
        }
        if ($retailerCount > 0) {
            $firstContextTokens[] = $retailerCount.' retailer'.($retailerCount === 1 ? '' : 's');
        }
        if ($regionCount > 0) {
            $firstContextTokens[] = $regionCount.' region'.($regionCount === 1 ? '' : 's');
        }
        if ($currencyCount > 0) {
            $firstContextTokens[] = $currencyCount.' '.($currencyCount === 1 ? 'currency' : 'currencies');
        }

        if ($firstContextTokens === []) {
            $firstContextTokens = ['Live ingest warming up'];
        }

        return [
            'initialProduct' => $initialProduct,
            'spotlight' => $spotlight,
            'spotlightCollection' => $spotlightCollection,
            'firstSpotlight' => $firstSpotlight,
            'firstImage' => $firstImage,
            'firstImageAlt' => $firstImageAlt,
            'firstMediaCaption' => $firstMediaCaption,
            'firstSubtitle' => $firstSubtitle,
            'firstScoreValue' => $firstScoreValue,
            'firstVerdict' => $firstVerdict,
            'firstGrade' => $firstGrade,
            'firstMetrics' => $firstMetrics,
            'firstContextTokens' => $firstContextTokens,
            'regionOptions' => self::normalizeRegionOptionsForView($this->regionOptions),
            'crossReferenceStats' => $this->crossReferenceStats,
            'apiEndpoints' => $this->apiEndpoints,
            'prioritizedMatches' => $this->crossReferenceMatches,
            'crossReferencePlatforms' => $this->crossReferencePlatforms,
            'crossReferenceCurrencies' => $this->crossReferenceCurrencies,
        ];
    }

    /**
     * @param  string[]  $regionOptions
     * @return array<string, string>
     */
    private static function normalizeRegionOptionsForView(array $regionOptions): array
    {
        $labels = [
            'US' => 'United States',
            'GB' => 'United Kingdom',
            'EU' => 'Europe',
            'JP' => 'Japan',
            'CA' => 'Canada',
            'AU' => 'Australia',
        ];

        $map = [];

        foreach ($regionOptions as $code) {
            $normalized = strtoupper(trim((string) $code));
            if ($normalized === '') {
                continue;
            }

            $map[$normalized] = $labels[$normalized] ?? $normalized;
        }

        ksort($map);

        return $map;
    }
}
