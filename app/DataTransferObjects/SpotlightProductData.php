<?php

namespace App\DataTransferObjects;

final class SpotlightProductData
{
    /**
     * @param array<int, array<string, mixed>> $breakdown
     * @param array<string, mixed> $metrics
     * @param array<string, mixed> $media
     * @param array<string, mixed> $context
     */
    public function __construct(
        private readonly PresentedProductData $presented,
        private readonly float $score,
        private readonly array $breakdown,
        private readonly array $metrics,
        private readonly array $media,
        private readonly array $context = [],
    ) {
    }

    public function presented(): PresentedProductData
    {
        return $this->presented;
    }

    public function score(): float
    {
        return $this->score;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function breakdown(): array
    {
        return $this->breakdown;
    }

    /**
     * @return array<string, mixed>
     */
    public function metrics(): array
    {
        return $this->metrics;
    }

    /**
     * @return array<string, mixed>
     */
    public function media(): array
    {
        return $this->media;
    }

    /**
     * @return array<string, mixed>
     */
    public function context(): array
    {
        return $this->context;
    }

    public function coverImage(): ?string
    {
        $cover = $this->media['cover'] ?? null;

        if (is_string($cover) && $cover !== '') {
            return $cover;
        }

        $product = $this->presented->toArray();

        $image = $product['image'] ?? $product['trailer_thumbnail'] ?? null;

        return is_string($image) && $image !== '' ? $image : null;
    }

    public function slug(): ?string
    {
        $product = $this->presented->toArray();
        $slug = $product['slug'] ?? null;

        return is_string($slug) && $slug !== '' ? $slug : null;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $product = $this->presented->toArray();

        $breakdown = array_map(static function (array $segment): array {
            $segment['points'] = round((float) ($segment['points'] ?? 0), 2);
            $segment['percentage'] = isset($segment['max_points']) && (float) $segment['max_points'] > 0
                ? max(0, min(100, ($segment['points'] / (float) $segment['max_points']) * 100))
                : 0;

            return $segment;
        }, $this->breakdown);

        return [
            'product' => $product,
            'slug' => $product['slug'] ?? null,
            'score' => [
                'value' => round($this->score, 2),
                'formatted' => number_format($this->score, 1),
                'breakdown' => $breakdown,
                'max' => 10.0,
            ],
            'metrics' => $this->metrics,
            'media' => $this->media,
            'context' => $this->context,
        ];
    }
}