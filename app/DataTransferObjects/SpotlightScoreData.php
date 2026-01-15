<?php

namespace App\DataTransferObjects;

class SpotlightScoreData
{
    /**
     * @param array<int, array<string, mixed>> $metrics
     * @param array<string, mixed> $context
     */
    public function __construct(
        private readonly int $productId,
        private readonly float $totalScore,
        private readonly float $normalizedTotal,
        private readonly array $metrics,
        private readonly array $context,
        private readonly string $grade,
        private readonly string $verdict,
    ) {}

    public function productId(): int
    {
        return $this->productId;
    }

    public function totalScore(): float
    {
        return $this->totalScore;
    }

    public function normalizedTotal(): float
    {
        return $this->normalizedTotal;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function metrics(): array
    {
        return $this->metrics;
    }

    /**
     * @return array<string, mixed>
     */
    public function context(): array
    {
        return $this->context;
    }

    public function grade(): string
    {
        return $this->grade;
    }

    public function verdict(): string
    {
        return $this->verdict;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'product_id' => $this->productId,
            'total' => $this->totalScore,
            'normalized_total' => $this->normalizedTotal,
            'metrics' => $this->metrics,
            'context' => $this->context,
            'grade' => $this->grade,
            'verdict' => $this->verdict,
        ];
    }
}
