<?php

namespace App\DataTransferObjects;

class PresentedProductData
{
    /**
     * @param array<string, mixed> $attributes
     */
    private function __construct(private readonly array $attributes)
    {
    }

    /**
     * @param array<string, mixed> $attributes
     */
    public static function fromArray(array $attributes): self
    {
        return new self($attributes);
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return $this->attributes;
    }

    public function hasImage(): bool
    {
        $image = $this->attributes['image'] ?? null;

        return is_string($image) && $image !== '';
    }

    public function hasTrailerThumbnail(): bool
    {
        $thumbnail = $this->attributes['trailer_thumbnail'] ?? null;

        return is_string($thumbnail) && $thumbnail !== '';
    }

    public function image(): ?string
    {
        $image = $this->attributes['image'] ?? null;

        return is_string($image) ? $image : null;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function spotlightScore(): ?array
    {
        $score = $this->attributes['spotlight_score'] ?? null;

        return is_array($score) ? $score : null;
    }

    /**
     * @return array<int, array<string, mixed>>|null
     */
    public function spotlightGallery(): ?array
    {
        $gallery = $this->attributes['spotlight_gallery'] ?? null;

        return is_array($gallery) ? $gallery : null;
    }
}
