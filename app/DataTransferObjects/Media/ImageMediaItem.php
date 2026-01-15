<?php

declare(strict_types=1);

namespace App\DataTransferObjects\Media;

use Illuminate\Contracts\Support\Arrayable;

final class ImageMediaItem implements Arrayable
{
    public function __construct(
        public readonly int $id,
        public readonly string $source,
        public readonly string $url,
        public readonly ?string $thumbnail,
        public readonly ?string $title,
        public readonly ?string $caption,
        public readonly ?string $license,
        public readonly ?string $licenseUrl,
        public readonly ?string $attribution,
        public readonly ?int $width,
        public readonly ?int $height,
        public readonly ?float $quality,
        public readonly ?int $ordinal,
        public readonly bool $isPrimary,
        public readonly string $kind,
        /** @var array<string, mixed> */
        public readonly array $metadata = [],
        public readonly ?string $fetchedAt = null,
    ) {
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return array_filter([
            'id' => $this->id,
            'source' => $this->source,
            'url' => $this->url,
            'thumbnail' => $this->thumbnail,
            'title' => $this->title,
            'caption' => $this->caption,
            'license' => $this->license,
            'license_url' => $this->licenseUrl,
            'attribution' => $this->attribution,
            'width' => $this->width,
            'height' => $this->height,
            'quality' => $this->quality,
            'ordinal' => $this->ordinal,
            'is_primary' => $this->isPrimary,
            'kind' => $this->kind,
            'metadata' => $this->metadata,
            'fetched_at' => $this->fetchedAt,
        ], static fn ($value) => $value !== null);
    }
}
