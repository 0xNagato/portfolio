<?php

declare(strict_types=1);

namespace App\DataTransferObjects\Media;

use Illuminate\Contracts\Support\Arrayable;

final class VideoMediaItem implements Arrayable
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
        public readonly ?int $durationSeconds,
        public readonly ?string $embedUrl,
        public readonly ?string $playUrl,
        public readonly ?string $videoId,
        public readonly ?int $ordinal,
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
            'thumbnail_url' => $this->thumbnail,
            'title' => $this->title,
            'caption' => $this->caption,
            'license' => $this->license,
            'license_url' => $this->licenseUrl,
            'attribution' => $this->attribution,
            'duration_seconds' => $this->durationSeconds,
            'embed_url' => $this->embedUrl,
            'play_url' => $this->playUrl,
            'video_id' => $this->videoId,
            'ordinal' => $this->ordinal,
            'metadata' => $this->metadata,
            'fetched_at' => $this->fetchedAt,
        ], static fn ($value) => $value !== null);
    }
}
