<?php

declare(strict_types=1);

namespace App\DataTransferObjects\Media;

use Illuminate\Support\Collection;

final class ProductMediaSet
{
    /**
     * @param  Collection<int, ImageMediaItem>  $images
     * @param  Collection<int, VideoMediaItem>  $videos
     */
    public function __construct(
        public readonly Collection $images,
        public readonly Collection $videos,
    ) {
    }

    public function primaryImage(): ?ImageMediaItem
    {
        return $this->images
            ->sortByDesc(fn (ImageMediaItem $item) => [$item->isPrimary, $this->imagePriority($item), $item->quality ?? 0.0, $item->ordinal ?? 0, $item->id])
            ->first();
    }

    /**
     * Policy: never use screenshots as cover.
     * If only screenshots exist, returns null.
     */
    public function primaryCoverImage(): ?ImageMediaItem
    {
        $candidates = $this->images
            ->reject(fn (ImageMediaItem $item) => $this->isScreenshotKind($item->kind));

        if ($candidates->isEmpty()) {
            return null;
        }

        return $candidates
            ->sortByDesc(fn (ImageMediaItem $item) => [$item->isPrimary, $this->imagePriority($item), $item->quality ?? 0.0, $item->ordinal ?? 0, $item->id])
            ->first();
    }

    public function primaryVideo(): ?VideoMediaItem
    {
        return $this->videos
            ->sortBy(fn (VideoMediaItem $item) => [$item->ordinal ?? 0, $item->id])
            ->first();
    }

    /**
     * @return Collection<int, ImageMediaItem>
     */
    public function gallery(): Collection
    {
        return $this->images
            ->sortByDesc(fn (ImageMediaItem $item) => [$item->isPrimary, $this->imagePriority($item), $item->quality ?? 0.0, -($item->ordinal ?? 0)])
            ->values();
    }

    /**
     * @return Collection<int, VideoMediaItem>
     */
    public function trailers(): Collection
    {
        return $this->videos
            ->sortBy(fn (VideoMediaItem $item) => [$item->ordinal ?? 0, $item->id])
            ->values();
    }

    public function imageCount(): int
    {
        return $this->images->count();
    }

    public function videoCount(): int
    {
        return $this->videos->count();
    }

    public function totalAssets(): int
    {
        return $this->imageCount() + $this->videoCount();
    }

    protected function imagePriority(ImageMediaItem $item): int
    {
        return match ($item->kind) {
            'cover', 'box_art', 'boxart' => 400,
            'background', 'backdrop', 'banner', 'fanart', 'keyart', 'key_art', 'poster' => 350,
            'artwork' => 300,
            'screenshot' => 200,
            'icon', 'thumb', 'thumbnail' => 150,
            default => 100,
        };
    }

    protected function isScreenshotKind(?string $kind): bool
    {
        $kind = strtolower((string) ($kind ?? ''));

        if ($kind === '') {
            return false;
        }

        if ($kind === 'screenshot') {
            return true;
        }

        return str_contains($kind, 'screenshot');
    }
}
