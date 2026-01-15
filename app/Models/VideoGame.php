<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasOne;
use Spatie\MediaLibrary\HasMedia;
use Spatie\MediaLibrary\InteractsWithMedia;

/**
 * VideoGame model with Spatie Media Library integration.
 *
 * This model bridges between our aggregated media storage (Image/Video models)
 * and Spatie's interface patterns. It provides both:
 * - Native Spatie media collections (for user uploads)
 * - Bridge methods to access IGDB-imported media with full metadata
 *
 * @property int $id
 * @property int $video_game_title_id
 * @property string $provider
 * @property int $external_id
 * @property string|null $name
 * @property float|null $rating
 * @property string|null $release_date
 * @property array|null $attributes
 * @property \Carbon\Carbon $created_at
 * @property \Carbon\Carbon $updated_at
 */
class VideoGame extends Model implements HasMedia
{
    use HasFactory;
    use InteractsWithMedia;

    protected $fillable = [
        'video_game_title_id',
        'provider',
        'external_id',
        'name',
        'rating',
        'release_date',
        'attributes',
    ];

    protected $casts = [
        'attributes' => 'array',
        'provider' => 'string',
        'external_id' => 'integer',
        'rating' => 'decimal:2',
        'release_date' => 'date',
    ];

    // =========================================================================
    // RELATIONSHIPS
    // =========================================================================

    public function title(): BelongsTo
    {
        return $this->belongsTo(VideoGameTitle::class, 'video_game_title_id');
    }

    public function videoGameTitle(): BelongsTo
    {
        return $this->belongsTo(VideoGameTitle::class);
    }

    public function prices(): HasMany
    {
        return $this->hasMany(VideoGamePrice::class);
    }

    /**
     * Get the latest price record for efficient display.
     */
    public function latestPrice(): HasOne
    {
        return $this->hasOne(VideoGamePrice::class)->latestOfMany();
    }

    /**
     * Get the aggregated image record for this game.
     * Contains all images (covers, screenshots, artworks) in one row.
     */
    public function images(): HasOne
    {
        return $this->hasOne(Image::class);
    }

    /**
     * Get the aggregated video record for this game.
     * Contains all videos (trailers, gameplay) in one row.
     */
    public function videos(): HasOne
    {
        return $this->hasOne(Video::class);
    }

    // =========================================================================
    // SPATIE MEDIA COLLECTIONS (for user uploads)
    // =========================================================================

    public function registerMediaCollections(): void
    {
        $this->addMediaCollection('cover_images')
            ->singleFile()
            ->acceptsMimeTypes(['image/jpeg', 'image/png', 'image/webp', 'image/gif']);

        $this->addMediaCollection('screenshots')
            ->acceptsMimeTypes(['image/jpeg', 'image/png', 'image/webp', 'image/gif']);

        $this->addMediaCollection('artworks')
            ->acceptsMimeTypes(['image/jpeg', 'image/png', 'image/webp', 'image/gif']);

        $this->addMediaCollection('trailers')
            ->acceptsMimeTypes(['video/mp4', 'video/webm', 'video/ogg']);

        $this->addMediaCollection('gameplay')
            ->acceptsMimeTypes(['video/mp4', 'video/webm', 'video/ogg']);

        $this->addMediaCollection('preview')
            ->acceptsMimeTypes(['video/mp4', 'video/webm', 'video/ogg']);

        $this->addMediaCollection('adverts')
            ->acceptsMimeTypes(['video/mp4', 'video/webm', 'video/ogg']);
    }

    // =========================================================================
    // BRIDGE METHODS: Access aggregated media with Spatie-like interface
    // =========================================================================

    /**
     * Get all media for a collection (Spatie-like interface).
     * Checks both native Spatie media and our aggregated Image/Video storage.
     *
     * @return Collection<int, \Spatie\MediaLibrary\MediaCollections\Models\Media|array<string, mixed>>
     */
    public function getAllMediaForCollection(string $collectionName): Collection
    {
        // First, check native Spatie media
        $spatieMedia = $this->getMedia($collectionName);

        if ($spatieMedia->isNotEmpty()) {
            return $spatieMedia;
        }

        // Fall back to our aggregated storage
        return $this->getAggregatedMediaForCollection($collectionName);
    }

    /**
     * Get media from aggregated Image/Video storage for a collection.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function getAggregatedMediaForCollection(string $collectionName): Collection
    {
        $imageCollections = ['cover_images', 'screenshots', 'artworks'];
        $videoCollections = ['trailers', 'gameplay', 'preview', 'adverts'];

        if (in_array($collectionName, $imageCollections, true)) {
            $image = $this->images;
            if (! $image) {
                return collect();
            }

            return $image->expandToIndividual()
                ->filter(fn ($item) => ($item['collection_name'] ?? '') === $collectionName);
        }

        if (in_array($collectionName, $videoCollections, true)) {
            $video = $this->videos;
            if (! $video) {
                return collect();
            }

            return $video->expandToIndividual()
                ->filter(fn ($item) => ($item['collection_name'] ?? '') === $collectionName);
        }

        return collect();
    }

    /**
     * Check if this game has media in a collection.
     */
    public function hasMediaInCollection(string $collectionName): bool
    {
        // Check Spatie first
        if ($this->hasMedia($collectionName)) {
            return true;
        }

        // Check aggregated storage
        $image = $this->images;
        if ($image && $image->hasCollection($collectionName)) {
            return true;
        }

        $video = $this->videos;
        if ($video && $video->hasCollection($collectionName)) {
            return true;
        }

        return false;
    }

    /**
     * Get the first media item for a collection (Spatie-like interface).
     *
     * @return \Spatie\MediaLibrary\MediaCollections\Models\Media|array<string, mixed>|null
     */
    public function getFirstMediaForCollection(string $collectionName)
    {
        $media = $this->getAllMediaForCollection($collectionName);

        return $media->first();
    }

    /**
     * Get the first media URL for a collection (Spatie-like interface).
     */
    public function getFirstMediaUrl(string $collectionName = 'default', string $conversionName = ''): string
    {
        // Try Spatie first
        $spatieUrl = parent::getFirstMediaUrl($collectionName, $conversionName);
        if ($spatieUrl !== '') {
            return $spatieUrl;
        }

        // Fall back to aggregated storage
        $media = $this->getFirstMediaForCollection($collectionName);

        if (is_array($media)) {
            return $media['url'] ?? '';
        }

        return '';
    }

    // =========================================================================
    // CONVENIENCE METHODS: Direct access to common media with metadata
    // =========================================================================

    /**
     * Get the cover image with all metadata.
     *
     * @return array{url: string|null, size_variants: array<string, string>, width: int|null, height: int|null, external_id: string|null}|null
     */
    public function getCoverImage(): ?array
    {
        $image = $this->images;
        if (! $image || ! $image->hasCollection('cover_images')) {
            return null;
        }

        $details = $image->getDetailsForCollection('cover_images');
        $firstCover = $details[0] ?? null;

        if (! $firstCover) {
            return null;
        }

        return [
            'url' => $image->url,
            'size_variants' => $image->getSizeVariants(),
            'width' => $firstCover['width'] ?? $image->width,
            'height' => $firstCover['height'] ?? $image->height,
            'external_id' => $firstCover['image_id'] ?? $image->external_id,
            'checksum' => $firstCover['checksum'] ?? null,
            'custom_properties' => $image->custom_properties,
        ];
    }

    /**
     * Get all screenshots with metadata.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function getScreenshots(): Collection
    {
        $image = $this->images;
        if (! $image) {
            return collect();
        }

        return collect($image->getDetailsForCollection('screenshots'))
            ->map(fn ($detail) => [
                'url' => $detail['size_variants'][0] ?? $detail['url'] ?? null,
                'size_variants' => $detail['size_variants'] ?? [],
                'width' => $detail['width'] ?? null,
                'height' => $detail['height'] ?? null,
                'external_id' => $detail['image_id'] ?? null,
                'checksum' => $detail['checksum'] ?? null,
            ]);
    }

    /**
     * Get all artworks with metadata.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function getArtworks(): Collection
    {
        $image = $this->images;
        if (! $image) {
            return collect();
        }

        return collect($image->getDetailsForCollection('artworks'))
            ->map(fn ($detail) => [
                'url' => $detail['size_variants'][0] ?? $detail['url'] ?? null,
                'size_variants' => $detail['size_variants'] ?? [],
                'width' => $detail['width'] ?? null,
                'height' => $detail['height'] ?? null,
                'external_id' => $detail['image_id'] ?? null,
                'checksum' => $detail['checksum'] ?? null,
            ]);
    }

    /**
     * Get all trailers with metadata.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function getTrailers(): Collection
    {
        $video = $this->videos;
        if (! $video) {
            return collect();
        }

        return $video->getFilteredVideos('devlog');
    }

    /**
     * Get the first trailer with full metadata.
     *
     * @return array<string, mixed>|null
     */
    public function getFirstTrailer(): ?array
    {
        return $this->getTrailers()->first();
    }

    /**
     * Get cover image URL for a specific size.
     *
     * @param  string  $size  e.g., 't_thumb', 't_cover_big', 't_720p', 't_1080p'
     */
    public function getCoverUrl(string $size = 't_1080p'): ?string
    {
        $image = $this->images;
        if (! $image) {
            return null;
        }

        return $image->getUrlForSize($size) ?? $image->url;
    }

    /**
     * Get all image metadata (custom_properties) for this game.
     *
     * @return array<string, mixed>
     */
    public function getImageCustomProperties(): array
    {
        $image = $this->images;

        return $image ? $image->custom_properties : [];
    }

    /**
     * Get all video metadata (custom_properties) for this game.
     *
     * @return array<string, mixed>
     */
    public function getVideoCustomProperties(): array
    {
        $video = $this->videos;

        return $video ? $video->custom_properties : [];
    }

    /**
     * Get a specific image custom property.
     *
     * @param  mixed  $default
     * @return mixed
     */
    public function getImageCustomProperty(string $key, $default = null)
    {
        $image = $this->images;

        return $image ? $image->getCustomProperty($key, $default) : $default;
    }

    /**
     * Get a specific video custom property.
     *
     * @param  mixed  $default
     * @return mixed
     */
    public function getVideoCustomProperty(string $key, $default = null)
    {
        $video = $this->videos;

        return $video ? $video->getCustomProperty($key, $default) : $default;
    }

    /**
     * Get complete media summary with all metadata.
     *
     * @return array{images: array<string, mixed>, videos: array<string, mixed>}
     */
    public function getMediaSummary(): array
    {
        $image = $this->images;
        $video = $this->videos;

        return [
            'images' => [
                'has_cover' => $image && $image->hasCollection('cover_images'),
                'has_screenshots' => $image && $image->hasCollection('screenshots'),
                'has_artworks' => $image && $image->hasCollection('artworks'),
                'cover_url' => $image?->url,
                'collections' => $image?->getCollections() ?? [],
                'total_count' => $image?->getImageCount() ?? 0,
                'custom_properties' => $image?->custom_properties ?? [],
            ],
            'videos' => [
                'has_trailers' => $video !== null,
                'provider' => $video?->provider,
                'total_count' => $video?->getVideoCount() ?? 0,
                'collections' => $video?->getCollections() ?? [],
                'custom_properties' => $video?->custom_properties ?? [],
            ],
        ];
    }
}
