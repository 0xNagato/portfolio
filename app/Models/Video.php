<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Casts\Attribute;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use Illuminate\Support\Str;

/**
 * Video model with Spatie-compatible interface.
 *
 * This model uses aggregated storage (one row per game with all videos in JSON)
 * while providing Spatie Media Library-like accessors for interface compatibility.
 *
 * @property int $id
 * @property string $videoable_type
 * @property int $videoable_id
 * @property int|null $video_game_id
 * @property int|null $media_id
 * @property string|null $uuid
 * @property array|null $collection_names
 * @property string|null $primary_collection
 * @property string $url
 * @property string|null $external_id
 * @property string|null $video_id
 * @property string|null $source_url
 * @property string|null $provider
 * @property int|null $duration
 * @property int|null $width
 * @property int|null $height
 * @property string|null $thumbnail_url
 * @property string|null $title
 * @property string|null $description
 * @property int|null $order_column
 * @property array|null $urls
 * @property array|null $metadata
 * @property \Carbon\Carbon $created_at
 * @property \Carbon\Carbon $updated_at
 */
class Video extends Model
{
    /** @use HasFactory<\Database\Factories\VideoFactory> */
    use HasFactory;

    protected $fillable = [
        'videoable_type',
        'videoable_id',
        'video_game_id',
        'media_id',
        'uuid',
        'collection_names',
        'primary_collection',
        'url',
        'external_id',
        'video_id',
        'source_url',
        'urls',
        'provider',
        'duration',
        'width',
        'height',
        'thumbnail_url',
        'title',
        'description',
        'order_column',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'duration' => 'integer',
            'width' => 'integer',
            'height' => 'integer',
            'urls' => 'array',
            'metadata' => 'array',
            'collection_names' => 'array',
            'order_column' => 'integer',
        ];
    }

    protected static function booted(): void
    {
        static::creating(function (Video $video) {
            if (empty($video->uuid)) {
                $video->uuid = (string) Str::uuid();
            }
        });
    }

    // =========================================================================
    // RELATIONSHIPS
    // =========================================================================

    public function videoable(): MorphTo
    {
        return $this->morphTo();
    }

    public function videoGame(): BelongsTo
    {
        return $this->belongsTo(VideoGame::class);
    }

    public function media(): BelongsTo
    {
        return $this->belongsTo(\Spatie\MediaLibrary\MediaCollections\Models\Media::class);
    }

    // =========================================================================
    // SPATIE-COMPATIBLE ACCESSORS
    // =========================================================================

    /**
     * Get custom_properties (Spatie naming convention).
     * Maps to our 'metadata' column.
     */
    protected function customProperties(): Attribute
    {
        return Attribute::make(
            get: fn () => $this->metadata ?? [],
            set: fn (array $value) => ['metadata' => $value],
        );
    }

    /**
     * Get a specific custom property by key (Spatie interface).
     *
     * @param  mixed  $default
     * @return mixed
     */
    public function getCustomProperty(string $key, $default = null)
    {
        $properties = $this->metadata ?? [];

        return data_get($properties, $key, $default);
    }

    /**
     * Set a custom property (Spatie interface).
     *
     * @param  mixed  $value
     * @return $this
     */
    public function setCustomProperty(string $key, $value): self
    {
        $properties = $this->metadata ?? [];
        data_set($properties, $key, $value);
        $this->metadata = $properties;

        return $this;
    }

    /**
     * Check if a custom property exists (Spatie interface).
     */
    public function hasCustomProperty(string $key): bool
    {
        $properties = $this->metadata ?? [];

        return data_get($properties, $key) !== null;
    }

    /**
     * Forget a custom property (Spatie interface).
     *
     * @return $this
     */
    public function forgetCustomProperty(string $key): self
    {
        $properties = $this->metadata ?? [];
        data_forget($properties, $key);
        $this->metadata = $properties;

        return $this;
    }

    /**
     * Get all video metadata entries from the aggregated data.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getAllDetails(): array
    {
        return $this->metadata ?? [];
    }

    /**
     * Get collections array.
     *
     * @return array<int, string>
     */
    public function getCollections(): array
    {
        return $this->collection_names ?? ['trailers'];
    }

    /**
     * Get the collection name (Spatie interface).
     */
    protected function collectionName(): Attribute
    {
        return Attribute::make(
            get: fn () => $this->primary_collection
                ?? ($this->collection_names[0] ?? 'trailers'),
        );
    }

    /**
     * Get file name (Spatie interface).
     */
    protected function fileName(): Attribute
    {
        return Attribute::make(
            get: fn () => ($this->video_id ?? $this->external_id ?? 'video').'.mp4',
        );
    }

    /**
     * Get name without extension (Spatie interface).
     */
    protected function name(): Attribute
    {
        return Attribute::make(
            get: fn () => $this->title ?? $this->video_id ?? 'video',
        );
    }

    /**
     * Get mime type (Spatie interface).
     */
    protected function mimeType(): Attribute
    {
        return Attribute::make(
            get: fn () => 'video/mp4',
        );
    }

    // =========================================================================
    // VIDEO-SPECIFIC ACCESSORS
    // =========================================================================

    /**
     * Get YouTube embed URL.
     */
    public function getYoutubeEmbedUrl(): ?string
    {
        $videoId = $this->video_id ?? $this->external_id;

        if (! $videoId || $this->provider !== 'youtube') {
            return null;
        }

        return "https://www.youtube.com/embed/{$videoId}";
    }

    /**
     * Get YouTube watch URL.
     */
    public function getYoutubeWatchUrl(): ?string
    {
        $videoId = $this->video_id ?? $this->external_id;

        if (! $videoId || $this->provider !== 'youtube') {
            return null;
        }

        return "https://www.youtube.com/watch?v={$videoId}";
    }

    /**
     * Get YouTube thumbnail URL.
     *
     * @param  string  $quality  maxresdefault, hqdefault, mqdefault, sddefault
     */
    public function getYoutubeThumbnailUrl(string $quality = 'hqdefault'): ?string
    {
        $videoId = $this->video_id ?? $this->external_id;

        if (! $videoId || $this->provider !== 'youtube') {
            return $this->thumbnail_url;
        }

        return "https://img.youtube.com/vi/{$videoId}/{$quality}.jpg";
    }

    // =========================================================================
    // COLLECTION ACCESS METHODS (Spatie-like interface)
    // =========================================================================

    /**
     * Get all video IDs from the aggregated urls array.
     *
     * @return array<int, string>
     */
    public function getAllVideoIds(): array
    {
        return $this->urls ?? [];
    }

    /**
     * Get details for a specific video by index.
     *
     * @return array<string, mixed>|null
     */
    public function getVideoDetails(int $index): ?array
    {
        $metadata = $this->metadata ?? [];

        return $metadata[$index] ?? null;
    }

    /**
     * Get the count of individual videos in this aggregated row.
     */
    public function getVideoCount(): int
    {
        return count($this->urls ?? []);
    }

    /**
     * Check if this video row has a specific collection.
     */
    public function hasCollection(string $collection): bool
    {
        $collections = $this->collection_names ?? [];

        return in_array($collection, $collections, true);
    }

    /**
     * Expand this aggregated row into individual "virtual" video objects.
     * Useful for iterating over each video separately.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function expandToIndividual(): Collection
    {
        $videoIds = $this->urls ?? [];
        $metadata = $this->metadata ?? [];

        return collect($videoIds)->map(function ($videoId, $index) use ($metadata) {
            $detail = $metadata[$index] ?? [];

            return [
                'id' => $this->id,
                'parent_id' => $this->id,
                'index' => $index,
                'uuid' => $this->uuid ? "{$this->uuid}-{$index}" : null,
                'collection_name' => $this->primary_collection ?? 'trailers',
                'video_id' => $videoId,
                'external_id' => $videoId,
                'provider' => $this->provider ?? 'youtube',
                'name' => $detail['name'] ?? null,
                'title' => $detail['name'] ?? null,
                'youtube_embed_url' => $this->provider === 'youtube'
                    ? "https://www.youtube.com/embed/{$videoId}"
                    : null,
                'youtube_watch_url' => $this->provider === 'youtube'
                    ? "https://www.youtube.com/watch?v={$videoId}"
                    : null,
                'thumbnail_url' => $this->provider === 'youtube'
                    ? "https://img.youtube.com/vi/{$videoId}/hqdefault.jpg"
                    : null,
            ];
        });
    }

    /**
     * Get videos filtered by name pattern (e.g., exclude "devlog").
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function getFilteredVideos(?string $excludePattern = null): Collection
    {
        $expanded = $this->expandToIndividual();

        if ($excludePattern) {
            $expanded = $expanded->filter(function ($video) use ($excludePattern) {
                $name = $video['name'] ?? '';

                return ! str_contains(strtolower($name), strtolower($excludePattern));
            });
        }

        return $expanded->values();
    }

    // =========================================================================
    // QUERY SCOPES
    // =========================================================================

    /**
     * Scope to filter by collection name.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Video>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Video>
     */
    public function scopeForCollection($query, string $collection)
    {
        return $query->where(function ($q) use ($collection) {
            $q->where('primary_collection', $collection)
                ->orWhereJsonContains('collection_names', $collection);
        });
    }

    /**
     * Scope to filter by provider.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Video>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Video>
     */
    public function scopeForProvider($query, string $provider)
    {
        return $query->where('provider', $provider);
    }

    /**
     * Scope to order by Spatie-compatible order_column.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Video>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Video>
     */
    public function scopeOrdered($query)
    {
        return $query->orderBy('order_column');
    }

    /**
     * Scope to filter YouTube videos only.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Video>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Video>
     */
    public function scopeYoutube($query)
    {
        return $query->where('provider', 'youtube');
    }
}
