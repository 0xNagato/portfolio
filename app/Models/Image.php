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
 * Image model with Spatie-compatible interface.
 *
 * This model uses aggregated storage (one row per game with all images in JSON)
 * while providing Spatie Media Library-like accessors for interface compatibility.
 *
 * @property int $id
 * @property string $imageable_type
 * @property int $imageable_id
 * @property int|null $video_game_id
 * @property int|null $media_id
 * @property string|null $uuid
 * @property array|null $collection_names
 * @property string|null $primary_collection
 * @property string $url
 * @property string|null $external_id
 * @property string|null $provider
 * @property string|null $source_url
 * @property int|null $width
 * @property int|null $height
 * @property string|null $alt_text
 * @property string|null $caption
 * @property bool $is_thumbnail
 * @property int|null $order_column
 * @property array|null $urls
 * @property array|null $metadata
 * @property \Carbon\Carbon $created_at
 * @property \Carbon\Carbon $updated_at
 */
class Image extends Model
{
    /** @use HasFactory<\Database\Factories\ImageFactory> */
    use HasFactory;

    protected $fillable = [
        'imageable_type',
        'imageable_id',
        'video_game_id',
        'media_id',
        'uuid',
        'collection_names',
        'primary_collection',
        'url',
        'external_id',
        'provider',
        'source_url',
        'width',
        'height',
        'alt_text',
        'caption',
        'is_thumbnail',
        'order_column',
        'urls',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'urls' => 'array',
            'metadata' => 'array',
            'collection_names' => 'array',
            'width' => 'integer',
            'height' => 'integer',
            'is_thumbnail' => 'boolean',
            'order_column' => 'integer',
        ];
    }

    protected static function booted(): void
    {
        static::creating(function (Image $image) {
            if (empty($image->uuid)) {
                $image->uuid = (string) Str::uuid();
            }
        });
    }

    // =========================================================================
    // RELATIONSHIPS
    // =========================================================================

    public function imageable(): MorphTo
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
     * Get all details (individual image metadata) from the aggregated data.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getAllDetails(): array
    {
        return $this->metadata['all_details'] ?? $this->metadata['details'] ?? [];
    }

    /**
     * Get collections array from metadata.
     *
     * @return array<int, string>
     */
    public function getCollections(): array
    {
        return $this->collection_names ?? $this->metadata['collections'] ?? [];
    }

    /**
     * Get the collection name (Spatie interface).
     * Returns primary_collection or first from collection_names.
     */
    protected function collectionName(): Attribute
    {
        return Attribute::make(
            get: fn () => $this->primary_collection
                ?? ($this->collection_names[0] ?? 'default'),
        );
    }

    /**
     * Get file name (Spatie interface).
     * Extracts filename from URL.
     */
    protected function fileName(): Attribute
    {
        return Attribute::make(
            get: function () {
                $url = $this->url ?? '';
                $path = parse_url($url, PHP_URL_PATH);

                return $path ? basename($path) : 'image.jpg';
            },
        );
    }

    /**
     * Get name without extension (Spatie interface).
     */
    protected function name(): Attribute
    {
        return Attribute::make(
            get: fn () => pathinfo($this->file_name, PATHINFO_FILENAME),
        );
    }

    /**
     * Get mime type (Spatie interface).
     */
    protected function mimeType(): Attribute
    {
        return Attribute::make(
            get: function () {
                $ext = strtolower(pathinfo($this->file_name, PATHINFO_EXTENSION));

                return match ($ext) {
                    'jpg', 'jpeg' => 'image/jpeg',
                    'png' => 'image/png',
                    'gif' => 'image/gif',
                    'webp' => 'image/webp',
                    'svg' => 'image/svg+xml',
                    default => 'image/jpeg',
                };
            },
        );
    }

    // =========================================================================
    // COLLECTION ACCESS METHODS (Spatie-like interface)
    // =========================================================================

    /**
     * Get all URLs for a specific collection from the aggregated data.
     *
     * @return array<int, string>
     */
    public function getUrlsForCollection(string $collection): array
    {
        $details = $this->metadata['all_details'] ?? $this->metadata['details'] ?? [];

        return collect($details)
            ->filter(fn ($detail) => ($detail['collection'] ?? '') === $collection)
            ->flatMap(fn ($detail) => $detail['size_variants'] ?? [$detail['url'] ?? null])
            ->filter()
            ->unique()
            ->values()
            ->all();
    }

    /**
     * Get details for a specific collection.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getDetailsForCollection(string $collection): array
    {
        $details = $this->metadata['all_details'] ?? $this->metadata['details'] ?? [];

        return collect($details)
            ->filter(fn ($detail) => ($detail['collection'] ?? '') === $collection)
            ->values()
            ->all();
    }

    /**
     * Check if this image row has a specific collection.
     */
    public function hasCollection(string $collection): bool
    {
        $collections = $this->collection_names ?? $this->metadata['collections'] ?? [];

        return in_array($collection, $collections, true);
    }

    /**
     * Get the primary/cover image URL (largest size).
     */
    public function getCoverUrl(?string $size = null): ?string
    {
        if ($size && $this->hasCollection('cover_images')) {
            $urls = $this->urls ?? [];
            foreach ($urls as $url) {
                if (str_contains($url, "/{$size}/")) {
                    return $url;
                }
            }
        }

        return $this->url;
    }

    /**
     * Get URL for a specific IGDB size variant.
     *
     * @param  string  $size  e.g., 't_thumb', 't_cover_big', 't_1080p'
     */
    public function getUrlForSize(string $size): ?string
    {
        $urls = $this->urls ?? [];

        foreach ($urls as $url) {
            if (str_contains($url, "/{$size}/")) {
                return $url;
            }
        }

        return null;
    }

    /**
     * Get all size variants as an associative array.
     *
     * @return array<string, string>
     */
    public function getSizeVariants(): array
    {
        $urls = $this->urls ?? [];
        $variants = [];

        foreach ($urls as $url) {
            if (preg_match('/\/t_([a-z0-9_]+)\//', $url, $matches)) {
                $variants[$matches[1]] = $url;
            }
        }

        return $variants;
    }

    /**
     * Get the count of individual images in this aggregated row.
     */
    public function getImageCount(): int
    {
        $details = $this->metadata['all_details'] ?? $this->metadata['details'] ?? [];

        return count($details);
    }

    /**
     * Expand this aggregated row into individual "virtual" image objects.
     * Useful for iterating over each image separately.
     *
     * @return Collection<int, array<string, mixed>>
     */
    public function expandToIndividual(): Collection
    {
        $details = $this->metadata['all_details'] ?? $this->metadata['details'] ?? [];

        return collect($details)->map(function ($detail, $index) {
            return [
                'id' => $this->id,
                'parent_id' => $this->id,
                'index' => $index,
                'uuid' => $this->uuid ? "{$this->uuid}-{$index}" : null,
                'collection_name' => $detail['collection'] ?? $this->primary_collection,
                'url' => $detail['url'] ?? ($detail['size_variants'][0] ?? null),
                'size_variants' => $detail['size_variants'] ?? [],
                'external_id' => $detail['image_id'] ?? $this->external_id,
                'width' => $detail['width'] ?? null,
                'height' => $detail['height'] ?? null,
                'is_thumbnail' => $detail['is_thumbnail'] ?? false,
                'checksum' => $detail['checksum'] ?? null,
            ];
        });
    }

    // =========================================================================
    // QUERY SCOPES
    // =========================================================================

    /**
     * Scope to filter by collection name.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Image>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Image>
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
     * @param  \Illuminate\Database\Eloquent\Builder<Image>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Image>
     */
    public function scopeForProvider($query, string $provider)
    {
        return $query->where('provider', $provider);
    }

    /**
     * Scope to order by Spatie-compatible order_column.
     *
     * @param  \Illuminate\Database\Eloquent\Builder<Image>  $query
     * @return \Illuminate\Database\Eloquent\Builder<Image>
     */
    public function scopeOrdered($query)
    {
        return $query->orderBy('order_column');
    }
}
