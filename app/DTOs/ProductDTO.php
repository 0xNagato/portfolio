<?php

namespace App\DTOs;

class ProductDTO extends DataTransferObject
{
    public function __construct(
        public int $id,
        public string $name,
        public ?string $slug,
        public string $type,
        public ?array $attributes,
        public ?string $created_at,
        public ?string $updated_at,
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,name,platform,slug,category,release_date,metadata,created_at,updated_at,uid,synopsis,primary_platform_family,popularity_score,rating,freshness_score,external_ids

        // Map fields that don't have direct columns to attributes JSON
        $attributes = [
            'platform' => $row['platform'] ?? null,
            'category' => $row['category'] ?? null,
            'release_date' => $row['release_date'] ?? null,
            'uid' => $row['uid'] ?? null,
            'synopsis' => $row['synopsis'] ?? null,
            'primary_platform_family' => $row['primary_platform_family'] ?? null,
            'popularity_score' => $row['popularity_score'] ?? null,
            'rating' => $row['rating'] ?? null,
            'freshness_score' => $row['freshness_score'] ?? null,
            'external_ids' => $row['external_ids'] ?? null,
            'original_metadata' => isset($row['metadata']) ? json_decode($row['metadata'], true) : [],
        ];

        return new self(
            id: (int) $row['id'],
            name: $row['name'] ?? 'Unknown Product',
            slug: $row['slug'] ?? null,
            type: 'video_game', // Defaulting to video_game as per context
            attributes: $attributes, // Schema doesn't have attributes col, but Products usually don't need it or keys are distinct. 
            // Wait, schema for `products` table: id, slug, name, title, normalized_title, type, created_at, updated_at.
            // There is no attributes column in `products`. 
            // We should map what we can and maybe ignore the rest or verify if we need to add columns.
            // The user asked to map to *existing* tables.
            created_at: $row['created_at'] ?? now()->toDateTimeString(),
            updated_at: $row['updated_at'] ?? now()->toDateTimeString(),
        );
    }
    
    public function toArray(): array
    {
        // Adjust to match table schema exactly
        return [
            'id' => $this->id,
            'slug' => $this->slug,
            'name' => $this->name,
            'title' => $this->name, // Map name to title as well
            'normalized_title' => \Illuminate\Support\Str::slug($this->name),
            'type' => $this->type,
            'created_at' => $this->created_at,
            'updated_at' => $this->updated_at,
        ];
    }
}
