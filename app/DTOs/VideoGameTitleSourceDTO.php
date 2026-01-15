<?php

namespace App\DTOs;

class VideoGameTitleSourceDTO extends DataTransferObject
{
    public function __construct(
        public int $id,
        public int $video_game_title_id,
        public int $video_game_source_id,
        public ?string $provider_item_id,
        public ?string $raw_title,
        public ?string $normalized_title,
        public ?array $metadata, // Mapped to raw_payload or specific fields
        public ?string $created_at,
        public ?string $updated_at,
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,video_game_id,video_game_source_id,provider_item_id,raw_title,normalized_title,locale,version_hint,metadata,created_at,updated_at
        
        // Note: CSV has `video_game_id`, but usually this table links to `video_game_title_id`. 
        // Assuming `video_game_id` in this CSV refers to the ID in `video_game_titles` table (which usually links to `products`).
        
        return new self(
            id: (int) $row['id'],
            video_game_title_id: (int) $row['video_game_id'], // Mapping CSV video_game_id -> DB video_game_title_id
            video_game_source_id: (int) $row['video_game_source_id'],
            provider_item_id: $row['provider_item_id'] ?? null,
            raw_title: $row['raw_title'] ?? null,
            normalized_title: $row['normalized_title'] ?? null,
            metadata: isset($row['metadata']) ? json_decode($row['metadata'], true) : [],
            created_at: $row['created_at'] ?? now()->toDateTimeString(),
            updated_at: $row['updated_at'] ?? now()->toDateTimeString(),
        );
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'video_game_title_id' => $this->video_game_title_id,
            'video_game_source_id' => $this->video_game_source_id,
            // provider column is needed in DB but not in CSV explicitely. 
            // We might need to look it up or infer it. 
            // For bulk import, maybe skip 'provider' string if nullable or inferred? 
            // Schema says 'provider' varchar is present.
            // We'll leave it null for now or handle in Command.
            'provider_item_id' => $this->provider_item_id,
            'name' => $this->raw_title, // Map raw_title -> name
            'slug' => \Illuminate\Support\Str::slug($this->raw_title ?? ''), // Generate slug
            'raw_payload' => json_encode($this->metadata),
            'created_at' => $this->created_at,
            'updated_at' => $this->updated_at,
        ];
    }
}
