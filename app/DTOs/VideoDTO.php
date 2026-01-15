<?php

namespace App\DTOs;

use Illuminate\Support\Str;

class VideoDTO extends DataTransferObject
{
    public function __construct(
        public string $uuid,
        public string $collection_names, // JSON
        public string $primary_collection,
        public ?int $order_column,
        public ?string $external_id,
        public ?string $provider,
        public string $url,
        public ?string $title,
        public ?int $duration,
        public ?string $source_url,
        public ?string $embed_url,
        public ?array $metadata,
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,game_provider_id,video_key,name,description,site_detail_url,embed_url,stream_url,duration_seconds,published_at,thumbnails,metadata,created_at,updated_at,media_id,storage_disk,storage_path,provider_item_id,video_game_source_id,provider_payload

        $collection = 'videos';

        return new self(
            uuid: (string) Str::uuid(),
            collection_names: json_encode([$collection]),
            primary_collection: $collection,
            order_column: 0, // CSV doesn't have explicit rank/order
            external_id: $row['video_key'] ?? $row['provider_item_id'] ?? null,
            provider: $row['game_provider_id'] ?? 'unknown',
            url: $row['stream_url'] ?? $row['embed_url'] ?? '',
            title: $row['name'] ?? null,
            duration: isset($row['duration_seconds']) && $row['duration_seconds'] !== '' ? (int) $row['duration_seconds'] : null,
            source_url: $row['site_detail_url'] ?? null,
            embed_url: $row['embed_url'] ?? null,
            metadata: isset($row['metadata']) ? json_decode($row['metadata'], true) : [],
        );
    }
}
