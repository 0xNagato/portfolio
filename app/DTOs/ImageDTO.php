<?php

namespace App\DTOs;

use Illuminate\Support\Str;

class ImageDTO extends DataTransferObject
{
    public function __construct(
        public string $uuid,
        public string $collection_names, // JSON
        public string $primary_collection,
        public ?int $order_column,
        public ?string $external_id,
        public ?string $provider,
        public string $url,
        public ?string $source_url,
        public ?int $width,
        public ?int $height,
        public ?string $alt_text,
        public ?string $caption,
        public ?array $metadata,
        public ?string $variants, // JSON from CSV
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,game_provider_id,image_key,url,mime_type,width,height,rank,caption,variants,metadata,created_at,updated_at,media_id,storage_disk,storage_path,provider_item_id,video_game_source_id,provider_payload

        $rank = isset($row['rank']) && $row['rank'] !== '' ? (int) $row['rank'] : 0;
        
        // Determine collection based on rank or caption
        // Common convention: Rank 1 is often cover, others are screenshots
        $collection = $rank === 1 ? 'cover' : 'screenshots';

        return new self(
            uuid: (string) Str::uuid(),
            collection_names: json_encode([$collection]),
            primary_collection: $collection,
            order_column: $rank,
            external_id: $row['image_key'] ?? $row['provider_item_id'] ?? null,
            provider: $row['game_provider_id'] ?? 'unknown',
            url: $row['url'] ?? '',
            source_url: $row['url'] ?? '', // Default to same URL if no source
            width: isset($row['width']) && $row['width'] !== '' ? (int) $row['width'] : null,
            height: isset($row['height']) && $row['height'] !== '' ? (int) $row['height'] : null,
            alt_text: $row['caption'] ?? null,
            caption: $row['caption'] ?? null,
            metadata: isset($row['metadata']) ? json_decode($row['metadata'], true) : [],
            variants: $row['variants'] ?? null,
        );
    }
}
