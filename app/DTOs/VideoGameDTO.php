<?php

namespace App\DTOs;

class VideoGameDTO extends DataTransferObject
{
    public function __construct(
        public ?string $external_id,
        public ?string $provider,
        public string $title,
        public ?string $release_date, // Keep as string for now, parse later if needed
        public ?array $attributes, // JSON column in DB
        // Add other mapped fields
        public ?string $slug,
        public ?string $normalized_title,
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,product_id,title,genre,release_date,developer,metadata,created_at,updated_at,slug,normalized_title,external_ids,external_links,platform_codes,region_codes,title_keywords,payload_hash,last_synced_at,genres

        // Map flat CSV fields to the 'attributes' JSON column where appropriate
        // The DB schema for video_games is:
        // id, video_game_title_id, provider, external_id, created_at, updated_at,
        // opencritic_score, opencritic_review_count, opencritic_tier, opencritic_user_score,
        // opencritic_user_count, opencritic_percent_recommended, opencritic_id, opencritic_updated_at, attributes

        $attributes = [
            'genre' => $row['genre'] ?? null,
            'developer' => $row['developer'] ?? null,
            'external_links' => $row['external_links'] ?? null,
            'platform_codes' => $row['platform_codes'] ?? null,
            'region_codes' => $row['region_codes'] ?? null,
            'title_keywords' => $row['title_keywords'] ?? null,
            'genres' => $row['genres'] ?? null,
            'original_metadata' => isset($row['metadata']) ? json_decode($row['metadata'], true) : [],
        ];

        return new self(
            external_id: $row['product_id'] ?? null, // Mapping product_id to external_id
            provider: 'unknown', // CSV doesn't seem to have provider? Or maybe inferred from source?
            title: $row['title'] ?? '',
            release_date: $row['release_date'] ?? null,
            attributes: $attributes,
            slug: $row['slug'] ?? null,
            normalized_title: $row['normalized_title'] ?? null,
        );
    }
}
