<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class VideoGameTitleSource extends Model
{
    protected $fillable = [
        'video_game_title_id',
        'video_game_source_id',
        'provider',
        'external_id',
        'provider_item_id',
        'slug',
        'name',
        'description',
        'release_date',
        'platform',
        'rating',
        'rating_count',
        'developer',
        'publisher',
        'genre',
        'raw_payload',
    ];

    protected $casts = [
        'platform' => 'array',
        'genre' => 'array',
        'raw_payload' => 'array',
        'release_date' => 'date',
    ];

    public function title(): BelongsTo
    {
        return $this->belongsTo(VideoGameTitle::class, 'video_game_title_id');
    }

    public function videoGameSource(): BelongsTo
    {
        return $this->belongsTo(VideoGameSource::class);
    }
}
