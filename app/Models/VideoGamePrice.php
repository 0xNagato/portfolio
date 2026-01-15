<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class VideoGamePrice extends Model
{
    /** @use HasFactory<\Database\Factories\VideoGamePriceFactory> */
    use HasFactory;

    protected $fillable = [
        'video_game_id',
        'currency',
        'amount_minor',
        'recorded_at',
        'retailer',
        'tax_inclusive',
    ];

    protected function casts(): array
    {
        return [
            'currency' => 'string',
            'amount_minor' => 'int',
            'recorded_at' => 'datetime',
            'tax_inclusive' => 'bool',
        ];
    }

    public function videoGame(): BelongsTo
    {
        return $this->belongsTo(VideoGame::class);
    }
}
