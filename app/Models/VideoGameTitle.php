<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

class VideoGameTitle extends Model
{
    /** @use HasFactory<\Database\Factories\VideoGameTitleFactory> */
    use HasFactory;

    /**
     * CRITICAL:
     * This is the ONLY model/table in the game domain that owns `product_id`.
     * Products relate to VideoGames ONLY through this model.
     */
    protected $fillable = [
        'product_id',
        'name',
        'normalized_title',
        'slug',
        'providers',
    ];

    protected function casts(): array
    {
        return [
            'name' => 'string',
            'normalized_title' => 'string',
            'providers' => 'array',
        ];
    }

    public function product(): BelongsTo
    {
        return $this->belongsTo(Product::class);
    }

    public function videoGames(): HasMany
    {
        return $this->hasMany(VideoGame::class);
    }

    public function sources(): HasMany
    {
        return $this->hasMany(VideoGameTitleSource::class);
    }
}
