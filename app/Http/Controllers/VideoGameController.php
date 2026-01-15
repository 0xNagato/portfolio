<?php

namespace App\Http\Controllers;

use App\Models\VideoGame;
use Illuminate\Http\Request;
use Inertia\Inertia;

class VideoGameController extends Controller
{
    public function index(Request $request)
    {
        $sort = $request->input('sort', 'top_rated');
        
        $query = VideoGame::query();

        // Sorting logic
        if ($sort === 'top_rated') {
            $query->orderByDesc('opencritic_score');
        } elseif ($sort === 'newest') {
            $query->orderByDesc('created_at'); 
        }

        $games = $query->paginate(24)->withQueryString();

        return Inertia::render('VideoGames/Index', [
            'games' => $games,
            'filters' => $request->only(['sort']),
        ]);
    }

    public function show(VideoGame $game)
    {
        $prices = \Illuminate\Support\Facades\DB::table('video_game_prices')
            ->where('video_game_id', $game->id)
            ->get();

        return Inertia::render('VideoGames/Show', [
            'game' => $game,
            'prices' => $prices,
            'media' => $game->getMediaSummary(), 
        ]);
    }
}
