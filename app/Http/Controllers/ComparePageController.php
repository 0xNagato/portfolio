<?php

namespace App\Http\Controllers;

use App\Actions\Compare\BuildComparePageDataAction;
use Illuminate\Http\Request;
use Inertia\Inertia;
use Inertia\Response;

class ComparePageController extends Controller
{
    /**
     * Handle the incoming request.
     */
    public function __invoke(Request $request): Response
    {
        $page = app(BuildComparePageDataAction::class)->handle(withCrossReference: true);

        return Inertia::render('compare', [
            'spotlight' => $page->spotlight,
            'crossReferenceStats' => $page->crossReferenceStats,
            'prioritizedMatches' => $page->crossReferenceMatches,
            'crossReferencePlatforms' => $page->crossReferencePlatforms,
            'crossReferenceCurrencies' => $page->crossReferenceCurrencies,
            'regionOptions' => $page->regionOptions,
            'apiEndpoints' => $page->apiEndpoints,
        ]);
    }
}
