<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Third Party Services
    |--------------------------------------------------------------------------
    |
    | This file is for storing the credentials for third party services such
    | as Mailgun, Postmark, AWS and more. This file provides the de facto
    | location for this type of information, allowing packages to have
    | a conventional file to locate the various service credentials.
    |
    */

    'postmark' => [
        'key' => env('POSTMARK_API_KEY'),
    ],

    'resend' => [
        'key' => env('RESEND_API_KEY'),
    ],

    'ses' => [
        'key' => env('AWS_ACCESS_KEY_ID'),
        'secret' => env('AWS_SECRET_ACCESS_KEY'),
        'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
    ],

    'slack' => [
        'notifications' => [
            'bot_user_oauth_token' => env('SLACK_BOT_USER_OAUTH_TOKEN'),
            'channel' => env('SLACK_BOT_USER_DEFAULT_CHANNEL'),
        ],
    ],

    'igdb' => [
        'client_id' => env('IGDB_CLIENT_ID'),
        'client_secret' => env('IGDB_CLIENT_SECRET'),
    ],

    'bybit' => [
        'base_url' => env('BYBIT_API_URL', 'https://api.bybit.com'),
        'timeout' => env('BYBIT_TIMEOUT', 10),
    ],

    'forex' => [
        'base_url' => env('FOREX_API_URL', 'https://api.exchangerate-api.com/v4'),
        'timeout' => env('FOREX_TIMEOUT', 10),
    ],

    'opencritic' => [
        'enabled' => env('OPENCRITIC_ENABLED', false),
        'api_key' => env('OPENCRITIC_API_KEY'),
        'base_url' => env('OPENCRITIC_BASE_URL', 'https://opencritic-api.p.rapidapi.com'),
        'host' => env('OPENCRITIC_HOST', 'opencritic-api.p.rapidapi.com'),
        'reqs_per_min' => env('OPENCRITIC_REQS_PER_MIN', 240),
        'cache_ttl' => env('OPENCRITIC_CACHE_TTL', 3600),
        'retry_attempts' => env('OPENCRITIC_RETRY_ATTEMPTS', 3),
        'timeout' => env('OPENCRITIC_TIMEOUT', 15),
    ],

    'ggdeals' => [
        'enabled' => env('GGDEALS_ENABLED', false),
        'api_key' => env('GGDEALS_API_KEY'),
        'base_url' => env('GGDEALS_BASE_URL', 'https://gg.deals/api'),
        'default_region' => env('GGDEALS_DEFAULT_REGION', 'us'),
        'reqs_per_min' => env('GGDEALS_REQS_PER_MIN', 100),
        'cache_ttl' => env('GGDEALS_CACHE_TTL', 1800),
        'timeout' => env('GGDEALS_TIMEOUT', 10),
    ],

    'itad' => [
        'enabled' => env('ITAD_ENABLED', true),
        'api_key' => env('ITAD_API_KEY'),
        'client_id' => env('ITAD_CLIENT_ID'),
        'default_region' => env('ITAD_DEFAULT_REGION', 'us'),
        'base_url' => 'https://api.isthereanydeal.com',
    ],

];
