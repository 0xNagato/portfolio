<?php

declare(strict_types=1);

namespace App\Providers;

use App\Services\Ipv6ConnectionResolver;
use Illuminate\Support\ServiceProvider;

class DatabaseServiceProvider extends ServiceProvider
{
    /**
     * Register database connection handlers with IPv6 fallback support.
     */
    public function register(): void
    {
        // Register the IPv6-aware PostgreSQL connector
        $this->app->extend('db.connector.pgsql', function () {
            return new Ipv6ConnectionResolver;
        });
    }

    /**
     * Bootstrap services.
     */
    public function boot(): void
    {
        // Register the IPv6 fallback configuration for Supabase connections
        $this->registerSupabaseFallbacks();
    }

    /**
     * Register IPv6 fallback addresses from environment configuration.
     */
    private function registerSupabaseFallbacks(): void
    {
        $defaultConnection = config('database.default');

        if ($defaultConnection === 'pgsql') {
            $fallbackHost = env('DB_HOST_FALLBACK');

            if ($fallbackHost) {
                // Merge fallback configuration into the pgsql connection config
                config([
                    'database.connections.pgsql.host_fallback' => $fallbackHost,
                ]);
            }
        }
    }
}
