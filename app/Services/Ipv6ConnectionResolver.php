<?php

declare(strict_types=1);

namespace App\Services;

use Illuminate\Database\Connectors\PostgresConnector;
use PDOException;

/**
 * Custom PostgreSQL connector that attempts IPv6 fallback on DNS resolution failures.
 * Used for Supabase connections which may require IPv6 when IPv4 is unavailable.
 */
class Ipv6ConnectionResolver extends PostgresConnector
{
    /**
     * Attempt to connect with IPv6 fallback support.
     * If the primary host fails with a DNS error, tries the IPv6 fallback address.
     *
     * @param  array  $config  Database configuration array
     */
    public function connect(array $config): \PDO
    {
        try {
            // Attempt normal connection first
            return parent::connect($config);
        } catch (PDOException $e) {
            // Check if this is a DNS resolution error
            if ($this->isDnsError($e) && isset($config['host_fallback'])) {
                // Log the failover attempt
                \Log::info('PostgreSQL DNS resolution failed, attempting IPv6 fallback', [
                    'primary_host' => $config['host'],
                    'fallback_host' => $config['host_fallback'],
                    'error' => $e->getMessage(),
                ]);

                // Update config to use IPv6 fallback
                $config['host'] = $config['host_fallback'];

                try {
                    // Attempt connection with IPv6 address
                    return parent::connect($config);
                } catch (PDOException $fallbackError) {
                    // Log that even fallback failed
                    \Log::error('PostgreSQL IPv6 fallback also failed', [
                        'fallback_host' => $config['host_fallback'],
                        'error' => $fallbackError->getMessage(),
                    ]);

                    // Re-throw the original error if both attempts fail
                    throw $e;
                }
            }

            // Not a DNS error or no fallback available, re-throw
            throw $e;
        }
    }

    /**
     * Check if the exception is a DNS resolution error.
     */
    private function isDnsError(PDOException $exception): bool
    {
        $message = $exception->getMessage();

        // Common DNS error patterns
        $dnsPatterns = [
            'nodename nor servname provided',
            'could not translate host name',
            'Name or service not known',
            'getaddrinfo failed',
            'Unknown error',
            'Connection refused',
        ];

        foreach ($dnsPatterns as $pattern) {
            if (stripos($message, $pattern) !== false) {
                return true;
            }
        }

        return false;
    }
}
