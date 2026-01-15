<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\FetchExchangeRatesJob;
use App\Models\Currency;
use App\Models\ExchangeRate;
use App\Services\ExchangeRates\BybitClient;
use App\Services\ExchangeRates\ForexClient;
use Illuminate\Console\Command;

class FetchExchangeRatesCommand extends Command
{
    protected $signature = 'rates:fetch
                            {--dry-run : Show what would be fetched without storing}
                            {--health : Check API health status}';

    protected $description = 'Fetch exchange rates from Bybit (crypto) and ExchangeRate-API (forex)';

    public function handle(BybitClient $bybitClient, ForexClient $forexClient): int
    {
        if ($this->option('health')) {
            return $this->healthCheck($bybitClient, $forexClient);
        }

        $this->info('🔄 Fetching exchange rates...');
        $this->newLine();

        // Show currency summary
        $currencies = Currency::all();
        $crypto = $currencies->where('is_crypto', true);
        $fiat = $currencies->where('is_crypto', false);

        $this->components->twoColumnDetail('Crypto currencies', $crypto->pluck('code')->join(', '));
        $this->components->twoColumnDetail('Fiat currencies', $fiat->pluck('code')->join(', '));
        $this->newLine();

        if ($this->option('dry-run')) {
            $this->warn('🏃 Dry run mode - no data will be stored');
            $this->newLine();

            $this->info('Would fetch the following rates:');
            $this->line('  • '.$crypto->count().' crypto × '.$fiat->count().' fiat = '.($crypto->count() * $fiat->count()).' crypto rates');
            $this->line('  • '.($fiat->count() - 1).' USD cross-rates');
            $this->line('  • 2 EUR cross-rates (EUR/GBP, EUR/JPY)');
            $this->newLine();

            return self::SUCCESS;
        }

        // Dispatch the job
        $this->info('📡 Dispatching FetchExchangeRatesJob...');

        FetchExchangeRatesJob::dispatchSync();

        $this->newLine();
        $this->components->info('✅ Exchange rates fetched successfully');

        // Show summary
        $latest = ExchangeRate::latest('fetched_at')->first();
        if ($latest) {
            $this->newLine();
            $this->components->twoColumnDetail('Latest fetch', $latest->fetched_at->diffForHumans());
            $this->components->twoColumnDetail('Total rates stored', number_format(ExchangeRate::count()));
            $this->components->twoColumnDetail('Bybit rates', number_format(ExchangeRate::where('provider', 'bybit')->count()));
            $this->components->twoColumnDetail('Forex rates', number_format(ExchangeRate::where('provider', 'exchangerate-api')->count()));
        }

        return self::SUCCESS;
    }

    private function healthCheck(BybitClient $bybitClient, ForexClient $forexClient): int
    {
        $this->info('🏥 Checking API health...');
        $this->newLine();

        $bybitStatus = $bybitClient->healthCheck();
        $forexStatus = $forexClient->healthCheck();

        $this->components->twoColumnDetail(
            'Bybit API',
            $bybitStatus ? '<fg=green>✓ Online</>' : '<fg=red>✗ Offline</>'
        );

        $this->components->twoColumnDetail(
            'ExchangeRate-API',
            $forexStatus ? '<fg=green>✓ Online</>' : '<fg=red>✗ Offline</>'
        );

        $this->newLine();

        if ($bybitStatus && $forexStatus) {
            $this->components->info('✅ All APIs are healthy');

            return self::SUCCESS;
        }

        $this->components->error('⚠️  Some APIs are unavailable');

        return self::FAILURE;
    }
}
