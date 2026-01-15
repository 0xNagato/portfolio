<?php

namespace App\Console\Commands;

use App\Jobs\ExtractRetailerPricesJob;
use Illuminate\Console\Command;

class ExtractRetailersCommand extends Command
{
    protected $signature = 'app:extract-retailers';
    protected $description = 'Scan video games for retailer links and populate prices table';

    public function handle()
    {
        $this->info("Dispatching ExtractRetailerPricesJob...");
        ExtractRetailerPricesJob::dispatch();
        $this->info("Job dispatched.");
    }
}
