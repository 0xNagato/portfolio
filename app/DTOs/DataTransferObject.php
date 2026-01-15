<?php

namespace App\DTOs;

use Illuminate\Contracts\Support\Arrayable;

abstract class DataTransferObject implements Arrayable
{
    abstract public static function fromCsv(array $row): self;

    public function toArray(): array
    {
        return get_object_vars($this);
    }
}
