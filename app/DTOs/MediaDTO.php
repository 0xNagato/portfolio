<?php

namespace App\DTOs;

class MediaDTO extends DataTransferObject
{
    public function __construct(
        public int $id,
        public string $uuid,
        public string $model_type,
        public int $model_id,
        public string $collection_name,
        public string $name,
        public string $file_name,
        public string $mime_type,
        public string $disk,
        public ?string $conversions_disk,
        public int $size,
        public array $manipulations,
        public array $custom_properties,
        public array $generated_conversions,
        public array $responsive_images,
        public ?int $order_column,
        public ?string $created_at,
        public ?string $updated_at,
    ) {}

    public static function fromCsv(array $row): self
    {
        // CSV Headers:
        // id,uuid,model_type,model_id,collection_name,name,file_name,mime_type,disk,conversions_disk,size,manipulations,custom_properties,generated_conversions,responsive_images,order_column,derived_from_type,derived_from_id,created_at,updated_at

        return new self(
            id: (int) $row['id'],
            uuid: $row['uuid'],
            model_type: $row['model_type'],
            model_id: (int) $row['model_id'],
            collection_name: $row['collection_name'],
            name: $row['name'],
            file_name: $row['file_name'],
            mime_type: $row['mime_type'],
            disk: $row['disk'],
            conversions_disk: $row['conversions_disk'] ?: null,
            size: (int) $row['size'],
            manipulations: isset($row['manipulations']) ? json_decode($row['manipulations'], true) : [],
            custom_properties: isset($row['custom_properties']) ? json_decode($row['custom_properties'], true) : [],
            generated_conversions: isset($row['generated_conversions']) ? json_decode($row['generated_conversions'], true) : [],
            responsive_images: isset($row['responsive_images']) ? json_decode($row['responsive_images'], true) : [],
            order_column: isset($row['order_column']) && $row['order_column'] !== '' ? (int) $row['order_column'] : null,
            created_at: $row['created_at'] ?? now()->toDateTimeString(),
            updated_at: $row['updated_at'] ?? now()->toDateTimeString(),
        );
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'model_type' => $this->model_type,
            'model_id' => $this->model_id,
            'uuid' => $this->uuid,
            'collection_name' => $this->collection_name,
            'name' => $this->name,
            'file_name' => $this->file_name,
            'mime_type' => $this->mime_type,
            'disk' => $this->disk,
            'conversions_disk' => $this->conversions_disk,
            'size' => $this->size,
            'manipulations' => json_encode($this->manipulations),
            'custom_properties' => json_encode($this->custom_properties),
            'generated_conversions' => json_encode($this->generated_conversions),
            'responsive_images' => json_encode($this->responsive_images),
            'order_column' => $this->order_column,
            'created_at' => $this->created_at,
            'updated_at' => $this->updated_at,
        ];
    }
}
