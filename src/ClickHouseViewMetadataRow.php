<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

class ClickHouseViewMetadataRow
{
    /**
     * @param ?non-empty-string $schemaName
     * @param non-empty-string  $viewName
     */
    public function __construct(
        private ?string $schemaName,
        private string $viewName,
        private string $sql,
    ) {
    }

    /** @return ?non-empty-string */
    public function getSchemaName(): ?string
    {
        return $this->schemaName;
    }

    /** @return non-empty-string */
    public function getViewName(): string
    {
        return $this->viewName;
    }

    public function getSql(): string
    {
        return $this->sql;
    }
}
