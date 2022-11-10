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

use Doctrine\DBAL\Driver\Result;

class ClickHouseResult implements Result
{
    public function __construct(private ?\ArrayIterator $iterator)
    {
    }

    public function fetchNumeric()
    {
        $row = $this->iterator->current();

        if ($row === null) {
            return false;
        }

        $this->iterator->next();

        return array_values($row);
    }

    public function fetchAssociative()
    {
        $row = $this->iterator->current();

        if ($row === null) {
            return false;
        }

        $this->iterator->next();

        return $row;
    }

    public function fetchOne()
    {
        $row = $this->iterator->current();

        if ($row === null) {
            return false;
        }

        $this->iterator->next();

        return current($row);
    }

    public function fetchAllNumeric(): array
    {
        return array_map('array_values', $this->iterator->getArrayCopy());
    }

    public function fetchAllAssociative(): array
    {
        return $this->iterator->getArrayCopy();
    }

    public function fetchFirstColumn(): array
    {
        $row = $this->iterator->current();

        if ($row === null) {
            return [];
        }

        $this->iterator->next();

        return array_column($this->iterator->getArrayCopy(), array_key_first($row));
    }

    public function rowCount(): int
    {
        return $this->iterator->count();
    }

    public function columnCount(): int
    {
        $row = $this->iterator->current();

        if ($row === null) {
            return 0;
        }

        $this->iterator->next();

        return count($row);
    }

    public function free(): void
    {
        $this->iterator = null;
    }
}
