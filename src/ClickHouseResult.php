<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;

class ClickHouseResult implements Result
{
    private Statement $clickHouseStatement;

    public function __construct(
        Statement $clickHouseStatement,
    ) {
        $this->clickHouseStatement = $clickHouseStatement;
    }

    /**
     * @inheritDoc
     */
    public function fetchOne()
    {
        return $this->clickHouseStatement->fetch();
    }

    /**
     * @inheritDoc
     */
    public function columnCount(): int
    {
        return $this->clickHouseStatement->columnCount();
    }

    /**
     * @inheritDoc
     */
    public function fetchNumeric()
    {
        return $this->clickHouseStatement->fetch();
    }

    /**
     * @inheritDoc
     */
    public function fetchAssociative()
    {
        return $this->clickHouseStatement->fetch();
    }

    /**
     * @inheritDoc
     */
    public function fetchAllNumeric(): array
    {
        return $this->clickHouseStatement->fetchAll();
    }

    /**
     * @inheritDoc
     */
    public function fetchAllAssociative(): array
    {
        return $this->clickHouseStatement->fetchAll();
    }

    /**
     * @inheritDoc
     */
    public function free(): void
    {
        $this->clickHouseStatement->closeCursor();
    }

    /**
     * @inheritDoc
     */
    public function rowCount(): int
    {
        return $this->clickHouseStatement->rowCount();
    }

    public function fetchFirstColumn(): array
    {
        return $this->clickHouseStatement->fetch();
    }
}
