<?php


namespace FOD\DBALClickHouse;


use Doctrine\DBAL\Driver\Result;
use JetBrains\PhpStorm\Pure;


class ClickHouseResult implements Result
{
    public function __construct(
        private ClickHouseStatement $statement
    ) {
    }


    public function fetchNumeric()
    {
        return $this->statement->fetch();
    }


    public function fetchAssociative()
    {
        return $this->statement->fetch();
    }


    public function fetchOne()
    {
        return $this->statement->fetch();
    }


    public function fetchAllNumeric(): array
    {
        return $this->statement->fetchAll();
    }


    public function fetchAllAssociative(): array
    {
        return $this->statement->fetchAll();
    }


    public function fetchFirstColumn(): array
    {
        return $this->statement->fetchColumn();
    }


    #[Pure] public function rowCount(): int
    {
        return $this->statement->rowCount();
    }


    #[Pure] public function columnCount(): int
    {
        return $this->statement->columnCount();
    }


    public function free(): void
    {
        $this->statement->closeCursor();
    }
}
