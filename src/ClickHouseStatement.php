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

use ClickHouseDB\Client;
use ClickHouseDB\Exception\ClickHouseException;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\Exception\InvalidType;
use FOD\DBALClickHouse\Driver\Exception\Exception;

use function array_map;
use function array_replace;
use function current;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function mb_stripos;

class ClickHouseStatement implements Statement
{
    protected Client $client;

    protected string $statement;

    protected AbstractPlatform $platform;

    protected array $values = [];

    protected array $types = [];

    public function __construct(Client $client, string $statement, AbstractPlatform $platform)
    {
        $this->client    = $client;
        $this->statement = $statement;
        $this->platform  = $platform;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING): void
    {
        $this->values[$param] = $value;
        $this->types[$param]  = $type;
    }

    /**
     * {@inheritDoc}
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $this->values[$param] = $variable;
        $this->types[$param]  = $type;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function execute($params = null): Result
    {
        if (is_array($params)) {
            $this->values = array_replace($this->values, $params);
        }

        $statement = $this->statement;

        $firstPlaceholder = array_key_first($this->values);

        $positionalPlaceholders       = is_int($firstPlaceholder);
        $positionalPlaceholdersIsList = $firstPlaceholder === 0;

        if ($positionalPlaceholders) {
            $pieces = explode('?', $statement);

            foreach ($pieces as $key => &$piece) {
                $positionalPlaceholder = $positionalPlaceholdersIsList ? $key : $key + 1;

                if (array_key_exists($positionalPlaceholder, $this->values)) {
                    $piece .= $this->resolveType($positionalPlaceholder);
                }
            }

            $statement = implode('', $pieces);
        } else {
            foreach (array_keys($this->values) as $key) {
                $namedPlaceholder       = ":$key";
                $namedPlaceholderOffset = mb_stripos($statement, $namedPlaceholder);
                $namedPlaceholderLength = mb_strlen($namedPlaceholder);

                if ($namedPlaceholderOffset !== false) {
                    $statement = substr_replace(
                        $statement,
                        $this->resolveType($key),
                        $namedPlaceholderOffset,
                        $namedPlaceholderLength
                    );
                }
            }
        }

        try {
            return new ClickHouseResult(
                new \ArrayIterator(
                    mb_stripos($statement, 'select') === 0 ||
                    mb_stripos($statement, 'show') === 0 ||
                    mb_stripos($statement, 'describe') === 0
                        ? $this->client->select($statement)->rows()
                        : $this->client->write($statement)->rows()
                )
            );
        } catch (ClickHouseException $exception) {
            throw new Exception(previous: $exception, sqlState: $exception->getMessage());
        }
    }

    /**
     * @throws DBALException
     */
    protected function resolveType(int|string $key): string
    {
        $value = $this->values[$key];

        if ($value === null) {
            return 'NULL';
        }

        if (is_array($value)) {
            if (is_int(current($value)) || is_float(current($value))) {
                foreach ($value as $item) {
                    if (!is_int($item) && !is_float($item)) {
                        throw new InvalidType('Array values must all be int/float or string, mixes not allowed');
                    }
                }
            } else {
                $value = array_map(function (?string $item): string {
                    return $item === null ? 'NULL' : $this->platform->quoteStringLiteral($item);
                }, $value);
            }

            return '[' . implode(', ', $value) . ']';
        }

        $type = $this->types[$key] ?? null;

        if ($type === null) {
            if (is_int($value) || is_float($value)) {
                $type = ParameterType::INTEGER;
            } elseif (is_bool($value)) {
                $type = ParameterType::BOOLEAN;
            }
        }

        return match ($type) {
            ParameterType::INTEGER => (string) $value,
            ParameterType::BOOLEAN => (string) (int) (bool) $value,
            default => $this->platform->quoteStringLiteral((string) $value)
        };
    }
}
