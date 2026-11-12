<?php
namespace Beanbun\Tests;

/**
 * In-process Redis double implementing exactly the surface RedisQueue uses.
 * Holds the same logical structures as a real Redis deployment:
 *  - lists (rPush/lPush/lPop/rPop/lSize)
 *  - sets (sAdd/sRem/sIsMember/sMembers/sSize)
 *  - hashes (hGet/hIncrBy/hDel)
 *  - strings/bits (setbit/getbit/delete)
 * Pipeline mode queues commands and replays them atomically on exec().
 */
class FakeRedis
{
    protected $store = [];
    protected $pipeline = null;
    public $failNext = 0;

    public function connect($host, $port)
    {
        return true;
    }

    public function auth($password)
    {
        return true;
    }

    public function select($db)
    {
        return true;
    }

    public function setOption($name, $value)
    {
        return true;
    }

    protected function maybeFail()
    {
        if ($this->failNext > 0) {
            $this->failNext--;
            throw new \RedisException('injected fake redis failure');
        }
    }

    protected function dispatch($method, array $args)
    {
        $this->maybeFail();
        switch ($method) {
            case 'rPush':
                $key = $args[0];
                if (!isset($this->store[$key]) || !is_array($this->store[$key])) {
                    $this->store[$key] = [];
                }
                array_push($this->store[$key], $args[1]);
                return count($this->store[$key]);
            case 'lPush':
                $key = $args[0];
                if (!isset($this->store[$key]) || !is_array($this->store[$key])) {
                    $this->store[$key] = [];
                }
                array_unshift($this->store[$key], $args[1]);
                return count($this->store[$key]);
            case 'lPop':
                return array_shift($this->store[$args[0]]);
            case 'rPop':
                return array_pop($this->store[$args[0]]);
            case 'lSize':
                return isset($this->store[$args[0]]) && is_array($this->store[$args[0]])
                    ? count($this->store[$args[0]]) : 0;
            case 'sAdd':
                $key = $args[0];
                if (!isset($this->store[$key]) || !is_array($this->store[$key])) {
                    $this->store[$key] = [];
                }
                if (!in_array($args[1], $this->store[$key], true)) {
                    $this->store[$key][] = $args[1];
                }
                return 1;
            case 'sRem':
                if (isset($this->store[$args[0]]) && is_array($this->store[$args[0]])) {
                    $pos = array_search($args[1], $this->store[$args[0]], true);
                    if ($pos !== false) {
                        unset($this->store[$args[0]][$pos]);
                        $this->store[$args[0]] = array_values($this->store[$args[0]]);
                    }
                }
                return 1;
            case 'sIsMember':
                return isset($this->store[$args[0]]) && is_array($this->store[$args[0]])
                    && in_array($args[1], $this->store[$args[0]], true);
            case 'sMembers':
                return isset($this->store[$args[0]]) && is_array($this->store[$args[0]])
                    ? array_values($this->store[$args[0]]) : [];
            case 'sSize':
                return isset($this->store[$args[0]]) && is_array($this->store[$args[0]])
                    ? count($this->store[$args[0]]) : 0;
            case 'hGet':
                return isset($this->store[$args[0]][$args[1]]) ? $this->store[$args[0]][$args[1]] : false;
            case 'hIncrBy':
                if (!isset($this->store[$args[0]]) || !is_array($this->store[$args[0]])) {
                    $this->store[$args[0]] = [];
                }
                if (!isset($this->store[$args[0]][$args[1]])) {
                    $this->store[$args[0]][$args[1]] = 0;
                }
                return $this->store[$args[0]][$args[1]] += (int) $args[2];
            case 'hDel':
                unset($this->store[$args[0]][$args[1]]);
                return 1;
            case 'setbit':
                $key = $args[0];
                if (!isset($this->store[$key])) {
                    $this->store[$key] = '';
                }
                $byte = (int) floor($args[1] / 8);
                $bit = 7 - ($args[1] % 8);
                while (strlen($this->store[$key]) <= $byte) {
                    $this->store[$key] .= "\x00";
                }
                $old = (ord($this->store[$key][$byte]) >> $bit) & 1;
                $this->store[$key][$byte] = chr(ord($this->store[$key][$byte]) | (1 << $bit));
                return $old;
            case 'getbit':
                $key = $args[0];
                $byte = (int) floor($args[1] / 8);
                $bit = 7 - ($args[1] % 8);
                if (!isset($this->store[$key]) || strlen($this->store[$key]) <= $byte) {
                    return 0;
                }
                return (ord($this->store[$key][$byte]) >> $bit) & 1;
            case 'delete':
                unset($this->store[$args[0]]);
                return 1;
        }
        throw new \BadMethodCallException("FakeRedis does not implement $method");
    }

    public function __call($name, $arguments)
    {
        if ($this->pipeline !== null) {
            $this->pipeline[] = [$name, $arguments];
            return $this;
        }
        return $this->dispatch($name, $arguments);
    }

    public function multi($mode = null)
    {
        $this->pipeline = [];
        return $this;
    }

    public function pipeline()
    {
        return $this->multi();
    }

    public function exec()
    {
        if ($this->pipeline === null) {
            return [];
        }
        $queued = $this->pipeline;
        $this->pipeline = null;
        $results = [];
        // Atomic replay: a failure mid-batch leaves the whole batch untouched.
        foreach ($queued as $command) {
            $results[] = $this->dispatch($command[0], $command[1]);
        }
        return $results;
    }

    public function discard()
    {
        $this->pipeline = null;
        return true;
    }

    // Test inspection helpers.
    public function dump($key)
    {
        return isset($this->store[$key]) ? $this->store[$key] : null;
    }

    public function keys()
    {
        return array_keys($this->store);
    }
}
