<?php
namespace Beanbun\Tests;

use Beanbun\Lib\BloomFilter;

/**
 * Dependency-free twin of Beanbun\Lib\Server speaking the same length-prefixed
 * serialize() wire protocol as Beanbun\Lib\Client, including the new atomic
 * transaction commands. Lets MemoryQueue run its full regression suite over a
 * real TCP socket (and real forked workers) without Workerman.
 */
class FakeGlobalDataServer
{
    protected $data = [];
    protected $socket;
    protected $clients = [];

    public function __construct($ip = '127.0.0.1', $port = 0)
    {
        $this->socket = stream_socket_server("tcp://$ip:$port", $errno, $errstr);
        if (!$this->socket) {
            throw new \RuntimeException("fake server bind failed: $errstr");
        }
        stream_set_blocking($this->socket, false);
    }

    public function address()
    {
        $name = stream_socket_get_name($this->socket, false);
        return $name;
    }

    /**
     * Close this process's copy of the listening socket after the server
     * role was handed to a forked child.
     */
    public function detachListener()
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
            $this->socket = null;
        }
    }

    public function stop()
    {
        foreach ($this->clients as $client) {
            @fclose($client);
        }
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
        $this->socket = null;
    }

    public function poll($microseconds = 200000)
    {
        $read = array_merge([$this->socket], $this->clients);
        $write = $except = null;
        if (!@stream_select($read, $write, $except, 0, $microseconds)) {
            return;
        }
        foreach ($read as $stream) {
            if ($stream === $this->socket) {
                $client = @stream_socket_accept($this->socket, 0);
                if ($client) {
                    // The real Beanbun Client performs blocking request/reply
                    // round-trips, so accepted connections block even though
                    // the listening socket polls non-blockingly.
                    stream_set_blocking($client, true);
                    $this->clients[(int) $client] = $client;
                }
                continue;
            }
            $this->handleClient($stream);
        }
    }

    protected function handleClient($client)
    {
        $header = @fread($client, 4);
        if ($header === '' || $header === false || strlen($header) < 4) {
            unset($this->clients[(int) $client]);
            @fclose($client);
            return;
        }
        $length = unpack('Ntotal_length', $header)['total_length'];
        $body = '';
        while (strlen($body) < $length - 4) {
            $chunk = fread($client, $length - 4 - strlen($body));
            if ($chunk === '' || $chunk === false) {
                unset($this->clients[(int) $client]);
                @fclose($client);
                return;
            }
            $body .= $chunk;
        }
        $data = unserialize($body);
        if ($data === 'ping') {
            return;
        }
        $reply = $this->dispatch($data);
        $payload = serialize($reply);
        fwrite($client, pack('N', 4 + strlen($payload)) . $payload);
    }

    protected function dispatch(array $data)
    {
        $cmd = $data['cmd'];
        $key = $data['key'];
        switch ($cmd) {
            case 'get':
                return array_key_exists($key, $this->data) ? $this->data[$key] : null;
            case 'set':
                $this->data[$key] = $data['value'];
                return true;
            case 'add':
                if (array_key_exists($key, $this->data)) {
                    return false;
                }
                $this->data[$key] = $data['value'];
                return true;
            case 'cas':
                if (array_key_exists($key, $this->data) && md5(serialize($this->data[$key])) === $data['md5']) {
                    $this->data[$key] = $data['value'];
                    return true;
                }
                return false;
            case 'push':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    return false;
                }
                return array_push($this->data[$key], $data['value']);
            case 'pushIfNotExist':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    return false;
                }
                if (in_array($data['value'], $this->data[$key])) {
                    return true;
                }
                return array_push($this->data[$key], $data['value']);
            case 'pushToLeftIfNotExist':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    return false;
                }
                if (in_array($data['value'], $this->data[$key])) {
                    return true;
                }
                return array_unshift($this->data[$key], $data['value']);
            case 'shift':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    return false;
                }
                return array_shift($this->data[$key]);
            case 'pop':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    return false;
                }
                return array_pop($this->data[$key]);
            case 'count':
                return isset($this->data[$key]) && is_array($this->data[$key]) ? count($this->data[$key]) : 0;
            case 'delete':
                unset($this->data[$key]);
                return true;
            case 'bfNew':
                if (isset($this->data[$key])) {
                    return false;
                }
                $m = isset($data['value'][0]) ? $data['value'][0] : 40000;
                $k = isset($data['value'][1]) ? $data['value'][1] : 14;
                $this->data[$key] = new BloomFilter($m, $k);
                return true;
            case 'bfAdd':
                if (!isset($this->data[$key]) || !($this->data[$key] instanceof BloomFilter)) {
                    return false;
                }
                $this->data[$key]->add($data['value']);
                return true;
            case 'bfIn':
                if (!isset($this->data[$key]) || !($this->data[$key] instanceof BloomFilter)) {
                    return false;
                }
                return $this->data[$key]->maybeInSet($data['value']);
            case 'commit':
                return $this->commit($data);
            case 'setAdd':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    $this->data[$key] = [];
                }
                if (!in_array($data['value'], $this->data[$key])) {
                    $this->data[$key][] = $data['value'];
                }
                return true;
            case 'setRemove':
                if (isset($this->data[$key]) && is_array($this->data[$key])) {
                    $pos = array_search($data['value'], $this->data[$key]);
                    if ($pos !== false) {
                        unset($this->data[$key][$pos]);
                        $this->data[$key] = array_values($this->data[$key]);
                    }
                }
                return true;
            case 'setMembers':
                return isset($this->data[$key]) && is_array($this->data[$key]) ? array_values($this->data[$key]) : [];
            case 'hashIncr':
                if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
                    $this->data[$key] = [];
                }
                $field = $data['field'];
                if (!isset($this->data[$key][$field])) {
                    $this->data[$key][$field] = 0;
                }
                return $this->data[$key][$field] += isset($data['step']) ? (int) $data['step'] : 1;
            case 'hashGet':
                return (isset($this->data[$key]) && is_array($this->data[$key])
                    && array_key_exists($data['field'], $this->data[$key]))
                    ? $this->data[$key][$data['field']] : null;
            case 'hashDelete':
                if (isset($this->data[$key]) && is_array($this->data[$key])) {
                    unset($this->data[$key][$data['field']]);
                }
                return true;
        }
        throw new \RuntimeException("fake server: unknown command $cmd");
    }

    protected function commit(array $data)
    {
        $key = $data['key'];
        if (!isset($this->data[$key]) || !is_array($this->data[$key])) {
            $this->data[$key] = [];
        }
        $bloomOn = !empty($data['bloom']);
        $queuedBloom = $bloomOn && isset($this->data[$data['queuedKey']])
            && $this->data[$data['queuedKey']] instanceof BloomFilter
            ? $this->data[$data['queuedKey']] : null;
        $queuedSet = !$bloomOn && isset($this->data[$data['queuedKey']])
            && is_array($this->data[$data['queuedKey']]) ? $this->data[$data['queuedKey']] : [];

        $maxQueueSize = isset($data['maxQueueSize']) ? (int) $data['maxQueueSize'] : 0;
        $added = 0;
        foreach ((array) $data['items'] as $item) {
            if ($maxQueueSize != 0 && count($this->data[$key]) >= $maxQueueSize) {
                break;
            }
            $serialized = $item['serialized'];
            if (in_array($serialized, $this->data[$key])) {
                continue;
            }
            if ($queuedBloom && $queuedBloom->maybeInSet($item['identity'])) {
                continue;
            }
            if (!$bloomOn && in_array($serialized, $queuedSet)) {
                continue;
            }
            if (!empty($item['reserve'])) {
                array_unshift($this->data[$key], $serialized);
            } else {
                array_push($this->data[$key], $serialized);
            }
            $added++;
        }

        if (!empty($data['markQueued'])) {
            if ($queuedBloom) {
                $queuedBloom->add($data['currentIdentity']);
            } elseif (!in_array($data['currentSerialized'], $queuedSet)) {
                $this->data[$data['queuedKey']][] = $data['currentSerialized'];
            }
        }
        return ['ok' => true, 'added' => $added];
    }
}
