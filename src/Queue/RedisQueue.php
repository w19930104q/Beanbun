<?php
namespace Beanbun\Queue;

class RedisQueue implements TransactionalQueueInterface
{
    public $redis = null;
    public $config = [];
    public $maxQueueSize = 10000;
    public $maxQueuedCount = 0;
    public $bloomFilter = true;

    protected $name = '';
    protected $key = '';
    protected $queuedKey = '';
    protected $inFlightKey = '';
    protected $retryKey = '';
    protected $failedKey = '';
    protected $algorithm = 'depth';

    protected $bfSize;
    protected $bfHashCount;

    // Per-worker staging state. Nothing here is visible to other workers.
    protected $active = false;
    protected $currentQueue = null;
    protected $currentIdentity = null;
    protected $currentSerialized = null;
    protected $staged = [];
    protected $queuedFlag = false;

    public function __construct($config)
    {
        $this->config = $config;
        $this->name = $config['name'];
        $this->key = $config['name'] . 'Queue';
        $this->queuedKey = $config['name'] . 'Queued';
        $this->inFlightKey = $config['name'] . 'InFlight';
        $this->retryKey = $config['name'] . 'Retry';
        $this->failedKey = $config['name'] . 'Failed';
        $this->bfSize = isset($config['size']) ? $config['size'] : 400000;
        $this->bfHashCount = isset($config['hash_count']) ? $config['hash_count'] : 14;
        if (isset($config['bloomFilter']) && !$config['bloomFilter']) {
            $this->bloomFilter = false;
        }

        if (isset($config['algorithm'])) {
            $this->algorithm = $config['algorithm'] != 'breadth' ? 'depth' : 'breadth';
        }
        $this->getInstance()->sAdd('beanbun', $this->name);
    }

    public function getInstance()
    {
        if (!$this->redis) {
            // Seam for custom / fake Redis-compatible clients (also useful for
            // unit tests and connection pools).
            if (isset($this->config['connection']) && is_object($this->config['connection'])) {
                $this->redis = $this->config['connection'];
                return $this->redis;
            }
            $this->redis = new \Redis();
	        $this->redis->connect($this->config['host'], $this->config['port']);
	        if(isset($this->config['auth'])) {
	            $this->redis->auth($this->config['auth']);
	        }
	        if (isset($this->config['db'])) {
	            $this->redis->select($this->config['db']);
	        }
	        if (isset($this->config['prefix'])) {
	            $this->redis->setOption(\Redis::OPT_PREFIX, $this->config['prefix'].":");
	        }

	        $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, -1);
        }
        return $this->redis;
    }

    protected function identity(array $queue)
    {
        return md5(serialize($queue));
    }

    protected function normalize($queue)
    {
        if (!is_array($queue)) {
            $queue = ['url' => $queue, 'options' => []];
        }
        if (!isset($queue['options'])) {
            $queue['options'] = [];
        }
        return $queue;
    }

    public function add($url, $options = [])
    {
        if (!$url) {
            return;
        }

        $item = [
            'url' => $url,
            'options' => (array) $options,
        ];
        $identity = $this->identity($item);

        if ($this->isQueued($identity)) {
            return;
        }

        if ($this->active) {
            // Private staging; shared Redis keys are untouched until commit().
            if (!isset($this->staged[$identity])) {
                $this->staged[$identity] = $item;
            }
            return;
        }

        if ($this->maxQueueSize != 0 && $this->count() >= $this->maxQueueSize) {
            return;
        }

        $queue = serialize($item);
        if (!isset($options['reserve']) || $options['reserve'] == false) {
            $this->getInstance()->rPush($this->key, $queue);
        } else {
            $this->getInstance()->lPush($this->key, $queue);
        }
    }

    public function next()
    {
        $size = $this->count();
        $skipped = 0;
        while ($skipped < $size) {
            if ($this->algorithm == 'depth') {
                $queue = $this->getInstance()->lPop($this->key);
            } else {
                $queue = $this->getInstance()->rPop($this->key);
            }

            if (!$queue) {
                return null;
            }

            if ($this->isQueued($queue)) {
                // Drop stale finished entries instead of recursing forever.
                $skipped++;
                continue;
            }

            return unserialize($queue);
        }
        return null;
    }

    public function count()
    {
        return $this->getInstance()->lSize($this->key);
    }

    public function queued($queue)
    {
        if ($this->active) {
            // Deferred to commit().
            $this->queuedFlag = true;
            return;
        }

        $queue = $this->normalize($queue);
        if ($this->bloomFilter) {
            $this->bfAdd($this->identity($queue));
        } else {
            $this->getInstance()->sAdd($this->queuedKey, serialize($queue));
        }
    }

    public function isQueued($queue)
    {
        if ($queue === null || $queue === false || $queue === '') {
            return false;
        }
        if ($this->bloomFilter) {
            $identity = is_array($queue) ? $this->identity($this->normalize($queue)) : (string) $queue;
            return $this->bfHas($identity);
        } else {
            if (is_array($queue)) {
                $queue = serialize($this->normalize($queue));
            }
            return $this->getInstance()->sIsMember($this->queuedKey, $queue);
        }
    }

    public function queuedCount()
    {
        if ($this->bloomFilter) {
            return 0;
        } else {
            return $this->getInstance()->sSize($this->queuedKey);
        }
    }

    public function begin(array $queue)
    {
        $this->currentQueue = $this->normalize($queue);
        $this->currentSerialized = serialize($this->currentQueue);
        $this->currentIdentity = $this->identity($this->currentQueue);
        $this->staged = [];
        $this->queuedFlag = false;
        $this->active = true;

        // In-flight registration enables recover() after a hard crash.
        $this->getInstance()->sAdd($this->inFlightKey, $this->currentSerialized);
    }

    public function commit($markQueued = true)
    {
        if (!$this->active) {
            return;
        }

        $redis = $this->getInstance();
        try {
            // Membership prechecks must happen before the pipeline opens,
            // otherwise phpredis would queue the reads instead of answering.
            $pending = [];
            $capacity = $this->maxQueueSize != 0
                ? max(0, $this->maxQueueSize - $this->count())
                : PHP_INT_MAX;
            foreach ($this->staged as $identity => $item) {
                if (count($pending) >= $capacity) {
                    break;
                }
                $serialized = serialize($item);
                if ($this->bloomFilter) {
                    if ($this->bfHas($identity)) {
                        continue;
                    }
                } elseif ($redis->sIsMember($this->queuedKey, $serialized)) {
                    continue;
                }
                $pending[] = $serialized;
            }

            $redis->multi(\Redis::PIPELINE);
            foreach ($pending as $serialized) {
                $item = unserialize($serialized);
                if (!empty($item['options']['reserve'])) {
                    $redis->lPush($this->key, $serialized);
                } else {
                    $redis->rPush($this->key, $serialized);
                }
            }

            if ($markQueued || $this->queuedFlag) {
                if ($this->bloomFilter) {
                    $this->bfAddPipeline($redis, $this->currentIdentity);
                } else {
                    $redis->sAdd($this->queuedKey, $this->currentSerialized);
                }
            }
            $redis->sRem($this->inFlightKey, $this->currentSerialized);
            $redis->hDel($this->retryKey, $this->currentIdentity);
            $redis->exec();
            $this->resetBatch();
        } catch (\Exception $e) {
            // Discard the open pipeline and surface the failure: nothing was
            // committed, so the caller can roll back and retry the page.
            try {
                $redis->discard();
            } catch (\Exception $inner) {
            }
            throw $e;
        }
    }

    public function rollback()
    {
        if (!$this->active) {
            return;
        }
        $this->staged = [];
        $this->requeueCurrent();
        $this->resetBatch();
    }

    public function finish($markQueued = false)
    {
        if (!$this->active) {
            return;
        }
        $this->staged = [];
        $redis = $this->getInstance();
        if ($markQueued || $this->queuedFlag) {
            if ($this->bloomFilter) {
                $this->bfAdd($this->currentIdentity);
            } else {
                $redis->sAdd($this->queuedKey, $this->currentSerialized);
            }
        }
        $redis->sRem($this->inFlightKey, $this->currentSerialized);
        $this->resetBatch();
    }

    protected function requeueCurrent()
    {
        if ($this->currentSerialized === null) {
            return;
        }
        $redis = $this->getInstance();
        $redis->sRem($this->inFlightKey, $this->currentSerialized);
        if ($this->isQueued($this->currentIdentity)) {
            return;
        }
        // Front of the queue: retries are picked up promptly.
        $redis->lPush($this->key, $this->currentSerialized);
    }

    protected function resetBatch()
    {
        $this->active = false;
        $this->currentQueue = null;
        $this->currentIdentity = null;
        $this->currentSerialized = null;
        $this->staged = [];
        $this->queuedFlag = false;
    }

    public function failureCount(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        $value = $this->getInstance()->hGet($this->retryKey, $identity);
        return $value === false ? 0 : (int) $value;
    }

    public function incrementFailures(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        return (int) $this->getInstance()->hIncrBy($this->retryKey, $identity, 1);
    }

    public function markFailed(array $queue, $reason = '')
    {
        $queue = $this->normalize($queue);
        $identity = $this->identity($queue);
        $serialized = serialize($queue);
        $redis = $this->getInstance();

        $redis->sAdd($this->failedKey, serialize([
            'queue' => $serialized,
            'identity' => $identity,
            'reason' => (string) $reason,
            'time' => time(),
        ]));
        $redis->sRem($this->inFlightKey, $serialized);
        $redis->hDel($this->retryKey, $identity);
        // Terminal identity: never dequeued again.
        if ($this->bloomFilter) {
            $this->bfAdd($identity);
        } else {
            $redis->sAdd($this->queuedKey, $serialized);
        }
        if ($this->active) {
            $this->resetBatch();
        }
    }

    public function recover()
    {
        $redis = $this->getInstance();
        $recovered = 0;
        foreach ((array) $redis->sMembers($this->inFlightKey) as $serialized) {
            if (!$serialized) {
                continue;
            }
            if ($this->bloomFilter) {
                $alreadyQueued = $this->bfHas(md5($serialized));
            } else {
                $alreadyQueued = $redis->sIsMember($this->queuedKey, $serialized);
            }
            if ($alreadyQueued) {
                $redis->sRem($this->inFlightKey, $serialized);
                continue;
            }
            $redis->lPush($this->key, $serialized);
            $redis->sRem($this->inFlightKey, $serialized);
            $recovered++;
        }
        return $recovered;
    }

    public function clean()
    {
        $redis = $this->getInstance();
        $redis->delete($this->key);
        $redis->delete($this->queuedKey);
        $redis->delete($this->inFlightKey);
        $redis->delete($this->retryKey);
        $redis->delete($this->failedKey);
        $redis->sRem('beanbun', $this->name);
        $this->resetBatch();
    }

    protected function bfAdd($item)
    {
        $index = 0;
        $pipe = $this->getInstance()->pipeline();
        while ($index < $this->bfHashCount) {
            $crc = $this->hash($item, $index);
            $pipe->setbit($this->queuedKey, $crc, 1);
            $index++;
        }
        $pipe->exec();
    }

    protected function bfAddPipeline($pipe, $item)
    {
        $index = 0;
        while ($index < $this->bfHashCount) {
            $crc = $this->hash($item, $index);
            $pipe->setbit($this->queuedKey, $crc, 1);
            $index++;
        }
    }

    protected function bfHas($item)
    {
        $index = 0;
        $pipe = $this->getInstance()->pipeline();
        while ($index < $this->bfHashCount) {
            $crc = $this->hash($item, $index);
            $pipe->getbit($this->queuedKey, $crc);
            $index++;
        }
        $result = $pipe->exec();
        return !in_array(0, $result);
    }

    protected function hash($item, $index)
    {
        return abs(crc32(md5('m' . $index . $item))) % $this->bfSize;
    }
}
