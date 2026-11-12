<?php
namespace Beanbun;

use Beanbun\Exception\BeanbunException;
use Beanbun\Queue\LocalQueue;
use Beanbun\Queue\TransactionalQueueInterface;
use Beanbun\Lib\Helper;
use Exception;
use GuzzleHttp\Client;
use Workerman\Lib\Timer;
use Workerman\Worker;

class Beanbun
{
    const VERSION = '1.0.5';

    public $id = null;
    public $name = null;
    public $max = 0;
    public $seed = [];
    public $daemonize = true;
    public $urlFilter = [];
    public $interval = 5;
    public $timeout = 5;
    public $userAgent = 'pc';
    public $logFile = '';
    public $commands = [];

    /**
     * Maximum number of retries for a page that fails with a hard exception.
     * 0 means the first failure writes the terminal failed state immediately.
     * BeanbunException (explicit error()/skip) is never retried.
     * @var int
     */
    public $retry = 3;

    public $queue = '';
    public $url = '';
    public $method = '';
    public $options = [];
    public $page = '';

    public $startWorker = '';
    public $beforeDownloadPage = '';
    public $downloadPage = '';
    public $afterDownloadPage = '';
    public $discoverUrl = '';
    public $afterDiscover = '';
    public $stopWorker = '';
    public $exceptionHandler = '';

    public $hooks = [
        'startWorkerHooks',
        'beforeDownloadPageHooks',
        'downloadPageHooks',
        'afterDownloadPageHooks',
        'discoverUrlHooks',
        'afterDiscoverHooks',
        'stopWorkerHooks',
    ];
    public $startWorkerHooks = [];
    public $beforeDownloadPageHooks = [];
    public $downloadPageHooks = [];
    public $afterDownloadPageHooks = [];
    public $discoverUrlHooks = [];
    public $afterDiscoverHooks = [];
    public $stopWorkerHooks = [];

    protected $queues = null;
    protected $downloader = null;
    protected $worker = null;
    protected $timer_id = null;
    protected $queueFactory = null;
    protected $queueArgs = [];
    protected $downloaderFactory = null;
    protected $downloaderArgs = [];
    protected $logFactory = null;

    // Transaction state for the page currently being processed.
    protected $batchActive = false;
    protected $queueExhausted = false;
    protected $userExceptionHandler = false;

    public static function timer($interval, $callback, $args = [], $persistent = true)
    {
        return Timer::add($interval, $callback, $args, $persistent);
    }

    public static function timerDel($time_id)
    {
        Timer::del($time_id);
    }

    public static function run()
    {
        @Worker::runAll();
    }

    public function __construct($config = [])
    {
        global $argv;
        $this->commands = $argv;
        $this->name = isset($config['name'])
        ? $config['name']
        : current(explode('.', $this->commands[0]));
        $this->logFile = isset($config['logFile']) ? $config['logFile'] : __DIR__ . '/' . $this->name . '_access.log';
        $this->setQueue();
        $this->setDownloader();
        $this->setLog();
    }

    public function command()
    {
        switch ($this->commands[1]) {
            case 'start':
                foreach ((array) $this->seed as $url) {
                    if (is_string($url)) {
                        $this->queue()->add($url);
                    } elseif (is_array($url)) {
                        $this->queue()->add($url[0], $url[1]);
                    }
                }
                $this->queues = null;
                echo "Beanbun is starting...\n";
                fclose(STDOUT);
                $STDOUT = fopen($this->logFile, "a");
                break;
            case 'clean':
                $this->queue()->clean();
                unlink($this->logFile);
                die();
                break;
            case 'stop':
                break;
            default:
                break;
        }
    }

    // 执行爬虫
    public function start()
    {
        if (!isset($this->commands[1])) {
            $this->daemonize = false;
        }

        if ($this->daemonize) {
            $this->check();

            $worker = new Worker;
            $worker->count = $this->count;
            $worker->name = $this->name;
            $worker->onWorkerStart = [$this, 'onWorkerStart'];
            $worker->onWorkerStop = [$this, 'onWorkerStop'];
            $this->worker = $worker;

            Worker::$daemonize = true;
            Worker::$stdoutFile = $this->logFile;
            \Beanbun\Lib\Db::closeAll();

            $this->queueArgs['name'] = $this->name;
            $this->initHooks();
            $this->command();

            self::run();
        } else {
            $this->initHooks();
            $this->seed = (array) $this->seed;

            // Normal mode runs against an in-process transactional queue, so
            // page processing follows the same staging/commit semantics as
            // daemon mode without requiring an external queue server.
            $this->queues = new LocalQueue(['name' => $this->name]);
            foreach ($this->seed as $url) {
                if (is_string($url)) {
                    $this->queues->add($url);
                } elseif (is_array($url)) {
                    $this->queues->add($url[0], $url[1]);
                }
            }
            $this->seed = [];

            while ($this->crawler()) {
            }
        }
    }

    public function check()
    {
        $error = false;
        $text = '';
        $version_ok = $pcntl_loaded = $posix_loaded = true;
        if (!version_compare(phpversion(), "5.3.3", ">=")) {
            $text .= "PHP Version >= 5.3.3                 \033[31;40m [fail] \033[0m\n";
            $error = true;
        }

        if (!in_array("pcntl", get_loaded_extensions())) {
            $text .= "Extension posix check                \033[31;40m [fail] \033[0m\n";
            $error = true;
        }

        if (!in_array("posix", get_loaded_extensions())) {
            $text .= "Extension posix check                \033[31;40m [fail] \033[0m\n";
            $error = true;
        }

        $check_func_map = array(
            "stream_socket_server",
            "stream_socket_client",
            "pcntl_signal_dispatch",
        );

        if ($disable_func_string = ini_get("disable_functions")) {
            $disable_func_map = array_flip(explode(",", $disable_func_string));
        }

        foreach ($check_func_map as $func) {
            if (isset($disable_func_map[$func])) {
                $text .= "\033[31;40mFunction " . implode(', ', $check_func_map) . "may be disabled. Please check disable_functions in php.ini\033[0m\n";
                $error = true;
                break;
            }
        }

        if ($error) {
            echo $text;
            exit;
        }
    }

    public function shutdown()
    {
        $master_pid = is_file(Worker::$pidFile) ? file_get_contents(Worker::$pidFile) : 0;
        $master_pid && posix_kill($master_pid, SIGINT);
        $timeout = 5;
        $start_time = time();
        while (1) {
            $master_is_alive = $master_pid && posix_kill($master_pid, 0);
            if ($master_is_alive) {
                if (time() - $start_time >= $timeout) {
                    exit;
                }
                usleep(10000);
                continue;
            }
            exit(0);
            break;
        }
    }

    public function initHooks()
    {
        $this->startWorkerHooks[] = function ($beanbun) {
            $beanbun->id = $beanbun->worker->id;
            $beanbun->log("Beanbun worker {$beanbun->id} is starting ...");
        };

        if ($this->startWorker) {
            $this->startWorkerHooks[] = $this->startWorker;
        }

        $this->startWorkerHooks[] = function ($beanbun) {
            $beanbun->queue()->maxQueueSize = $beanbun->max;
            // Put pages left in flight by a previous run back on the queue,
            // so resuming a crawl never loses dequeued-but-uncommitted pages.
            if ($beanbun->queue() instanceof TransactionalQueueInterface) {
                $recovered = $beanbun->queue()->recover();
                if ($recovered) {
                    $beanbun->log("Beanbun worker {$beanbun->id} recovered {$recovered} unfinished page(s).");
                }
            }
            $beanbun->timer_id = Beanbun::timer($beanbun->interval, [$beanbun, 'crawler']);
        };

        $this->beforeDownloadPageHooks[] = [$this, 'defaultBeforeDownloadPage'];

        if ($this->beforeDownloadPage) {
            $this->beforeDownloadPageHooks[] = $this->beforeDownloadPage;
        }

        if ($this->downloadPage) {
            $this->downloadPageHooks[] = $this->downloadPage;
        } else {
            $this->downloadPageHooks[] = [$this, 'defaultDownloadPage'];
        }

        if ($this->afterDownloadPage) {
            $this->afterDownloadPageHooks[] = $this->afterDownloadPage;
        }

        if ($this->discoverUrl) {
            $this->discoverUrlHooks[] = $this->discoverUrl;
        } elseif ($this->daemonize) {
            $this->discoverUrlHooks[] = [$this, 'defaultDiscoverUrl'];
        }

        if ($this->afterDiscover) {
            $this->afterDiscoverHooks[] = $this->afterDiscover;
        }

        if ($this->daemonize) {
            $this->afterDiscoverHooks[] = function ($beanbun) {
                // Transactional queues write the completion marker in commit(),
                // so the legacy eager queued() call is skipped here. Legacy
                // custom queues keep the original behaviour unchanged.
                if (!($beanbun->queue() instanceof TransactionalQueueInterface)
                    && $beanbun->options['reserve'] == false) {
                    $beanbun->queue()->queued($beanbun->queue);
                }
            };
        }

        if ($this->stopWorker) {
            $this->stopWorkerHooks[] = $this->stopWorker;
        }

        if (!$this->exceptionHandler) {
            $this->exceptionHandler = [$this, 'defaultExceptionHandler'];
            $this->userExceptionHandler = false;
        } else {
            $this->userExceptionHandler = true;
        }
    }

    // 爬虫进程
    public function onWorkerStart($worker)
    {
        foreach ($this->startWorkerHooks as $hook) {
            call_user_func($hook, $this);
        }
    }

    public function queue()
    {
        if ($this->queues == null) {
            $this->queues = call_user_func($this->queueFactory, $this->queueArgs);
        }
        return $this->queues;
    }

    public function setQueue($callback = null, $args = [
        'host' => '127.0.0.1',
        'port' => '2207',
    ]) {
        if ($callback === 'memory' || $callback === null) {
            $this->queueFactory = function ($args) {
                return new \Beanbun\Queue\MemoryQueue($args);
            };
        } elseif ($callback == 'redis') {
            $this->queueFactory = function ($args) {
                return new \Beanbun\Queue\RedisQueue($args);
            };
        } else {
            $this->queueFactory = $callback;
        }

        $this->queueArgs = $args;
    }

    public function downloader()
    {
        if ($this->downloader === null) {
            $this->downloader = call_user_func($this->downloaderFactory, $this->downloaderArgs);
        }
        return $this->downloader;
    }

    public function setDownloader($callback = null, $args = [])
    {
        if ($callback === null) {
            $this->downloaderFactory = function ($args) {
                return new Client($args);
            };
        } else {
            $this->downloaderFactory = $callback;
        }
        $this->downloaderArgs = $args;
    }

    public function log($msg)
    {
        call_user_func($this->logFactory, $msg, $this);
    }

    public function setLog($callback = null)
    {
        $this->logFactory = $callback === null
        ? function ($msg, $beanbun) {
            echo date('Y-m-d H:i:s') . " {$beanbun->name} : $msg\n";
        }
        : $callback;
    }

    public function error($msg = null)
    {
        throw new BeanbunException($msg === null ? '' : $msg);
    }

    /**
     * Process one page as one committable unit.
     *
     * Discovery results only land in a private staging batch; they are
     * published together with the page completion marker in a single commit.
     * Any exception rolls the uncommitted batch back. Hard exceptions requeue
     * only the current page according to the configured retry policy, and a
     * page that exhausts its retries is written to the terminal failed set.
     *
     * @return bool false when no page was available (normal mode loop ends)
     */
    public function crawler()
    {
        $this->batchActive = false;
        $this->queueExhausted = false;

        try {
            $hookGroups = $this->hooks;
            array_shift($hookGroups); // startWorkerHooks
            array_pop($hookGroups);   // stopWorkerHooks

            $queue = $this->queue();

            if ($queue instanceof TransactionalQueueInterface) {
                return $this->runTransactional($hookGroups, $queue);
            }

            // Backward-compatible path for custom queues that only implement
            // QueueInterface: keep the legacy single-try behaviour.
            return $this->runLegacy($hookGroups);
        } catch (Exception $e) {
            // Defensive net: a failure outside the inner handlers must not kill
            // the worker timer. State was never committed for this page.
            call_user_func($this->exceptionHandler, $e);
            $this->resetPageState();
            return !$this->queueExhausted;
        }
    }

    protected function runTransactional(array $hookGroups, TransactionalQueueInterface $queue)
    {
        // beforeDownloadPage: dequeue (or stop when the queue is empty).
        try {
            $this->runHookGroup('beforeDownloadPageHooks');
        } catch (BeanbunException $e) {
            // Empty queue / max limit: nothing was started. A duplicate-skip
            // (queue non-empty) still returns true so the caller keeps draining.
            $this->resetPageState();
            if ($this->queueExhausted) {
                return false;
            }
            return $this->queue()->count() > 0;
        } catch (Exception $e) {
            // A hard failure inside a user before-hook, after the page was
            // dequeued: apply the same retry/terminal policy as any later stage
            // so the page is never silently lost.
            if ($this->queue) {
                // Register in flight first so rollback()/recover() can return
                // the page to the queue even though no staging batch began.
                $queue->begin($this->queue);
                $this->batchActive = true;
                $this->handleHardFailure($queue, $e);
                $this->batchActive = false;
                $this->resetPageState();
                return true;
            }
            throw $e;
        }

        if (!$this->queue) {
            $this->resetPageState();
            return false;
        }

        // Open the staging batch. From here on, queue()->add() is buffered.
        $queue->begin($this->queue);
        $this->batchActive = true;

        try {
            foreach (['downloadPageHooks', 'afterDownloadPageHooks', 'discoverUrlHooks', 'afterDiscoverHooks'] as $group) {
                $this->runHookGroup($group);
            }

            // One atomic publish: discovered URLs + the completion marker.
            $markQueued = empty($this->options['reserve']);
            $queue->commit($markQueued);
            $this->batchActive = false;
            $this->resetPageState();
            return true;
        } catch (BeanbunException $e) {
            // Controlled skip (explicit error()): discard staged results, do
            // not requeue the page, optionally keep the completion marker.
            if ($e->getMessage()) {
                $this->log($e->getMessage());
            }
            $this->handleControlledSkip($queue);
            $this->resetPageState();
            return true;
        } catch (Exception $e) {
            // Hard failure: nothing uncommitted becomes visible.
            $this->handleHardFailure($queue, $e);
            $this->resetPageState();
            return true;
        }
    }

    protected function runLegacy(array $hookGroups)
    {
        try {
            foreach ($hookGroups as $hooks) {
                $this->runHookGroup($hooks);
            }
        } catch (Exception $e) {
            call_user_func($this->exceptionHandler, $e);
        }

        // The loop continues while the legacy queue still holds pages; an
        // explicit empty/max signal ends it immediately.
        $available = !$this->queueExhausted && $this->queue()->count() > 0;
        $this->resetPageState();
        return $available;
    }

    protected function runHookGroup($group)
    {
        foreach ($this->$group as $hook) {
            call_user_func($hook, $this);
        }
    }

    protected function handleControlledSkip(TransactionalQueueInterface $queue)
    {
        $markQueued = $this->batchActive && empty($this->options['reserve']);
        $queue->finish($markQueued);
        $this->batchActive = false;
    }

    protected function handleHardFailure(TransactionalQueueInterface $queue, Exception $e)
    {
        // Give a user-provided observer a chance to log the failure. It must
        // not change queue state: the queue owns the rollback.
        if ($this->userExceptionHandler) {
            try {
                call_user_func($this->exceptionHandler, $e);
            } catch (Exception $observerError) {
                $this->log($observerError->getMessage());
            }
        } else {
            $this->log($e->getMessage());
        }

        // Discard every staged URL and make only the current page available again.
        try {
            $queue->rollback();
        } catch (Exception $rollbackError) {
            // The page remains registered as in flight; recover() re-queues it
            // on the next run. Do not double-count this attempt.
            $this->log('Rollback failed: ' . $rollbackError->getMessage());
            $this->batchActive = false;
            return;
        }
        $this->batchActive = false;

        $attempts = $queue->incrementFailures($this->queue);
        if ($attempts > $this->retry) {
            $queue->markFailed($this->queue, $e->getMessage());
            $this->log("Page {$this->url} failed after {$attempts} attempt(s), marked as failed: " . $e->getMessage());
        } else {
            $this->log("Page {$this->url} failure {$attempts}/" . ($this->retry + 1) . ", requeued for retry: " . $e->getMessage());
        }
    }

    protected function resetPageState()
    {
        $this->queue = '';
        $this->url = '';
        $this->method = '';
        $this->page = '';
        $this->options = [];
    }

    public function onWorkerStop($worker)
    {
        foreach ($this->stopWorkerHooks as $hook) {
            call_user_func($hook, $this);
        }
    }

    public function defaultExceptionHandler(Exception $e)
    {
        if ($e instanceof BeanbunException) {
            if ($e->getMessage()) {
                $this->log($e->getMessage());
            }
        } elseif ($e instanceof Exception) {
            $this->log($e->getMessage());
            // Legacy custom-queue path only. Transactional queues handle
            // re-queueing themselves through rollback().
            if ($this->queue && !($this->queue() instanceof TransactionalQueueInterface)) {
                if ($this->daemonize) {
                    $this->queue()->add($this->queue['url'], $this->queue['options']);
                } else {
                    $this->seed[] = $this->queue;
                }
            }
        }
    }

    public function defaultBeforeDownloadPage()
    {
        $transactional = $this->queue() instanceof TransactionalQueueInterface;

        if ($this->daemonize || $transactional) {
            if ($this->max > 0 && $this->queue()->queuedCount() >= $this->max) {
                $this->log("Download to the upper limit, Beanbun worker {$this->id} stop downloading.");
                if ($this->daemonize) {
                    self::timerDel($this->timer_id);
                }
                $this->queueExhausted = true;
                $this->error();
            }

            $this->queue = $queue = $this->queue()->next();
        } else {
            // Legacy normal-mode behavior: custom QueueInterface plugins were
            // driven straight from the seed array, not the queue object.
            $queue = array_shift($this->seed);
        }

        if (is_null($queue) || !$queue) {
            // In normal mode an empty queue ends the crawl directly; the
            // 30-second blocking wait only applies to long-lived daemon workers.
            if ($this->daemonize) {
                sleep(30);
            }
            $this->queueExhausted = true;
            $this->error();
        }

        if (!is_array($queue)) {
            $this->queue = $queue = [
                'url' => $queue,
                'options' => [],
            ];
        } else {
            $this->queue = $queue;
        }

        $headers = isset($this->options['headers']) && is_array($this->options['headers'])
            ? $this->options['headers']
            : [];
        $options = array_merge([
            'headers' => $headers,
            'reserve' => false,
            'timeout' => $this->timeout,
        ], (array) $queue['options']);

        if (($this->daemonize || $transactional) && !$options['reserve'] && $this->queue()->isQueued($queue)) {
            // Already finished: no batch is opened, nothing needs rolling back.
            $this->queueExhausted = false;
            $this->error();
        }

        $this->url = $queue['url'];
        $this->method = isset($options['method']) ? $options['method'] : 'GET';
        $this->options = $options;
        if (!isset($this->options['headers']['User-Agent'])) {
            $this->options['headers']['User-Agent'] = Helper::randUserAgent($this->userAgent);
        }
    }

    public function defaultDownloadPage()
    {
        $response = $this->downloader()->request($this->method, $this->url, $this->options);
        $this->page = $response->getBody();
        if ($this->page) {
            $worker_id = isset($this->id) ? $this->worker->id : '';
            $this->log("Beanbun worker {$worker_id} download {$this->url} success.");
        } else {
            $this->error();
        }
    }

    public function defaultDiscoverUrl()
    {
        $countUrlFilter = count($this->urlFilter);
        if ($countUrlFilter === 1 && !$this->urlFilter[0]) {
            $this->error();
        }

        $urls = Helper::getUrlByHtml($this->page, $this->url);

        if ($countUrlFilter > 0) {
            foreach ($urls as $url) {
                foreach ($this->urlFilter as $urlPattern) {
                    if (preg_match($urlPattern, $url)) {
                        $this->queue()->add($url);
                    }
                }
            }
        } else {
            foreach ($urls as $url) {
                $this->queue()->add($url);
            }
        }
    }

    public function middleware($middleware, $action = 'handle')
    {
        if (is_object($middleware)) {
            $middleware->$action($this);
        } else {
            call_user_func($middleware, $this);
        }
    }
}
