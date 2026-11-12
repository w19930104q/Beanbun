<?php
namespace Beanbun\Tests;

/**
 * Minimal Guzzle-shaped downloader double: request() returns an object whose
 * getBody() yields a fixed stub page, so defaultDownloadPage works without
 * Guzzle or any network access.
 */
class FakeDownloader
{
    public $body = '<html><a href="/stub">stub</a></html>';

    public function request($method, $url, $options = [])
    {
        return new FakeResponse($this->body);
    }
}

class FakeResponse
{
    protected $body;

    public function __construct($body)
    {
        $this->body = $body;
    }

    public function getBody()
    {
        return $this->body;
    }
}
