<?php
namespace Beanbun\Tests;

class Assert
{
    public static $passed = 0;
    public static $failed = 0;
    public static $failures = [];

    public static function true($condition, $message)
    {
        if ($condition === true) {
            self::$passed++;
        } else {
            self::$failed++;
            self::$failures[] = $message . ' (expected true, got ' . var_export($condition, true) . ')';
            echo "  [FAIL] $message\n";
        }
    }

    public static function equals($expected, $actual, $message)
    {
        if ($expected === $actual) {
            self::$passed++;
        } else {
            self::$failed++;
            $detail = "expected " . var_export($expected, true) . ", got " . var_export($actual, true);
            self::$failures[] = "$message ($detail)";
            echo "  [FAIL] $message -- $detail\n";
        }
    }

    public static function same($expected, $actual, $message)
    {
        if ($expected == $actual) {
            self::$passed++;
        } else {
            self::$failed++;
            $detail = "expected " . var_export($expected, true) . ", got " . var_export($actual, true);
            self::$failures[] = "$message ($detail)";
            echo "  [FAIL] $message -- $detail\n";
        }
    }

    public static function throws($callback, $expectedClass, $message)
    {
        try {
            call_user_func($callback);
            self::$failed++;
            self::$failures[] = "$message (no exception thrown)";
            echo "  [FAIL] $message -- no exception thrown\n";
        } catch (\Exception $e) {
            if ($e instanceof $expectedClass) {
                self::$passed++;
            } else {
                self::$failed++;
                self::$failures[] = "$message (got " . get_class($e) . ': ' . $e->getMessage() . ')';
                echo "  [FAIL] $message -- got " . get_class($e) . "\n";
            }
        }
    }

    public static function summary()
    {
        echo "\n" . self::$passed . " passed, " . self::$failed . " failed\n";
        if (self::$failed) {
            exit(1);
        }
    }
}
