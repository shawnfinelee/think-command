<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tests\Unit;

use app\example\AliyunRabbitMQCommand;
use app\example\traits\AliyunRabbitMQClientTraits;
use PHPUnit\Framework\TestCase;

/**
 * 阿里云RabbitMQ客户端测试
 */
class AliyunRabbitMQClientTest extends TestCase
{
    use AliyunRabbitMQClientTraits;

    Public function testSend()
    {
        $command = new AliyunRabbitMQCommand();
        $command->sendTestMessage();
    }

    public function testAliyunRabbitMQConnection()
    {
        // 测试配置检查
        $this->assertTrue(class_exists('PhpAmqpLib\Connection\AMQPStreamConnection'));
        $this->assertTrue(class_exists('PhpAmqpLib\Message\AMQPMessage'));
    }

    public function testGenerateAliyunRabbitMQUsername()
    {
        $configs = [
            'instance_id' => 'amqp-test-instance',
            'access_key' => 'test-access-key'
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQUsername');
        $method->setAccessible(true);
        
        $username = $method->invoke($this, $configs);
        $this->assertEquals('amqp-test-instance:test-access-key', $username);
    }

    public function testGenerateAliyunRabbitMQUsernameWithDirectConfig()
    {
        $configs = [
            'username' => 'direct-username',
            'instance_id' => 'amqp-test-instance',
            'access_key' => 'test-access-key'
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQUsername');
        $method->setAccessible(true);
        
        $username = $method->invoke($this, $configs);
        $this->assertEquals('direct-username', $username);
    }

    public function testGenerateAliyunRabbitMQPassword()
    {
        $configs = [
            'access_secret' => 'test-access-secret'
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQPassword');
        $method->setAccessible(true);
        
        $password = $method->invoke($this, $configs);
        $this->assertEquals('test-access-secret', $password);
    }

    public function testGenerateAliyunRabbitMQPasswordWithDirectConfig()
    {
        $configs = [
            'password' => 'direct-password',
            'access_secret' => 'test-access-secret'
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQPassword');
        $method->setAccessible(true);
        
        $password = $method->invoke($this, $configs);
        $this->assertEquals('direct-password', $password);
    }

    public function testGenerateAliyunRabbitMQUsernameException()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Aliyun RabbitMQ requires instance_id and access_key or username');
        
        $configs = [
            'instance_id' => 'amqp-test-instance'
            // missing access_key
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQUsername');
        $method->setAccessible(true);
        
        $method->invoke($this, $configs);
    }

    public function testGenerateAliyunRabbitMQPasswordException()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Aliyun RabbitMQ requires access_secret or password');
        
        $configs = [
            // missing access_secret and password
        ];
        
        $reflection = new \ReflectionClass($this);
        $method = $reflection->getMethod('generateAliyunRabbitMQPassword');
        $method->setAccessible(true);
        
        $method->invoke($this, $configs);
    }
}
