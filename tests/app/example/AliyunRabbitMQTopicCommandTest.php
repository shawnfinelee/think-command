<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tests\app\example;

use app\example\AliyunRabbitMQTopicCommand;
use app\example\traits\AliyunRabbitMQClientTraits;
use PHPUnit\Framework\TestCase;

/**
 * 阿里云RabbitMQ Topic交换机命令测试
 */
class AliyunRabbitMQTopicCommandTest extends TestCase
{
    use AliyunRabbitMQClientTraits;
    /**
     * @var AliyunRabbitMQTopicCommand
     */
    protected $command;

    protected function setUp(): void
    {
        parent::setUp();
        
        $this->command = new AliyunRabbitMQTopicCommand();
    }

    public function testSendTestTopicMessages()
    {
        // 捕获输出，测试方法执行
        $result = $this->command->sendTestTopicMessages();
        var_dump($result);
        $this->assertTrue($result);
    }

    public function testDelayTopicMessage()
    {
        // 测试延迟Topic消息发送方法
        $this->assertTrue(method_exists($this->command, 'sendTestDelayTopicMessage'));
        
        // 测试方法是否为public
        $reflection = new \ReflectionMethod($this->command, 'sendTestDelayTopicMessage');
        $this->assertTrue($reflection->isPublic());
        
        // 捕获输出，测试方法执行
        ob_start();
        $result = $this->command->sendTestDelayTopicMessage();
        $output = ob_get_clean();

        var_dump($result);
        
        // 验证输出包含预期的延迟消息信息
        $this->assertStringContainsString('Sending delayed topic message', $output);
        $this->assertStringContainsString('topic.aliyun.exchange', $output);
        $this->assertStringContainsString('user.notification.delayed', $output);
        $this->assertStringContainsString('Expected delivery time:', $output);
        
        // 验证返回结果 - 如果发送失败，应该算作测试用例失败
        $this->assertNotFalse($result, 'Delayed topic message sending failed');
        $this->assertIsString($result);
        $this->assertStringContainsString('Delayed topic message sent successfully', $output);
        $this->assertStringContainsString('Message will be delivered at:', $output);
    }
}
