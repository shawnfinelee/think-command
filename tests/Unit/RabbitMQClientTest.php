<?php

namespace Tests\Unit;

use app\example\traits\RabbitMQClientTraits;
use PHPUnit\Framework\TestCase;

/**
 * @internal
 * @coversNothing
 */
final class RabbitMQClientTest extends TestCase
{
    use RabbitMQClientTraits;

    public function testExchangeQueueSend()
    {
        $params = [
            'action' => 'send_email',
            'email' => 'test@example.com',
            'subject' => 'Test Email',
            'body' => 'This is a test email from RabbitMQ',
            't' => time(),
        ];
        
        $messageId = $this->sendToQueue('test_exchange', 'test.routing.key', $params);
        $this->assertNotFalse($messageId);
        $this->assertIsString($messageId);
    }

    public function testDirectQueueSend()
    {
        $params = [
            'action' => 'process_order',
            'order_id' => 'ORDER_' . time(),
            'amount' => 99.99,
            't' => time(),
        ];
        
        $messageId = $this->sendToQueueWithQueue('test_queue', $params);
        $this->assertNotFalse($messageId);
        $this->assertIsString($messageId);
    }

    public function testQueueBatchSend()
    {
        $successCount = 0;
        
        for ($i = 1; $i <= 10; $i++) {
            $params = [
                'action' => 'generate_report',
                'report_type' => 'daily',
                'batch_id' => $i,
                'i' => $i,
                't' => microtime(true),
            ];
            
            $messageId = $this->sendToQueue('test_exchange', 'test.routing.key', $params);
            if ($messageId !== false) {
                $successCount++;
            }
        }
        
        $this->assertEquals(10, $successCount);
        $this->assertTrue(true);
    }

    public function testMessageWithHeaders()
    {
        $params = [
            'action' => 'send_notification',
            'user_id' => 12345,
            'type' => 'system',
            'message' => 'System maintenance notification',
            't' => time(),
        ];
        
        $properties = [
            'priority' => 5,
        ];
        
        $messageId = $this->sendToQueue('test_exchange', 'notification.key', $params, $properties);
        $this->assertNotFalse($messageId);
        $this->assertIsString($messageId);
    }

    public function testEmptyMessageSend()
    {
        $params = [];
        
        $messageId = $this->sendToQueue('test_exchange', 'empty.test', $params);
        $this->assertNotFalse($messageId);
        $this->assertIsString($messageId);
    }

    public function testLargeMessageSend()
    {
        // 创建一个较大的测试消息
        $largeData = [];
        for ($i = 0; $i < 100; $i++) {
            $largeData["field_$i"] = str_repeat("data_$i", 50);
        }
        
        $params = [
            'action' => 'process_large_data',
            'data' => $largeData,
            't' => time(),
        ];
        
        $messageId = $this->sendToQueue('test_exchange', 'large.data', $params);
        $this->assertNotFalse($messageId);
        $this->assertIsString($messageId);
    }

    protected function tearDown(): void
    {
        // 确保连接被正确关闭
        $this->closeRabbitMQConnection();
        parent::tearDown();
    }
}
