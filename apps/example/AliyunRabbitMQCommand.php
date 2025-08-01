<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace app\example;

use app\example\traits\AliyunRabbitMQClientTraits;
use PhpAmqpLib\Message\AMQPMessage;
use think\command\ThinkAliyunRabbitMQCommand;

/**
 * 阿里云消息队列RabbitMQ版示例命令
 *
 * 使用方法:
 * php think example:aliyun-rabbitmq
 * php think example:aliyun-rabbitmq --debug
 * php think example:aliyun-rabbitmq --thread=2
 * php think example:aliyun-rabbitmq --queue=test-queue --exchange=test-exchange --routing-key=test.key
 */
class AliyunRabbitMQCommand extends ThinkAliyunRabbitMQCommand
{
    use AliyunRabbitMQClientTraits;

    /** @var string 命令名称 */
    protected $commandName = 'example:aliyun-rabbitmq';
    /** @var string 命令描述 */
    protected $commandDescription = '阿里云消息队列RabbitMQ版示例命令';

    /** @var string 交换机名称 */
    protected $exchangeName = 'example.aliyun.exchange';
    /** @var string 队列名称 */
    protected $queueName = 'example.aliyun.queue';
    /** @var string 路由键 */
    protected $routingKey = 'example.aliyun.key';
    /** @var bool 是否允许动态覆盖队列名称 */
    protected $allowOverrideQueueName = true;
    /** @var int worker进程数量 */
    protected $workerNum = 1;

    /**
     * 消息处理逻辑
     *
     * @param string $messageId
     * @param array $json
     * @param array $headers
     * @param AMQPMessage $message
     * @param int $workerId
     *
     * @return bool 返回true表示消费成功，false表示消费失败需要重试
     */
    protected function consume(
        string $messageId,
        array $json,
        array $headers,
        AMQPMessage $message,
        int $workerId = 0
    ): bool {
        try {

            // 记录接收到的消息
            __LOG_MESSAGE([
                'message_id' => $messageId,
                'worker_id' => $workerId,
                'data' => $json,
                'headers' => $headers,
                'exchange' => $this->getExchangeName(),
                'queue' => $this->getQueueName(),
                'routing_key' => $this->getRoutingKey(),
                'timestamp' => $message->get('timestamp'),
                'aliyun_rabbitmq' => true
            ], 'AliyunRabbitMQ.consume.received');

            // 模拟业务处理
            if (isset($json['action'])) {
                switch ($json['action']) {
                    case 'send_email':
                        return $this->handleSendEmail($json, $messageId, $workerId);
                    case 'process_order':
                        return $this->handleProcessOrder($json, $messageId, $workerId);
                    case 'sync_data':
                        return $this->handleSyncData($json, $messageId, $workerId);
                    case 'test_fail':
                        // 模拟处理失败的情况
                        __LOG_MESSAGE('Simulating failure for testing', $messageId);
                        return false;
                    default:
                        __LOG_MESSAGE("Unknown action: {$json['action']}", $messageId);
                        return true; // 未知动作直接确认，避免重复消费
                }
            }

            // 默认处理逻辑
            __LOG_MESSAGE('Default processing completed', $messageId);
            return true;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            // 异常情况下返回false，让消息重试
            return false;
        }
    }

    /**
     * 处理发送邮件
     */
    private function handleSendEmail(array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'action' => 'send_email',
                'to' => $data['to'] ?? 'unknown',
                'subject' => $data['subject'] ?? 'no subject',
                'worker_id' => $workerId
            ], "AliyunRabbitMQ.handleSendEmail.{$messageId}");

            // 模拟邮件发送逻辑
            sleep(1); // 模拟耗时操作

            // 模拟发送成功/失败
            if (isset($data['force_fail']) && $data['force_fail']) {
                return false;
            }

            return true;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 处理订单处理
     */
    private function handleProcessOrder(array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'action' => 'process_order',
                'order_id' => $data['order_id'] ?? 'unknown',
                'amount' => $data['amount'] ?? 0,
                'worker_id' => $workerId
            ], "AliyunRabbitMQ.handleProcessOrder.{$messageId}");

            // 模拟订单处理逻辑
            sleep(2); // 模拟耗时操作

            return true;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 处理数据同步
     */
    private function handleSyncData(array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'action' => 'sync_data',
                'table' => $data['table'] ?? 'unknown',
                'record_id' => $data['record_id'] ?? 'unknown',
                'worker_id' => $workerId
            ], "AliyunRabbitMQ.handleSyncData.{$messageId}");

            // 模拟数据同步逻辑
            sleep(1); // 模拟耗时操作

            return true;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 发送测试消息的方法 (可用于测试)
     */
    public function sendTestMessage()
    {
        $testMessages = [
            [
                'action' => 'send_email',
                'to' => 'test@example.com',
                'subject' => 'Test Email from Aliyun RabbitMQ',
                'content' => 'This is a test message',
                'timestamp' => time()
            ],
            [
                'action' => 'process_order',
                'order_id' => 'ORD' . uniqid(),
                'amount' => 99.99,
                'customer_id' => 12345,
                'timestamp' => time()
            ],
            [
                'action' => 'sync_data',
                'table' => 'users',
                'record_id' => 12345,
                'operation' => 'update',
                'timestamp' => time()
            ]
        ];

        foreach ($testMessages as $message) {
            $messageId = $this->sendToAliyunQueue(
                $this->getExchangeName(),
                $this->getRoutingKey(),
                $message,
                [], // properties
                'direct' // AliyunRabbitMQCommand 使用 direct 类型
            );

            if ($messageId) {
                echo "Sent test message: {$messageId}\n";
            } else {
                echo "Failed to send test message\n";
            }
        }
    }
}
