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
 * 阿里云消息队列RabbitMQ版Topic交换机示例命令
 *
 * Topic交换机特点:
 * - 支持通配符路由
 * - * 匹配一个单词
 * - # 匹配零个或多个单词
 * - 例如: user.*.created 可以匹配 user.email.created, user.profile.created
 * - 例如: order.# 可以匹配 order.created, order.payment.success
 *
 * 使用方法:
 * php think example:aliyun-rabbitmq-topic
 * php think example:aliyun-rabbitmq-topic --debug
 * php think example:aliyun-rabbitmq-topic --thread=2
 * php think example:aliyun-rabbitmq-topic --queue=user-queue --exchange=topic-exchange --routing-key=user.*
 */
class AliyunRabbitMQTopicCommand extends ThinkAliyunRabbitMQCommand
{
    use AliyunRabbitMQClientTraits;

    /** @var string 命令名称 */
    protected $commandName = 'example:aliyun-rabbitmq-topic';
    /** @var string 命令描述 */
    protected $commandDescription = '阿里云消息队列RabbitMQ版Topic交换机示例命令';

    /** @var string 交换机名称 */
    protected $exchangeName = 'topic.aliyun.exchange';
    /** @var string 交换机类型 - Topic类型支持通配符路由 */
    protected $exchangeType = 'topic';
    /** @var string 队列名称 */
    protected $queueName = 'topic.aliyun.queue';
    /** @var string 路由键 - 支持通配符 * 和 # */
    protected $routingKey = 'user.#';  // 匹配所有以user.开头的路由键
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
            // 获取消息的实际路由键
            $actualRoutingKey = $message->getRoutingKey();
            
            // 记录接收到的消息
            __LOG_MESSAGE([
                'message_id' => $messageId,
                'worker_id' => $workerId,
                'data' => $json,
                'headers' => $headers,
                'exchange' => $this->getExchangeName(),
                'exchange_type' => $this->exchangeType,
                'queue' => $this->getQueueName(),
                'routing_key_pattern' => $this->getRoutingKey(),
                'actual_routing_key' => $actualRoutingKey,
                'timestamp' => $message->get('timestamp'),
                'aliyun_rabbitmq' => true
            ], 'AliyunRabbitMQTopic.consume.received');

            // 根据路由键进行不同的处理
            return $this->handleByRoutingKey($actualRoutingKey, $json, $messageId, $workerId);

        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            // 异常情况下返回false，让消息重试
            return false;
        }
    }

    /**
     * 根据路由键处理不同类型的消息
     *
     * @param string $routingKey
     * @param array $data
     * @param string $messageId
     * @param int $workerId
     * @return bool
     */
    private function handleByRoutingKey(string $routingKey, array $data, string $messageId, int $workerId): bool
    {
        $routingParts = explode('.', $routingKey);
        $category = $routingParts[0] ?? 'unknown';
        $action = $routingParts[1] ?? 'unknown';
        $event = $routingParts[2] ?? 'unknown';

        __LOG_MESSAGE([
            'routing_key' => $routingKey,
            'category' => $category,
            'action' => $action,
            'event' => $event,
            'worker_id' => $workerId
        ], "AliyunRabbitMQTopic.handleByRoutingKey.{$messageId}");

        switch ($category) {
            case 'user':
                return $this->handleUserEvent($action, $event, $data, $messageId, $workerId);
            case 'order':
                return $this->handleOrderEvent($action, $event, $data, $messageId, $workerId);
            case 'product':
                return $this->handleProductEvent($action, $event, $data, $messageId, $workerId);
            case 'system':
                return $this->handleSystemEvent($action, $event, $data, $messageId, $workerId);
            default:
                __LOG_MESSAGE("Unknown category: {$category}", $messageId);
                return true; // 未知类别直接确认，避免重复消费
        }
    }

    /**
     * 处理用户相关事件
     * 路由键示例: user.profile.updated, user.email.verified, user.account.created
     */
    private function handleUserEvent(string $action, string $event, array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'category' => 'user',
                'action' => $action,
                'event' => $event,
                'user_id' => $data['user_id'] ?? 'unknown',
                'worker_id' => $workerId
            ], "AliyunRabbitMQTopic.handleUserEvent.{$messageId}");

            switch ("{$action}.{$event}") {
                case 'profile.updated':
                    return $this->processUserProfileUpdate($data, $messageId, $workerId);
                case 'email.verified':
                    return $this->processUserEmailVerification($data, $messageId, $workerId);
                case 'account.created':
                    return $this->processUserAccountCreation($data, $messageId, $workerId);
                case 'password.changed':
                    return $this->processUserPasswordChange($data, $messageId, $workerId);
                default:
                    __LOG_MESSAGE("Unknown user event: {$action}.{$event}", $messageId);
                    return true;
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 处理订单相关事件
     * 路由键示例: order.payment.success, order.status.shipped, order.refund.requested
     */
    private function handleOrderEvent(string $action, string $event, array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'category' => 'order',
                'action' => $action,
                'event' => $event,
                'order_id' => $data['order_id'] ?? 'unknown',
                'worker_id' => $workerId
            ], "AliyunRabbitMQTopic.handleOrderEvent.{$messageId}");

            switch ("{$action}.{$event}") {
                case 'payment.success':
                    return $this->processOrderPaymentSuccess($data, $messageId, $workerId);
                case 'status.shipped':
                    return $this->processOrderShipped($data, $messageId, $workerId);
                case 'refund.requested':
                    return $this->processOrderRefundRequest($data, $messageId, $workerId);
                default:
                    __LOG_MESSAGE("Unknown order event: {$action}.{$event}", $messageId);
                    return true;
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 处理产品相关事件
     * 路由键示例: product.inventory.updated, product.price.changed
     */
    private function handleProductEvent(string $action, string $event, array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'category' => 'product',
                'action' => $action,
                'event' => $event,
                'product_id' => $data['product_id'] ?? 'unknown',
                'worker_id' => $workerId
            ], "AliyunRabbitMQTopic.handleProductEvent.{$messageId}");

            switch ("{$action}.{$event}") {
                case 'inventory.updated':
                    return $this->processProductInventoryUpdate($data, $messageId, $workerId);
                case 'price.changed':
                    return $this->processProductPriceChange($data, $messageId, $workerId);
                default:
                    __LOG_MESSAGE("Unknown product event: {$action}.{$event}", $messageId);
                    return true;
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    /**
     * 处理系统相关事件
     * 路由键示例: system.maintenance.start, system.alert.critical
     */
    private function handleSystemEvent(string $action, string $event, array $data, string $messageId, int $workerId): bool
    {
        try {
            __LOG_MESSAGE([
                'category' => 'system',
                'action' => $action,
                'event' => $event,
                'worker_id' => $workerId
            ], "AliyunRabbitMQTopic.handleSystemEvent.{$messageId}");

            switch ("{$action}.{$event}") {
                case 'maintenance.start':
                    return $this->processSystemMaintenanceStart($data, $messageId, $workerId);
                case 'alert.critical':
                    return $this->processSystemCriticalAlert($data, $messageId, $workerId);
                default:
                    __LOG_MESSAGE("Unknown system event: {$action}.{$event}", $messageId);
                    return true;
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e, $messageId);
            return false;
        }
    }

    // ========== 具体业务处理方法 ==========

    private function processUserProfileUpdate(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing user profile update...', $messageId);
        sleep(1); // 模拟处理时间
        return true;
    }

    private function processUserEmailVerification(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing user email verification...', $messageId);
        sleep(1);
        return true;
    }

    private function processUserAccountCreation(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing user account creation...', $messageId);
        sleep(2);
        return true;
    }

    private function processUserPasswordChange(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing user password change...', $messageId);
        sleep(1);
        return true;
    }

    private function processOrderPaymentSuccess(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing order payment success...', $messageId);
        sleep(2);
        return true;
    }

    private function processOrderShipped(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing order shipped...', $messageId);
        sleep(1);
        return true;
    }

    private function processOrderRefundRequest(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing order refund request...', $messageId);
        sleep(2);
        return true;
    }

    private function processProductInventoryUpdate(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing product inventory update...', $messageId);
        sleep(1);
        return true;
    }

    private function processProductPriceChange(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing product price change...', $messageId);
        sleep(1);
        return true;
    }

    private function processSystemMaintenanceStart(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing system maintenance start...', $messageId);
        sleep(1);
        return true;
    }

    private function processSystemCriticalAlert(array $data, string $messageId, int $workerId): bool
    {
        __LOG_MESSAGE('Processing system critical alert...', $messageId);
        sleep(1);
        return true;
    }

    /**
     * 发送测试消息的方法 (可用于测试Topic交换机)
     */
    public function sendTestTopicMessages()
    {
        $testMessages = [
            // 用户事件
            //[
            //    'routing_key' => 'user.profile.updated',
            //    'data' => [
            //        'user_id' => 12345,
            //        'changes' => ['name', 'avatar'],
            //        'timestamp' => time()
            //    ]
            //],
            //[
            //    'routing_key' => 'user.email.verified',
            //    'data' => [
            //        'user_id' => 12346,
            //        'email' => 'user@example.com',
            //        'timestamp' => time()
            //    ]
            //],
            // 订单事件
            [
                'routing_key' => 'order.payment.success',
                'data' => [
                    'order_id' => 'ORD' . uniqid(),
                    'amount' => 99.99,
                    'payment_method' => 'credit_card',
                    'timestamp' => time()
                ]
            ],
        ];

        foreach ($testMessages as $message) {
            $messageId = $this->sendToAliyunQueue(
                $this->getExchangeName(),
                $message['routing_key'],
                $message['data'],
                [], // properties
                $this->exchangeType // 传入交换机类型
            );
            
            if ($messageId) {
                echo "✅ Sent message to {$message['routing_key']}: {$messageId}\n";
            } else {
                echo "❌ Failed to send message to {$message['routing_key']}\n";
                return false;
            }
            
            sleep(1); // 间隔发送
        }
        return true;
    }

    /**
     * 获取命令名称 (用于测试)
     */
    public function getCommandName(): string
    {
        return $this->commandName;
    }

    /**
     * 获取命令描述 (用于测试)
     */
    public function getCommandDescription(): string
    {
        return $this->commandDescription;
    }

    /**
     * 发送延迟Topic消息测试方法
     */
    public function sendTestDelayTopicMessage()
    {
        // 使用当前配置的Topic交换机发送延迟消息
        $delayExchangeName = $this->getExchangeName(); // 'topic.aliyun.exchange'
        $delaySeconds = 10; // 延迟10秒
        
        $testMessage = [
            'routing_key' => 'user.notification.delayed',
            'data' => [
                'user_id' => 12345,
                'message' => 'This is a delayed topic message',
                'delay_seconds' => $delaySeconds,
                'sent_at' => time(),
                'expected_delivery' => time() + $delaySeconds
            ]
        ];

        echo "🕐 Sending delayed topic message (delay: {$delaySeconds}s)...\n";
        echo "Exchange: {$delayExchangeName}\n";
        echo "RoutingKey: {$testMessage['routing_key']}\n";
        echo "Expected delivery time: " . date('Y-m-d H:i:s', $testMessage['data']['expected_delivery']) . "\n";
        echo "----------------------------------------\n";

        // 阿里云RabbitMQ延迟消息：使用Topic交换机 + TTL实现
        $messageId = $this->sendToAliyunQueueWithDelay(
            $delayExchangeName,
            $testMessage['routing_key'],
            $testMessage['data'],
            $delaySeconds,
            [], // properties
            'topic' // 使用普通Topic交换机
        );
        
        if ($messageId) {
            echo "✅ Delayed topic message sent successfully: {$messageId}\n";
            echo "📅 Message will be delivered at: " . date('Y-m-d H:i:s', time() + $delaySeconds) . "\n";
        } else {
            echo "❌ Failed to send delayed topic message\n";
        }
        
        return $messageId;
    }
}
