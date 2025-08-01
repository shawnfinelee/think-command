<?php

/*
 * 阿里云RabbitMQ Topic交换机消息发送测试脚本
 */

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/apps/example/traits/AliyunRabbitMQClientTraits.php';

use app\example\traits\AliyunRabbitMQClientTraits;

class TopicMessageSender
{
    use AliyunRabbitMQClientTraits;

    public function sendTestTopicMessages()
    {
        $testMessages = [
            // 用户事件
            [
                'routing_key' => 'user.profile.updated',
                'data' => [
                    'user_id' => 12345,
                    'changes' => ['name', 'avatar'],
                    'timestamp' => time()
                ]
            ],
            [
                'routing_key' => 'user.email.verified',
                'data' => [
                    'user_id' => 12346,
                    'email' => 'user@example.com',
                    'timestamp' => time()
                ]
            ],
            [
                'routing_key' => 'user.account.created',
                'data' => [
                    'user_id' => 12347,
                    'email' => 'newuser@example.com',
                    'username' => 'newuser123',
                    'timestamp' => time()
                ]
            ],
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
            [
                'routing_key' => 'order.status.shipped',
                'data' => [
                    'order_id' => 'ORD' . uniqid(),
                    'tracking_number' => 'TRK' . uniqid(),
                    'timestamp' => time()
                ]
            ],
            // 产品事件
            [
                'routing_key' => 'product.inventory.updated',
                'data' => [
                    'product_id' => 'PROD123',
                    'old_quantity' => 100,
                    'new_quantity' => 85,
                    'timestamp' => time()
                ]
            ],
            // 系统事件
            [
                'routing_key' => 'system.alert.critical',
                'data' => [
                    'alert_type' => 'high_cpu_usage',
                    'server' => 'web-01',
                    'value' => 95.5,
                    'timestamp' => time()
                ]
            ]
        ];

        $exchangeName = 'topic.aliyun.exchange';

        echo "开始发送Topic测试消息到阿里云RabbitMQ...\n";
        echo "Exchange: {$exchangeName} (type: topic)\n";
        echo "----------------------------------------\n";

        foreach ($testMessages as $index => $message) {
            $messageId = $this->sendToAliyunQueue(
                $exchangeName,
                $message['routing_key'],
                $message['data']
            );
            
            if ($messageId) {
                echo "✅ 消息 " . ($index + 1) . " 发送成功: {$messageId}\n";
                echo "   路由键: {$message['routing_key']}\n";
                echo "   内容: " . json_encode($message['data'], JSON_UNESCAPED_UNICODE) . "\n";
            } else {
                echo "❌ 消息 " . ($index + 1) . " 发送失败\n";
                echo "   路由键: {$message['routing_key']}\n";
            }
            
            echo "----------------------------------------\n";
            // 间隔发送，避免过快
            sleep(1);
        }

        echo "Topic测试消息发送完成！\n";
        echo "\n现在你可以运行不同的消费者来接收这些消息：\n";
        echo "# 接收所有用户相关消息 (user.*)\n";
        echo "php think example:aliyun-rabbitmq-topic --routing-key=user.*\n";
        echo "\n# 接收所有用户事件 (user.#)\n";
        echo "php think example:aliyun-rabbitmq-topic --routing-key=user.#\n";
        echo "\n# 接收所有订单支付事件 (order.payment.*)\n";
        echo "php think example:aliyun-rabbitmq-topic --routing-key=order.payment.*\n";
        echo "\n# 接收所有事件 (#)\n";
        echo "php think example:aliyun-rabbitmq-topic --routing-key=#\n";
        
        // 关闭连接
        $this->closeAliyunRabbitMQConnection();
    }
}

// 模拟ThinkPHP的config函数
if (!function_exists('config')) {
    function config($key) {
        static $config = null;
        
        if ($config === null) {
            // 解析.env文件
            $envFile = __DIR__ . '/.env';
            if (file_exists($envFile)) {
                $content = file_get_contents($envFile);
                $lines = explode("\n", $content);
                $currentSection = '';
                $config = [];
                
                foreach ($lines as $line) {
                    $line = trim($line);
                    if (empty($line) || strpos($line, ';') === 0 || strpos($line, '#') === 0) {
                        continue;
                    }
                    
                    // 检查是否是section
                    if (preg_match('/^\[([^\]]+)\]$/', $line, $matches)) {
                        $currentSection = $matches[1];
                        continue;
                    }
                    
                    // 解析key=value
                    if (strpos($line, '=') !== false) {
                        list($k, $v) = explode('=', $line, 2);
                        $k = trim($k);
                        $v = trim($v, ' "\'');
                        
                        if ($currentSection) {
                            $config[$currentSection][$k] = $v;
                        } else {
                            $config[$k] = $v;
                        }
                    }
                }
            }
        }
        
        $keys = explode('.', $key);
        $result = $config;
        
        foreach ($keys as $k) {
            if (isset($result[$k])) {
                $result = $result[$k];
            } else {
                return null;
            }
        }
        
        return $result;
    }
}

// 模拟__LOG_MESSAGE函数
if (!function_exists('__LOG_MESSAGE')) {
    function __LOG_MESSAGE($message, $context = '') {
        $timestamp = date('Y-m-d H:i:s');
        if (is_array($message) || is_object($message)) {
            $message = json_encode($message, JSON_UNESCAPED_UNICODE);
        }
        if ($context) {
            echo "[{$timestamp}] {$context}: {$message}\n";
        } else {
            echo "[{$timestamp}] {$message}\n";
        }
    }
}

// 运行测试
try {
    $sender = new TopicMessageSender();
    $sender->sendTestTopicMessages();
} catch (Exception $e) {
    echo "❌ 发送消息时出错: " . $e->getMessage() . "\n";
}