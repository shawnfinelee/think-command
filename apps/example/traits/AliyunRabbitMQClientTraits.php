<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace app\example\traits;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

trait AliyunRabbitMQClientTraits
{
    private $aliyunRabbitMQConnection;
    private $aliyunRabbitMQChannel;

    private function getAliyunRabbitMQConnection()
    {
        if (null === $this->aliyunRabbitMQConnection || !$this->aliyunRabbitMQConnection->isConnected()) {
            $configs = config('ram.aliyun_rabbitmq');

            // 生成阿里云RabbitMQ认证信息
            $username = $this->generateAliyunRabbitMQUsername($configs);
            $password = $this->generateAliyunRabbitMQPassword($configs);

            $this->aliyunRabbitMQConnection = new AMQPStreamConnection(
                $configs['endpoint'] ?? $configs['host'],
                $configs['port'] ?? 5672,
                $username,
                $password,
                $configs['vhost'] ?? '/',
                false, // insist
                'PLAIN', // login_method - 阿里云RabbitMQ使用PLAIN认证
                null, // login_response
                'en_US', // locale
                $configs['connection_timeout'] ?? 10.0,
                $configs['read_write_timeout'] ?? 10.0,
                null, // context
                $configs['keepalive'] ?? false,
                $configs['heartbeat'] ?? 0
            );
        }

        return $this->aliyunRabbitMQConnection;
    }

    private function getAliyunRabbitMQChannel()
    {
        if (null === $this->aliyunRabbitMQChannel || !$this->aliyunRabbitMQChannel->is_open()) {
            $this->aliyunRabbitMQChannel = $this->getAliyunRabbitMQConnection()->channel();
        }

        return $this->aliyunRabbitMQChannel;
    }

    /**
     * 生成阿里云RabbitMQ用户名
     * 格式: ${instanceId}:${accessKey}
     *
     * @param array $configs
     * @return string
     * @throws \Exception
     */
    private function generateAliyunRabbitMQUsername(array $configs): string
    {
        if (isset($configs['username']) && !empty($configs['username'])) {
            return $configs['username'];
        }

        if (empty($configs['instance_id']) || empty($configs['access_key'])) {
            throw new \Exception('Aliyun RabbitMQ requires instance_id and access_key or username');
        }

        return $configs['instance_id'] . ':' . $configs['access_key'];
    }

    /**
     * 生成阿里云RabbitMQ密码
     * 使用AccessKey Secret
     *
     * @param array $configs
     * @return string
     * @throws \Exception
     */
    private function generateAliyunRabbitMQPassword(array $configs): string
    {
        if (isset($configs['password']) && !empty($configs['password'])) {
            return $configs['password'];
        }

        if (empty($configs['access_secret'])) {
            throw new \Exception('Aliyun RabbitMQ requires access_secret or password');
        }

        return $configs['access_secret'];
    }

    private function sendToAliyunQueue(string $exchangeName, string $routingKey, array $data, array $properties = [], string $exchangeType = 'direct')
    {
        try {
            __LOG_MESSAGE($data, "sendToAliyunQueue.params__{$exchangeName}_{$routingKey}");

            $channel = $this->getAliyunRabbitMQChannel();

            // 声明交换机 (使用passive模式，只检查交换机是否存在)
            $channel->exchange_declare(
                $exchangeName,
                $exchangeType, // 使用传入的交换机类型
                true,         // passive - 只检查交换机是否存在，不重新创建
                true,         // durable
                false         // auto_delete
            );

            // 创建消息
            $messageBody = json_encode($data, JSON_UNESCAPED_UNICODE);
            $messageProperties = [
                'message_id' => uniqid(),
                'delivery_mode' => 2, // 持久化
                'timestamp' => time(),
                'content_type' => 'application/json'
            ];

            // 合并自定义属性（但保持application_headers分离）
            $applicationHeaders = $properties['application_headers'] ?? [];
            unset($properties['application_headers']);
            $messageProperties = array_merge($messageProperties, $properties);

            // 如果有application_headers，单独设置
            if (!empty($applicationHeaders)) {
                $messageProperties['application_headers'] = $applicationHeaders;
            }

            $message = new AMQPMessage($messageBody, $messageProperties);

            // 发布消息到阿里云RabbitMQ
            $channel->basic_publish($message, $exchangeName, $routingKey);

            $messageId = $messageProperties['message_id'];
            __LOG_MESSAGE([
                'exchange' => $exchangeName,
                'routing_key' => $routingKey,
                'message_id' => $messageId,
                'body_length' => strlen($messageBody),
                'aliyun_rabbitmq' => true
            ], 'sendToAliyunQueue.response__' . $messageId);

            return $messageId;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            return false;
        }
    }

    private function sendToAliyunQueueWithQueue(string $queueName, array $data, array $properties = [])
    {
        try {
            __LOG_MESSAGE($data, "sendToAliyunQueueWithQueue.params__{$queueName}");

            $channel = $this->getAliyunRabbitMQChannel();

            // 声明队列
            $channel->queue_declare(
                $queueName,
                false,  // passive
                true,   // durable
                false,  // exclusive
                false   // auto_delete
            );

            // 创建消息
            $messageBody = json_encode($data, JSON_UNESCAPED_UNICODE);
            $messageProperties = [
                'message_id' => uniqid(),
                'delivery_mode' => 2, // 持久化
                'timestamp' => time(),
                'content_type' => 'application/json'
            ];

            // 合并自定义属性（但保持application_headers分离）
            $applicationHeaders = $properties['application_headers'] ?? [];
            unset($properties['application_headers']);
            $messageProperties = array_merge($messageProperties, $properties);

            // 如果有application_headers，单独设置
            if (!empty($applicationHeaders)) {
                $messageProperties['application_headers'] = $applicationHeaders;
            }

            $message = new AMQPMessage($messageBody, $messageProperties);

            // 直接发布到阿里云RabbitMQ队列
            $channel->basic_publish($message, '', $queueName);

            $messageId = $messageProperties['message_id'];
            __LOG_MESSAGE([
                'queue' => $queueName,
                'message_id' => $messageId,
                'body_length' => strlen($messageBody),
                'aliyun_rabbitmq' => true
            ], 'sendToAliyunQueueWithQueue.response__' . $messageId);

            return $messageId;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            return false;
        }
    }

    /**
     * 发送延迟消息到阿里云RabbitMQ
     * 阿里云RabbitMQ支持延迟消息功能
     *
     * @param string $exchangeName
     * @param string $routingKey
     * @param array $data
     * @param int $delaySeconds 延迟秒数
     * @param array $properties
     * @return string|false
     */
    private function sendToAliyunQueueWithDelay(string $exchangeName, string $routingKey, array $data, int $delaySeconds, array $properties = [], string $exchangeType = 'x-delayed-message')
    {
        try {
            __LOG_MESSAGE($data, "sendToAliyunQueueWithDelay.params__{$exchangeName}_{$routingKey}_{$delaySeconds}");

            $channel = $this->getAliyunRabbitMQChannel();

            // 声明交换机
            if ($exchangeType === 'x-delayed-message') {
                // 延迟消息交换机
                $arguments = ['x-delayed-type' => 'direct'];
                $channel->exchange_declare(
                    $exchangeName,
                    $exchangeType,
                    false,        // passive
                    true,         // durable  
                    false,        // auto_delete
                    false,        // nowait
                    $arguments
                );
            } else {
                // 普通交换机（Topic、Direct等）
                $channel->exchange_declare(
                    $exchangeName,
                    $exchangeType,
                    false,        // passive
                    true,         // durable
                    false         // auto_delete
                );
            }

            // 创建消息
            $messageBody = json_encode($data, JSON_UNESCAPED_UNICODE);
            $messageProperties = [
                'message_id' => uniqid(),
                'delivery_mode' => 2, // 持久化
                'timestamp' => time(),
                'content_type' => 'application/json'
            ];

            // 合并自定义属性（但保持application_headers分离）
            $applicationHeaders = $properties['application_headers'] ?? [];
            unset($properties['application_headers']);
            $messageProperties = array_merge($messageProperties, $properties);

            if ($exchangeType === 'x-delayed-message') {
                // 延迟消息交换机：使用x-delay头部
                $applicationHeaders['x-delay'] = ['I', $delaySeconds * 1000]; // 毫秒
                $messageProperties['application_headers'] = $applicationHeaders;
            } else {
                // 普通交换机：使用TTL实现延迟
                $messageProperties['expiration'] = (string)($delaySeconds * 1000); // 毫秒，必须是字符串
                if (!empty($applicationHeaders)) {
                    $messageProperties['application_headers'] = $applicationHeaders;
                }
            }

            $message = new AMQPMessage($messageBody, $messageProperties);

            // 发布延迟消息到阿里云RabbitMQ
            $channel->basic_publish($message, $exchangeName, $routingKey);

            $messageId = $messageProperties['message_id'];
            __LOG_MESSAGE([
                'exchange' => $exchangeName,
                'routing_key' => $routingKey,
                'message_id' => $messageId,
                'delay_seconds' => $delaySeconds,
                'body_length' => strlen($messageBody),
                'aliyun_rabbitmq' => true
            ], 'sendToAliyunQueueWithDelay.response__' . $messageId);

            return $messageId;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            return false;
        }
    }

    protected function closeAliyunRabbitMQConnection()
    {
        try {
            if ($this->aliyunRabbitMQChannel && $this->aliyunRabbitMQChannel->is_open()) {
                $this->aliyunRabbitMQChannel->close();
            }
            if ($this->aliyunRabbitMQConnection && $this->aliyunRabbitMQConnection->isConnected()) {
                $this->aliyunRabbitMQConnection->close();
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
        }
    }

    public function __destruct()
    {
        $this->closeAliyunRabbitMQConnection();
    }
}
