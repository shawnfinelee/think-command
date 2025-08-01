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

trait RabbitMQClientTraits
{
    private $rabbitMQConnection;
    private $rabbitMQChannel;

    private function getRabbitMQConnection()
    {
        if (null === $this->rabbitMQConnection || !$this->rabbitMQConnection->isConnected()) {
            $configs = config('ram.rabbitmq');
            $this->rabbitMQConnection = new AMQPStreamConnection(
                $configs['host'] ?? 'localhost',
                $configs['port'] ?? 5672,
                $configs['username'] ?? 'guest',
                $configs['password'] ?? 'guest',
                $configs['vhost'] ?? '/',
                false, // insist
                'AMQPLAIN', // login_method
                null, // login_response
                'en_US', // locale
                $configs['connection_timeout'] ?? 3.0,
                $configs['read_write_timeout'] ?? 9.0,
                null, // context
                $configs['keepalive'] ?? false,
                $configs['heartbeat'] ?? 0
            );
        }

        return $this->rabbitMQConnection;
    }

    private function getRabbitMQChannel()
    {
        if (null === $this->rabbitMQChannel || !$this->rabbitMQChannel->is_open()) {
            $this->rabbitMQChannel = $this->getRabbitMQConnection()->channel();
        }

        return $this->rabbitMQChannel;
    }

    private function sendToQueue(string $exchangeName, string $routingKey, array $data, array $properties = [])
    {
        try {
            __LOG_MESSAGE($data, "sendToQueue.params__{$exchangeName}_{$routingKey}");

            $channel = $this->getRabbitMQChannel();

            // 声明交换机
            $channel->exchange_declare(
                $exchangeName,
                'direct', // type
                false,    // passive
                true,     // durable
                false     // auto_delete
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

            // 发布消息
            $result = $channel->basic_publish($message, $exchangeName, $routingKey);

            $messageId = $messageProperties['message_id'];
            __LOG_MESSAGE([
                'exchange' => $exchangeName,
                'routing_key' => $routingKey,
                'message_id' => $messageId,
                'body_length' => strlen($messageBody)
            ], 'sendToQueue.response__' . $messageId);

            return $messageId;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            return false;
        }
    }

    private function sendToQueueWithQueue(string $queueName, array $data, array $properties = [])
    {
        try {
            __LOG_MESSAGE($data, "sendToQueueWithQueue.params__{$queueName}");

            $channel = $this->getRabbitMQChannel();

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

            // 直接发布到队列
            $result = $channel->basic_publish($message, '', $queueName);

            $messageId = $messageProperties['message_id'];
            __LOG_MESSAGE([
                'queue' => $queueName,
                'message_id' => $messageId,
                'body_length' => strlen($messageBody)
            ], 'sendToQueueWithQueue.response__' . $messageId);

            return $messageId;
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            return false;
        }
    }

    private function closeRabbitMQConnection()
    {
        try {
            if ($this->rabbitMQChannel && $this->rabbitMQChannel->is_open()) {
                $this->rabbitMQChannel->close();
            }
            if ($this->rabbitMQConnection && $this->rabbitMQConnection->isConnected()) {
                $this->rabbitMQConnection->close();
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
        }
    }

    public function __destruct()
    {
        $this->closeRabbitMQConnection();
    }
}
