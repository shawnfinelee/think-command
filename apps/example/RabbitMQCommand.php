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

use PhpAmqpLib\Message\AMQPMessage;
use think\command\ThinkRabbitMQCommand;

/**
 * RabbitMQ队列消费示例
 * php think example:RabbitMQ --thread=2 --debug
 */
final class RabbitMQCommand extends ThinkRabbitMQCommand
{
    /** @var string 命令名称 */
    protected $commandName = 'example:RabbitMQ';
    
    /** @var string 命令描述 */
    protected $commandDescription = 'RabbitMQ queue consumer example';

    /** @var string 交换机名称 */
    protected $exchangeName = 'test_exchange';
    
    /** @var string 交换机类型 */
    protected $exchangeType = 'direct';
    
    /** @var string 队列名称 */
    protected $queueName = 'test_queue';
    
    /** @var string 路由键 */
    protected $routingKey = 'test.routing.key';
    
    /** @var bool 队列持久化 */
    protected $queueDurable = true;
    
    /** @var bool 自动确认消息 */
    protected $autoAck = false;
    
    /** @var int QoS预取数量 */
    protected $prefetchCount = 1;

    /** @var int 最大消费重试次数 */
    protected $maxConsumedTimes = 3;
    
    /** @var int 消费超时时间(秒) */
    protected $consumeTimeout = 30;
    
    /** @var bool 允许命令行参数覆盖队列名称 */
    protected $allowOverrideQueueName = true;

    /**
     * 消费消息处理
     *
     * @param string $messageId 消息ID
     * @param array $json 解析后的json数据
     * @param array $headers 消息头信息
     * @param AMQPMessage $message 原始消息对象
     * @param int $workerId worker进程ID
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
            // 打印消息信息
            $this->println('Processing message: %s', $messageId);
            $this->println('Worker ID: %d', $workerId);
            $this->println('Message data: %s', json_encode($json));
            
            // 模拟处理逻辑
            if (isset($json['action'])) {
                switch ($json['action']) {
                    case 'send_email':
                        return $this->handleSendEmail($json, $workerId);
                    default:
                        $this->addWarn("Unknown action: {$json['action']}");
                        return true; // 未知动作直接确认，避免无限重试
                }
            }
            
            // 默认处理逻辑
            $this->println('Default processing for message: %s', $messageId);
            
            // 模拟随机失败 (10%概率失败)
            if (rand(1, 10) === 1) {
                $this->addError("Random failure for message: $messageId");
                return false;
            }
            
            return true;
            
        } catch (\Exception $e) {
            $this->addError("Exception in consume: " . $e->getMessage());
            __LOG_MESSAGE($e);
            return false;
        }
    }

    /**
     * 处理发送邮件
     */
    private function handleSendEmail(array $data, int $workerId): bool
    {
        $this->println('Worker#%d: Sending email to %s', $workerId, $data['email'] ?? 'unknown');
        
        // 模拟邮件发送
        sleep(1);
        
        // 模拟失败情况
        if (empty($data['email'])) {
            $this->addError('Email address is required');
            return false;
        }
        
        $this->println('Worker#%d: Email sent successfully', $workerId);
        return true;
    }

    /**
     * 处理重试次数过多的消息
     */
    protected function triggerMaxConsumedTimes(AMQPMessage $message, int $consumedTimes = 0)
    {
        parent::triggerMaxConsumedTimes($message, $consumedTimes);
        
        // 可以在这里实现死信队列逻辑
        $messageId = $message->get('message_id') ?: uniqid();
        $this->println('Message %s exceeded max retry times(%d), should be moved to dead letter queue', 
            $messageId, $consumedTimes);
    }
}