<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace think\command;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use think\Config;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;

/**
 * ThinkAliyunRabbitMQCommand - 阿里云消息队列RabbitMQ版
 *
 * @author  lei.tian <whereismoney@qq.com>
 * @since   2024-01-01
 * @version 1.0
 */
abstract class ThinkAliyunRabbitMQCommand extends ThinkCommand
{
    /** @var string 交换机名称 */
    protected $exchangeName;
    /** @var string 交换机类型 */
    protected $exchangeType = 'direct';
    /** @var string 队列名称 */
    protected $queueName;
    /** @var string 路由键 */
    protected $routingKey = '';
    /** @var bool 队列是否持久化 */
    protected $queueDurable = true;
    /** @var bool 消息是否自动确认 */
    protected $autoAck = false;
    /** @var int 每次获取消息数量(QoS) */
    protected $prefetchCount = 1;
    // ----------
    /** @var int 重试到第N次消费,就删除 */
    protected $maxConsumedTimes = 5;
    /** @var int 消费超时时间(秒) */
    protected $consumeTimeout = 600;
    /** @var bool 数据格式 是否是 a=1&b=2格式 */
    protected $queueMessageIsParseStr = false;
    /** @var bool 是否允许动态覆盖队列名称 */
    protected $allowOverrideQueueName = false;
    // ----------
    /** @var array 配置 */
    private $configs;
    /** @var AMQPStreamConnection 连接 */
    private $connection;
    /** @var \PhpAmqpLib\Channel\AMQPChannel 信道 */
    private $channel;

    /**
     * 命令行参数配置
     *
     * @return array
     */
    protected function buildCommandDefinition(): array
    {
        return [
            new Option('queue', null, Option::VALUE_OPTIONAL, 'Override queue name'),
            new Option('exchange', null, Option::VALUE_OPTIONAL, 'Override exchange name'),
            new Option('routing-key', null, Option::VALUE_OPTIONAL, 'Override routing key'),
        ];
    }

    /**
     * 动态配置队列相关名称
     */
    protected function dynamicOverrideQueueNames(Input $input)
    {
        if (!$this->allowOverrideQueueName) {
            return;
        }
        $queueName = $input->getOption('queue');
        empty($queueName) or $this->setQueueName($queueName);
        $exchangeName = $input->getOption('exchange');
        empty($exchangeName) or $this->setExchangeName($exchangeName);
        $routingKey = $input->getOption('routing-key');
        empty($routingKey) or $this->setRoutingKey($routingKey);
    }

    /**
     * 主函数
     *
     * @param Input $input
     * @param Output $output
     *
     * @throws \Exception
     */
    protected function main(Input $input, Output $output)
    {
        // 检查php-amqplib依赖
        if (!class_exists('PhpAmqpLib\Connection\AMQPStreamConnection')) {
            throw new \Exception('php-amqplib library is required. Please install it via composer: composer require php-amqplib/php-amqplib');
        }

        // 动态配置队列相关名称
        $this->dynamicOverrideQueueNames($input);
        // 显示配置信息
        foreach (
            [
                "WorkerNum: <info>{$this->workerNum}</info>",
                "Endpoint: <info>{$this->getEndpoint()}</info>",
                "Vhost: <info>{$this->getVhost()}</info>",
                "Exchange: <info>{$this->getExchangeName()}</info> ({$this->exchangeType})",
                "Queue: <info>{$this->getQueueName()}</info>",
                "RoutingKey: <info>{$this->getRoutingKey()}</info>",
                "AutoAck: <info>" . ($this->autoAck ? 'true' : 'false') . "</info>",
            ] as $s
        ) {
            $output->comment($s);
            __LOG_MESSAGE(strip_tags($s));
        }
        // 使用Swoole\Process\Pool
        if ($this->workerNum > 1) {
            $this->startSwoolePoolWorkers();
        } else {
            // 单进程
            $this->onWorkerCallback();
        }
    }

    /**
     * 接收消息
     *
     * @param int $workerId
     */
    public function onWorkerCallback(int $workerId = 0)
    {
        $output = $this->output;
        $this->setupAliyunRabbitMQConnection();

        try {
            // 声明交换机
            $this->channel->exchange_declare(
                $this->exchangeName,
                $this->exchangeType,
                false,  // passive
                true,   // durable
                false   // auto_delete
            );

            // 声明队列
            $this->channel->queue_declare(
                $this->queueName,
                false,               // passive
                $this->queueDurable, // durable
                false,               // exclusive
                false                // auto_delete
            );

            // 绑定队列到交换机
            $this->channel->queue_bind(
                $this->queueName,
                $this->exchangeName,
                $this->routingKey
            );

            // 设置QoS
            $this->channel->basic_qos(0, $this->prefetchCount, false);

            // 定义消息处理回调
            $callback = function (AMQPMessage $message) use ($output, $workerId) {
                $this->processMessage($message, $output, $workerId);
            };

            // 开始消费
            $this->channel->basic_consume(
                $this->queueName,
                '',           // consumer_tag
                false,        // no_local
                $this->autoAck, // auto_ack
                false,        // exclusive
                false,        // nowait
                $callback
            );

            $output->writeln("Worker#$workerId waiting for messages from Aliyun RabbitMQ. To exit press CTRL+C");
            __LOG_MESSAGE("Worker#$workerId waiting for messages from Aliyun RabbitMQ");

            // 持续消费消息
            while ($this->channel->is_consuming()) {
                $this->channel->wait(null, false, $this->consumeTimeout);
                echo '.';
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            $output->error($e->getMessage());
        } finally {
            $this->closeAliyunRabbitMQConnection();
        }
    }

    /**
     * 处理单条消息
     *
     * @param AMQPMessage $message
     * @param Output $output
     * @param int $workerId
     */
    protected function processMessage(AMQPMessage $message, Output $output, int $workerId = 0)
    {
        $messageId = $message->get('message_id') ?: uniqid();
        $deliveryTag = $message->getDeliveryTag();

        // 获取重试次数 (从消息头中获取，如果没有则为第一次)
        $headers = [];
        if ($message->has('application_headers')) {
            $applicationHeaders = $message->get('application_headers');
            if ($applicationHeaders && method_exists($applicationHeaders, 'getNativeData')) {
                $headers = $applicationHeaders->getNativeData();
            }
        }
        $consumedTimes = isset($headers['x-retry-count']) ? (int)$headers['x-retry-count'] + 1 : 1;

        $s = "$messageId" . ($consumedTimes > 1 ? "($consumedTimes)" : '') . ' ';
        $output->write("#$workerId MessageId $s");
        __LOG_MESSAGE($s, "#$workerId MessageId");
        unset($s);

        try {
            // 处理最大消费失败
            if ($consumedTimes >= $this->maxConsumedTimes) {
                $this->triggerMaxConsumedTimes($message, $consumedTimes);
                if (!$this->autoAck) {
                    $this->channel->basic_ack($deliveryTag);
                }
                $output->warning('MAX_CONSUMED_TIMES');
                __LOG_MESSAGE('status = MAX_CONSUMED_TIMES', $messageId);
                return;
            }

            // 获取数组信息
            $json = $this->getMessageBodyJson($message);

            // 数据格式错误
            if (null === $json || false === $json) {
                $ret = true;
            } else {
                // 消费
                $ret = $this->consume($messageId, $json, $headers, $message, $workerId);
            }

            // 消费成功,确认消息
            if ($ret) {
                if (!$this->autoAck) {
                    $this->channel->basic_ack($deliveryTag);
                }
                $output->writeln('OK');
                __LOG_MESSAGE('status = OK', $messageId);
            } else {
                // 消费失败,拒绝消息并重新入队
                if (!$this->autoAck) {
                    // 增加重试计数
                    $retryHeaders = $headers;
                    $retryHeaders['x-retry-count'] = $consumedTimes;

                    // 发布重试消息
                    $properties = [
                        'delivery_mode' => 2, // 持久化
                    ];

                    // 设置应用头信息
                    if (!empty($retryHeaders)) {
                        $properties['application_headers'] = $retryHeaders;
                    }

                    $retryMessage = new AMQPMessage($message->getBody(), $properties);

                    $this->channel->basic_publish(
                        $retryMessage,
                        $this->exchangeName,
                        $this->routingKey
                    );

                    // 确认原消息
                    $this->channel->basic_ack($deliveryTag);
                }
                $output->warning('FAIL');
                __LOG_MESSAGE('status = FAIL', $messageId);
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            $output->error($e->getMessage());

            // 发生异常，拒绝消息但不重新入队
            if (!$this->autoAck) {
                $this->channel->basic_nack($deliveryTag, false, false);
            }
        }
    }

    /**
     * 消息消费
     *
     * @param string $messageId
     * @param array $json
     * @param array $headers
     * @param AMQPMessage $message
     * @param int $workerId
     *
     * @return mixed
     */
    abstract protected function consume(
        string $messageId,
        array $json,
        array $headers,
        AMQPMessage $message,
        int $workerId = 0
    );

    /**
     * 获取消息的数组信息
     *
     * @param AMQPMessage $message
     *
     * @return array|mixed
     */
    protected function getMessageBodyJson(AMQPMessage $message)
    {
        if ($this->queueMessageIsParseStr) {
            $json = [];
            parse_str($message->getBody(), $json);
        } else {
            $json = json_decode($message->getBody(), true);
        }
        return $json;
    }

    /**
     * 处理重试错误次数过多
     *
     * @param AMQPMessage $message
     * @param int $consumedTimes
     */
    protected function triggerMaxConsumedTimes(AMQPMessage $message, int $consumedTimes = 0)
    {
        $messageId = $message->get('message_id') ?: uniqid();
        $json = $this->getMessageBodyJson($message);
        __LOG_MESSAGE_ERROR(
            $json,
            sprintf(
                'triggerMaxConsumedTimes___%s___%s',
                $messageId,
                $consumedTimes
            )
        );
        unset($json);
    }

    /**
     * 建立阿里云RabbitMQ连接
     *
     * @throws \Exception
     */
    protected function setupAliyunRabbitMQConnection()
    {
        try {
            if (null === $this->connection || !$this->connection->isConnected()) {
                $configs = $this->getConfigs();

                // 阿里云RabbitMQ需要使用AccessKey生成的用户名密码
                $username = $this->generateAliyunUsername($configs);
                $password = $this->generateAliyunPassword($configs);

                $this->connection = new AMQPStreamConnection(
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

            if (null === $this->channel || !$this->channel->is_open()) {
                $this->channel = $this->connection->channel();
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
            throw new \Exception('Failed to establish Aliyun RabbitMQ connection: ' . $e->getMessage());
        }
    }

    /**
     * 生成阿里云RabbitMQ用户名
     * 格式: ${instanceId}:${accessKey}
     *
     * @param array $configs
     * @return string
     * @throws \Exception
     */
    protected function generateAliyunUsername(array $configs): string
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
    protected function generateAliyunPassword(array $configs): string
    {
        if (isset($configs['password']) && !empty($configs['password'])) {
            return $configs['password'];
        }

        if (empty($configs['access_secret'])) {
            throw new \Exception('Aliyun RabbitMQ requires access_secret or password');
        }

        return $configs['access_secret'];
    }

    /**
     * 关闭阿里云RabbitMQ连接
     */
    protected function closeAliyunRabbitMQConnection()
    {
        try {
            if ($this->channel && $this->channel->is_open()) {
                $this->channel->close();
            }
            if ($this->connection && $this->connection->isConnected()) {
                $this->connection->close();
            }
        } catch (\Exception $e) {
            __LOG_MESSAGE($e);
        }
    }

    /**
     * 获取配置
     *
     * @return array
     * @throws \Exception
     */
    protected function getConfigs(): array
    {
        if (null === $this->configs) {
            $this->configs = Config::get('ram.aliyun_rabbitmq');
            if (empty($this->configs)) {
                throw new \Exception('config[ram.aliyun_rabbitmq] not found.');
            }
        }

        return $this->configs;
    }

    /**
     * 获取阿里云RabbitMQ接入点
     *
     * @return string
     */
    protected function getEndpoint(): string
    {
        $configs = $this->getConfigs();
        return $configs['endpoint'] ?? $configs['host'] ?? 'unknown';
    }

    /**
     * 获取阿里云RabbitMQ虚拟主机
     *
     * @return string
     */
    protected function getVhost(): string
    {
        $configs = $this->getConfigs();
        return $configs['vhost'] ?? '/';
    }

    /**
     * @param array $configs
     */
    protected function setConfigs(array $configs)
    {
        $this->configs = $configs;
    }

    /**
     * @return string
     */
    public function getExchangeName(): string
    {
        return $this->exchangeName;
    }

    /**
     * @param string $exchangeName
     */
    public function setExchangeName(string $exchangeName): void
    {
        $this->exchangeName = $exchangeName;
    }

    /**
     * @return string
     */
    public function getQueueName(): string
    {
        return $this->queueName;
    }

    /**
     * @param string $queueName
     */
    public function setQueueName(string $queueName): void
    {
        $this->queueName = $queueName;
    }

    /**
     * @return string
     */
    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    /**
     * @param string $routingKey
     */
    public function setRoutingKey(string $routingKey): void
    {
        $this->routingKey = $routingKey;
    }
}
