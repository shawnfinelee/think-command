<?php

/*
 * This file is part of the think-command package.
 *
 * @link   https://github.com/chinayin/think-command
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

use think\Env;

// +----------------------------------------------------------------------
// | aliyun ram
// +----------------------------------------------------------------------
return [
    /**
     * MNS - 消息服务
     */
    'mns' => [
        'access_key_id' => Env::get('mns.access_key_id', ''),
        'access_key_secret' => Env::get('mns.access_key_secret', ''),
        'end_point' => Env::get('mns.end_point', ''),
    ],
    /**
     * MQ - RocketMQ
     */
    'mq' => [
        'access_key_id' => Env::get('mq.access_key_id', ''),
        'access_key_secret' => Env::get('mq.access_key_secret', ''),
        'end_point' => Env::get('mq.end_point', ''),
        'instance_id' => Env::get('mq.instance_id', ''),
        'group_id' => Env::get('mq.group_id', ''),
    ],
    /**
     * RabbitMQ - 消息队列
     */
    'rabbitmq' => [
        'host' => Env::get('rabbitmq.host', 'localhost'),
        'port' => Env::get('rabbitmq.port', 5672),
        'username' => Env::get('rabbitmq.username', 'guest'),
        'password' => Env::get('rabbitmq.password', 'guest'),
        'vhost' => Env::get('rabbitmq.vhost', '/'),
        'connection_timeout' => Env::get('rabbitmq.connection_timeout', 3.0),
        'read_write_timeout' => Env::get('rabbitmq.read_write_timeout', 9.0),
        'keepalive' => Env::get('rabbitmq.keepalive', true),
        'heartbeat' => Env::get('rabbitmq.heartbeat', 0),
    ],
];
