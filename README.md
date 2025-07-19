think-command 定制版命令行
====

[![Author](https://img.shields.io/badge/author-@chinayin-blue.svg)](https://github.com/chinayin)
[![Software License](https://img.shields.io/badge/license-Apache--2.0-brightgreen.svg)](LICENSE)
[![Latest Version](https://img.shields.io/packagist/v/chinayin/think-command.svg)](https://packagist.org/packages/chinayin/think-command)
[![Build Status](https://travis-ci.org/chinayin/think-command.svg?branch=0.4)](https://travis-ci.org/chinayin/think-command)
[![Total Downloads](https://img.shields.io/packagist/dt/chinayin/think-command.svg)](https://packagist.org/packages/chinayin/think-command)
![php 7.2+](https://img.shields.io/badge/php-min%207.2-red.svg)

安装
----

运行环境要求 PHP 7.2 及以上版本，以及
[thinkphp5](https://github.com/chinayin/thinkphp5)。

#### composer 安装

如果使用标准的包管理器 composer，你可以很容易的在项目中添加依赖并下载：

```bash
composer require chinayin/think-command
```

### 使用说明

#### `ThinkCommand` 命令行程序

- `ThinkCommand` 对thinkphp5中`Command`做了基础封装，可以更简单方便的开发命令行程序

    * 开启debug模式 `--debug` 程序中通过`IS_DEBUG_CONSOLE`来判断
    * 强制模式 `--force`
    * 配合swoole，进程数 `--thread`

- 命令行参数配置 `buildCommandDefinition()`

```php
protected function buildCommandDefinition(){
  return [
    new Argument('namespace', InputArgument::OPTIONAL, 'The namespace name'),
    new Option('raw', null, InputOption::VALUE_NONE, 'To output raw command list')
  ];
}
```

- 主入口 `main`

```php
protected function main(Input $input, Output $output){

}
```

#### `ThinkMNSQueueV2Command` 阿里云Mns队列消费

- 消息消费 `consume`

```php
* @param string  $message_id   消息ID
* @param array   $json         解析后的json数据
* @param         $message      原始消息
* @param int     $workerId     所进程进程索引ID
protected function consume(string $message_id, array $json, $message, int $workerId = 0);
```

#### `ThinkMQQueueCommand` 阿里云MQ队列消费

- 消息消费 `consume`

```php
* @param string  $message_id  消息ID
* @param array   $json        解析后的json数据
* @param array   $properties  消息属性
* @param         $message     原始消息
* @param int     $workerId    所进程进程索引ID
protected function consume(string $message_id, array $json, $message, array $properties, int $workerId = 0);
```

#### `ThinkRabbitMQCommand` RabbitMQ队列消费

- 消息消费 `consume`

```php
* @param string  $messageId  消息ID
* @param array   $json       解析后的json数据
* @param array   $headers    消息头信息
* @param AMQPMessage $message 原始消息对象
* @param int     $workerId   worker进程ID
protected function consume(string $messageId, array $json, array $headers, AMQPMessage $message, int $workerId = 0);
```

### RabbitMQ 开发环境

如果使用Docker运行RabbitMQ，可以使用以下命令启动：

```bash
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

#### RabbitMQ 管理界面登录凭据

访问管理界面：http://localhost:15672

**方案1：默认用户**
- 用户名：`guest`
- 密码：`guest`

**方案2：管理员用户**
- 用户名：`admin`
- 密码：`admin123`

> 注意：如果需要创建新用户，可以使用以下命令：
> ```bash
> docker exec rabbitmq rabbitmqctl add_user admin admin123
> docker exec rabbitmq rabbitmqctl set_user_tags admin administrator
> docker exec rabbitmq rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
> ```
