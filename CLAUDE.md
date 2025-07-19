# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个基于ThinkPHP5的定制化命令行库，主要用于阿里云消息队列(MNS/MQ)的消费处理。

## 常用命令

### 依赖管理
```bash
composer install          # 安装依赖
composer update           # 更新依赖
```

### 代码质量检查
```bash
composer test             # 运行PHPUnit测试
composer test-ci          # 运行测试并生成覆盖率报告
composer lint             # 使用PHP-CS-Fixer进行代码格式化
composer analyse          # 使用PHPStan进行静态代码分析
composer travis           # 运行lint和analyse(CI流程)
```

### 直接执行工具
```bash
vendor/bin/phpunit        # 直接运行PHPUnit
vendor/bin/php-cs-fixer fix -v  # 直接运行代码格式化
vendor/bin/phpstan analyse      # 直接运行静态分析
```

## 代码架构

### 核心抽象类

1. **ThinkCommand** (`src/ThinkCommand.php`)
   - 基础命令行抽象类，继承自ThinkPHP的Command类
   - 提供debug模式、强制模式、多进程支持等功能
   - 集成Swoole Process Pool用于多worker处理
   - 提供Snowflake ID生成器支持
   - 所有自定义命令都应继承此类

2. **ThinkMNSQueueV2Command** (`src/ThinkMNSQueueV2Command.php`)  
   - 专门用于阿里云MNS队列消费的抽象类
   - 支持轮询消息、消费重试、错误处理
   - 支持STS临时token认证方式
   - 子类需要实现`consume()`方法处理具体消息

3. **ThinkMQQueueCommand** (`src/ThinkMQQueueCommand.php`)
   - 专门用于阿里云MQ队列消费的抽象类  
   - 支持普通消息和顺序消息两种模式
   - 支持分组消费、多worker并发处理
   - 子类需要实现`consume()`方法处理具体消息

4. **ThinkRabbitMQCommand** (`src/ThinkRabbitMQCommand.php`)
   - 专门用于RabbitMQ队列消费的抽象类
   - 支持交换机、队列、路由键配置
   - 支持消息重试机制和死信队列
   - 支持QoS设置和手动/自动确认模式
   - 子类需要实现`consume()`方法处理具体消息

### 命令行参数设计

所有命令都支持以下基础参数：
- `--debug` / `-d`: 开启debug模式
- `--force` / `-f`: 强制模式执行
- `--thread`: 指定worker进程数量(配合Swoole使用)

队列相关命令额外支持：
- `--queue`: 动态覆盖队列名称(MNS/RabbitMQ)
- `--topic`: 动态覆盖主题名称(MQ/MNS)
- `--exchange`: 动态覆盖交换机名称(RabbitMQ)
- `--routing-key`: 动态覆盖路由键(RabbitMQ)

### 配置系统

- 队列配置通过ThinkPHP配置系统管理
- MNS配置键: `ram.mns`
- MQ配置键: `ram.mq`
- RabbitMQ配置键: `rabbitmq`
- 配置文件示例在`apps/configs/extra/ram.php`

### 多进程架构

- 使用Swoole Process Pool实现多worker并发消费
- 每个worker独立处理消息，支持失败重试
- 自动进程命名和生命周期管理
- 支持macOS和Linux平台

## 开发新命令

1. 继承对应的抽象类(`ThinkCommand`、`ThinkMNSQueueV2Command`、`ThinkMQQueueCommand`或`ThinkRabbitMQCommand`)
2. 设置`$commandName`和`$commandDescription`属性
3. 重写`buildCommandDefinition()`定义命令行参数
4. 实现`main()`方法(普通命令)或`consume()`方法(队列命令)
5. 在`apps/configs/command.php`中注册新命令

参考示例：
- 普通命令：`apps/example/Command.php`
- RabbitMQ队列：`apps/example/RabbitMQCommand.php`

## 消息队列最佳实践

- 设置合理的`$maxConsumedTimes`避免无限重试
- 使用`$waitSeconds`控制轮询间隔(MNS/MQ)或`$consumeTimeout`(RabbitMQ)
- 对于高并发场景，合理设置`$workerNum`
- 实现`triggerMaxConsumedTimes()`处理超过重试次数的消息
- 消费方法返回boolean值控制消息确认
- RabbitMQ特有：合理设置`$prefetchCount`控制QoS，选择合适的`$autoAck`模式

## RabbitMQ配置示例

```php
// config/rabbitmq.php
return [
    'host' => 'localhost',
    'port' => 5672,
    'username' => 'guest',
    'password' => 'guest',
    'vhost' => '/',
    'connection_timeout' => 3.0,
    'read_write_timeout' => 3.0,
    'keepalive' => false,
    'heartbeat' => 0,
];
```