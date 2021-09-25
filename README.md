<h1 align="center">
    mysql-util
</h1>

<p align="center">PHP MySQL 库</p>  

**安装**

```shell
composer require duiying/mysql-util
```

**使用**

**安装**

```shell
composer require duiying/mysql-util
```

**使用**  

1、获取数据库连接

```php
$db = MySQLUtil::getInstance()->getConnection($host, $user, $pass, $db, $port);
```

2、执行支持的方法  

- create
- update
- search
- find
- delete
- query（原生 SQL）
- beginTransaction
- commit
- rollback

3、关闭数据库连接  

```php
$db->closeConnection();
```

