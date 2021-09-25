<?php

namespace DuiYing;

class MySQLUtil
{
    const CODE_CONNECTION_FAIL              = 500;
    const CODE_CONNECTION_FAIL_MSG          = '连接数据库失败';
    const CODE_SQL_ERROR                    = 501;
    const CODE_SQL_ERROR_MSG                = 'SQL 执行失败';
    const CODE_BEGIN_ERROR                  = 502;
    const CODE_BEGIN_ERROR_MSG              = '开启事务失败';
    const CODE_COMMIT_ERROR                 = 503;
    const CODE_COMMIT_ERROR_MSG             = '提交事务失败';
    const CODE_ROLLBACK_ERROR               = 504;
    const CODE_ROLLBACK_ERROR_MSG           = '回滚事务失败';

    /**
     * @var \mysqli
     */
    public $conn;

    /**
     * 获取本类实例化对象
     *
     * @return MySQLUtil
     */
    public static function getInstance()
    {
        return new self();
    }

    /**
     * 建立连接
     *
     * @param $host
     * @param $user
     * @param $pass
     * @param $db
     * @param int $port
     * @param string $charset
     * @return $this
     * @throws \Exception
     */
    public function getConnection($host, $user, $pass, $db, $port = 3306, $charset = 'utf8mb4')
    {
        $this->conn = mysqli_connect($host, $user, $pass, $db, $port);
        if ($this->conn === false) throw new \Exception(self::CODE_CONNECTION_FAIL_MSG, self::CODE_CONNECTION_FAIL);
        mysqli_set_charset($this->conn, $charset);
        $this->conn->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);
        return $this;
    }

    /**
     * 断开连接
     */
    public function closeConnection()
    {
        $this->conn->close();
    }

    /**
     * 开启事务
     *
     * @return bool
     * @throws \Exception
     */
    public function beginTransaction()
    {
        $result = $this->conn->begin_transaction(MYSQLI_TRANS_START_WITH_CONSISTENT_SNAPSHOT);
        if ($result === false) throw new \Exception(self::CODE_BEGIN_ERROR_MSG, self::CODE_BEGIN_ERROR);
        return true;
    }

    /**
     * 提交事务
     *
     * @return bool
     * @throws \Exception
     */
    public function commmit()
    {
        $result = $this->conn->commit();
        if ($result === false) throw new \Exception(self::CODE_COMMIT_ERROR_MSG, self::CODE_COMMIT_ERROR);
        return true;
    }

    /**
     * 提交事务
     *
     * @return bool
     * @throws \Exception
     */
    public function rollback()
    {
        $result = $this->conn->rollback();
        if ($result === false) throw new \Exception(self::CODE_ROLLBACK_ERROR_MSG, self::CODE_ROLLBACK_ERROR);
        return true;
    }

    /**
     * 执行原生 SQL 语句
     *
     * @param string $sql
     * @return bool|\mysqli_result
     * @throws \Exception
     */
    public function query($sql = '')
    {
        $result = $this->conn->query($sql);
        if ($result === false) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);
        return $result;
    }

    /**
     * 查询
     *
     * @param string $table
     * @param array $where
     * @param int $p
     * @param int $size
     * @param array|string[] $columns
     * @param array $orderBy
     * @return mixed
     * @throws \Exception
     */
    public function search(string $table, array $where = [], int $p = 1, int $size = 0, array $columns = ['*'], array $orderBy = [])
    {
        if (empty($table)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);

        $sql = sprintf('SELECT %s FROM `%s`', $this->buildColumn($columns), $table);
        $sql .= $this->buildWhere($where);
        $sql .= $this->buildOrderBy($orderBy);
        if ($p && $size) {
            $offset = ($p - 1) * $size;
            $sql .= sprintf(' LIMIT %d,%d ', $offset, $size);
        }
        $sql = trim($sql);
        $result = $this->query($sql);
        return $result->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * 统计
     *
     * @param string $table
     * @param array $where
     * @return int
     * @throws \Exception
     */
    public function count(string $table, array $where = [])
    {
        if (empty($table)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);

        $sql = sprintf('SELECT count(*) `count` FROM `%s` ', $table);
        $sql .= $this->buildWhere($where);
        $sql = trim($sql);
        $result = $this->query($sql);
        $count = $result->fetch_assoc()['count'];
        return $count ? intval($count) : 0;
    }

    /**
     * 查找单条记录
     *
     * @param string $table
     * @param array $where
     * @param array|string[] $columns
     * @param array $orderBy
     * @return array
     * @throws \Exception
     */
    public function find(string $table, array $where = [], array $columns = ['*'], array $orderBy = [])
    {
        if (empty($table)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);
        $list = $this->search($table, $where, 1, 1, $columns, $orderBy);
        return $list ? (array)$list[0] : [];
    }

    /**
     * 创建
     *
     * @param string $table
     * @param array $data
     * @return int|string
     * @throws \Exception
     */
    public function create(string $table, array $data = [])
    {
        if (empty($table) || empty($data)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);

        $keyList = array_keys($data);
        $valueList = array_values($data);

        $sql = sprintf('INSERT INTO `%s` (%s) VALUES (', $table, $this->buildColumn($keyList));

        foreach ($valueList as $k => $v) {
            if (is_string($v)) {
                $v = $this->conn->real_escape_string($v);
                $sql .= "'{$v}',";
            } else {
                $sql .= "{$v},";
            }
        }

        $sql = rtrim($sql, ',');
        $sql .= ')';
        $this->query($sql);
        return mysqli_insert_id($this->conn);
    }

    /**
     * 更新
     *
     * @param string $table
     * @param array $where
     * @param array $data
     * @return int
     * @throws \Exception
     */
    public function update(string $table, array $where = [], array $data = [])
    {
        if (empty($table) || empty($data)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);

        $sql = sprintf('UPDATE `%s` SET', $table);

        foreach ($data as $k => $v) {
            if (is_string($v)) {
                // 转义
                $v = $this->conn->real_escape_string($v);
                $sql .= " `{$k}` = '$v',";
            } else {
                $sql .= " `{$k}` = $v,";
            }
        }

        $sql = rtrim($sql, ',');
        $sql .= $this->buildWhere($where);
        $this->query($sql);
        return $this->conn->affected_rows;
    }

    /**
     * 删除
     *
     * @param string $table
     * @param array $where
     * @return int
     * @throws \Exception
     */
    public function delete(string $table, array $where = [])
    {
        // DELETE 操作是危险操作，必须带 WHERE 条件
        if (empty($table) || empty($where)) throw new \Exception(self::CODE_SQL_ERROR_MSG, self::CODE_SQL_ERROR);
        $sql = sprintf('DELETE FROM `%s`', $table);
        $sql .= $this->buildWhere($where);
        $this->query($sql);
        return $this->conn->affected_rows;
    }

    /**
     * 构建排序条件
     *
     * @param $orderBy
     * @return string
     */
    public function buildOrderBy($orderBy)
    {
        if (empty($orderBy)) return '';

        $str = '';

        foreach ($orderBy as $k => $v) {
            $str .= sprintf("`%s` %s,", $k, $v);
        }

        return ' ORDER BY ' . rtrim($str, ',') . ' ';
    }

    /**
     * 构建查询字段
     *
     * @param $columns
     * @return string
     */
    public function buildColumn($columns)
    {
        $str = '';
        if (count($columns) === 1 && $columns[0] === '*') {
            return '*';
        } else {
            foreach ($columns as $k => $v) {
                $str .= sprintf("`%s`,", $v);
            }
        }
        return rtrim($str, ',');
    }

    /**
     * 构建 WHERE 语句
     *
     * @param array $where
     * @return string
     */
    public function buildWhere($where = [])
    {
        if (empty($where)) return '';

        $whereStr = '';

        foreach ($where as $field => $value) {
            if (is_null($value)) continue;

            if (!is_array($value)) {
                if (is_string($value)) {
                    // 转义
                    $value = $this->conn->real_escape_string($value);
                    $whereStr .= sprintf("AND `%s` = '%s' ", $field, $value);
                } else {
                    $whereStr .= sprintf("AND `%s` = %s ", $field, $value);
                }

                continue;
            }

            switch ($value[0]) {
                case '=':
                    if (!is_array($value[1])) {
                        // 转义
                        if (is_string($value[1])) $value[1] = $this->conn->real_escape_string($value[1]);
                        $whereStr .= sprintf("AND `%s` = '%s' ", $field, $value[1]);
                    }
                    break;
                case '!=':
                case '<>':
                    if (!is_array($value[1])) {
                        // 转义
                        if (is_string($value[1])) $value[1] = $this->conn->real_escape_string($value[1]);
                        $whereStr .= sprintf("AND `%s` <> '%s' ", $field, $value[1]);
                    }
                    break;
                case '%':   if (!is_array($value[1]))   $whereStr .= sprintf("AND `%s` LIKE '%s' ", $field, $value[1]); break;
                case '<':   if (!is_array($value[1]))   $whereStr .= sprintf("AND `%s` < %d ", $field, $value[1]); break;
                case '<=':  if (!is_array($value[1]))   $whereStr .= sprintf("AND `%s` <= %d ", $field, $value[1]); break;
                case '>':   if (!is_array($value[1]))   $whereStr .= sprintf("AND `%s` > %d ", $field, $value[1]); break;
                case '>=':  if (!is_array($value[1]))   $whereStr .= sprintf("AND `%s` >= %d ", $field, $value[1]);; break;
                case '&':   if (is_array($value[1]))    $whereStr .= sprintf("AND `%s` >= %d AND `%s` <= %d ", $field, $value[1][0], $field, $value[1][1]); break;
                default:
                    $whereStr .= "AND `{$field}` IN (";
                    foreach ($value as $k => $v) {
                        if (is_string($v)) {
                            $v = $this->conn->real_escape_string($v);
                            $whereStr .= sprintf("'%s',", $v);
                        } else {
                            $whereStr .= sprintf("%s,", $v);
                        }
                    }
                    $whereStr = rtrim($whereStr, ',');
                    $whereStr .= ") ";
            }
        }

        return ' WHERE' . ltrim(trim($whereStr), 'AND');
    }
}