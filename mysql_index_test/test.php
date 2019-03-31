<?php

class MySQL
{
    public function connect($cnf)
    {
        try {
            $this->pdo = new PDO(
                "mysql:host={$cnf->hostname};dbname={$cnf->database}",
                $cnf->username,
                $cnf->password
            );
        } catch (PDOException $e) {
            echo "connection failed: " . $e->getMessage() . "\n";
            $this->pdo = null;
            return false;
        }
        return true;
    }

    public function query($sql)
    {
        $start = microtime(true);
        $r = $this->pdo->query($sql);
        $diff = microtime(true) - $start;
        if ($diff > 1.00) {
            $diff = sprintf("%.3f", $diff);
            echo "slow query:$sql ($diff sec)\n";
        }
        return $r;
    }

    public function last_error()
    {
        if ($this->pdo == null) {
            return "";
        }
        $str = "";
        foreach ($this->pdo->errorInfo() as $v) {
            $str .= "$v,";
        }
        return $str . "\n";
    }

    // --------------------------------
    private $pdo = null;
}

function create_uniqid()
{
    $start_time = strtotime("2019-01-01 00:00:00");
    return (int)floor((microtime(true) - $start_time) * 1000000)
            * 100000 + rand(0, 99999);
}

$config = (object)[
    "hostname"   => "localhost",
    "database"   => "test",
    "username"   => "root",
    "password"   => "root",
    "table_name" => "test3",
];

$mysql = new MySQL;
if ($mysql->connect($config) === false) {
    exit;
}

$ret = $mysql->query("
CREATE TABLE IF NOT EXISTS {$config->table_name} (
    uniqid BIGINT UNSIGNED NOT NULL,
    groupid BIGINT UNSIGNED NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    prio INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(groupid,uniqid),
    INDEX idx_select(groupid, prio),
    INDEX idx_value(value)
);");
if ($ret === false) {
    echo $mysql->last_error();
    exit;
}

while (1) {
    usleep(1000);
    $gid = (rand() % 100) + 1;
    foreach ($mysql->query("SELECT COUNT(*) AS c, MIN(uniqid) AS min_uid"
                ." FROM {$config->table_name}"
                ." WHERE groupid={$gid}") as $row) {
        if ($row["c"] > 2000) {
            if (!$mysql->query("DELETE FROM {$config->table_name}"
                    ." WHERE groupid={$gid} AND uniqid={$row["min_uid"]}")) {
                echo $mysql->last_error() . "\n";
            }
        }
    }

    for ($i = 0; $i<100; $i++) {
        $gid = (rand() % 1000) + 1;
        $mysql->query("SELECT * FROM {$config->table_name}"
                ." WHERE groupid={$gid} AND value=1 ORDER BY prio LIMIT 1;");
    }
    $gid = (rand() % 1000) + 1;
    $mysql->query("begin");
    $is_err = false;
    for ($i = 0; $i<100; $i++) {
        $prio = rand() % 100;
        $value = rand() % 2;
        $id = create_uniqid();
        if (!$mysql->query("INSERT INTO {$config->table_name}"
            ." VALUES ({$id}, {$gid}, {$value}, {$prio})")) {
            echo $mysql->last_error() . "\n";
            if (!$mysql->query("rollback")) {
                echo $mysql->last_error() . "\n";
                exit;
            }
            $is_err = true;
            break;
        }
    }
    if (!$is_err) {
        if (!$mysql->query("commit")) {
            echo $mysql->last_error() . "\n";
        }
    }
}
