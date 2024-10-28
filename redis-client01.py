import random
import redis
from redis import Redis


class RedisClusterClient:
    def __init__(self, master_nodes, slave_nodes, password=None):
        """
        初始化 RedisClusterClient

        :param master_nodes: 主节点列表，格式 [(host1, port1), (host2, port2), ...]
        :param slave_nodes: 从节点列表，格式 [(host1, port1), (host2, port2), ...]
        :param password: Redis 密码
        """
        self.master_nodes = [Redis(host=host, port=port, password=password) for host, port in master_nodes]
        self.slave_nodes = [Redis(host=host, port=port, password=password) for host, port in slave_nodes]

    def _get_master(self):
        """随机选择一个主节点进行写操作"""
        return random.choice(self.master_nodes)

    def _get_slave(self):
        """随机选择一个从节点进行读操作"""
        return random.choice(self.slave_nodes)

    def set(self, key, value):
        """写入操作，选择主节点"""
        master = self._get_master()
        return master.set(key, value)

    def get(self, key):
        """读取操作，选择从节点"""
        slave = self._get_slave()
        return slave.get(key)

    def delete(self, key):
        """删除操作，选择主节点"""
        master = self._get_master()
        return master.delete(key)

    def hget(self, name, key):
        """哈希表读取操作，选择从节点"""
        slave = self._get_slave()
        return slave.hget(name, key)

    def hset(self, name, key, value):
        """哈希表写入操作，选择主节点"""
        master = self._get_master()
        return master.hset(name, key, value)

    def lpush(self, name, *values):
        """列表写入操作，选择主节点"""
        master = self._get_master()
        return master.lpush(name, *values)

    def rpop(self, name):
        """列表读取操作，选择从节点"""
        slave = self._get_slave()
        return slave.rpop(name)

    def close(self):
        """关闭所有连接"""
        for node in self.master_nodes + self.slave_nodes:
            node.close()


# 使用示例
if __name__ == "__main__":
    # 定义主节点和从节点列表
    master_nodes = [("192.168.1.16", 6379)]
    slave_nodes = [("192.168.1.16", 6378), ("192.168.1.16", 6377)]

    # 创建 RedisClusterClient 实例
    client = RedisClusterClient(master_nodes, slave_nodes)

    # 测试写入操作
    client.set("key1", "value1")

    # 测试读取操作
    print(client.get("key1"))

    # 关闭连接
    client.close()