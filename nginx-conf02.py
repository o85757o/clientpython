import json
import os
import subprocess

class NginxConfigGenerator:
    def __init__(self, config_path="nginx_config.json", nginx_conf_path="nginx.conf", nginx_executable="nginx.exe"):
        self.config_path = config_path
        self.nginx_conf_path = nginx_conf_path
        self.nginx_executable = nginx_executable
        self.config = self.load_config()

    def load_config(self):
        """加载配置文件，若不存在则初始化空配置。"""
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as file:
                return json.load(file)
        return {"proxies": []}

    def save_config(self):
        """将当前配置保存到JSON文件。"""
        with open(self.config_path, "w") as file:
            json.dump(self.config, file, indent=4)

    def add_proxy(self, protocol, tls_version, listen_ip, listen_port, pass_ips, pass_port,
                  cert_path=None, key_path=None, listen_tls_options="", pass_tls_options="", load_balancing=False):
        """
        添加代理配置。
        `protocol`: "tcp", "http", or "http2"
        `tls_version`: "none", "tls1.1", "tls1.2", "tls1.3"
        `listen_ip`: 监听的IP
        `listen_port`: 监听的端口
        `pass_ips`: 上游服务器的IP列表（用于负载均衡）
        `pass_port`: 上游服务器的端口
        `cert_path`: 证书路径（如果启用TLS，则必需）
        `key_path`: 证书密钥路径（如果启用TLS，则必需）
        `listen_tls_options`: 监听TLS的自定义选项
        `pass_tls_options`: 转发TLS的自定义选项
        `load_balancing`: 是否启用负载均衡
        """
        proxy = {
            "protocol": protocol,
            "tls_version": tls_version,
            "listen_ip": listen_ip,
            "listen_port": listen_port,
            "pass_ips": pass_ips,
            "pass_port": pass_port,
            "cert_path": cert_path,
            "key_path": key_path,
            "listen_tls_options": listen_tls_options,
            "pass_tls_options": pass_tls_options,
            "load_balancing": load_balancing
        }
        self.config["proxies"].append(proxy)
        self.save_config()

    def generate_nginx_conf(self):
        """生成nginx.conf配置内容。"""
        conf = """
        worker_processes 1;
        events {
            worker_connections 1024;
        }
        """

        # Stream块用于TCP代理
        tcp_proxies = [p for p in self.config["proxies"] if p["protocol"] == "tcp"]
        if tcp_proxies:
            conf += "\nstream {\n"
            for proxy in tcp_proxies:
                conf += self._generate_stream_proxy(proxy)
            conf += "}\n"

        # HTTP块用于HTTP/HTTP2代理
        http_proxies = [p for p in self.config["proxies"] if p["protocol"] in ["http", "http2"]]
        if http_proxies:
            conf += "\nhttp {\n"
            for proxy in http_proxies:
                conf += self._generate_http_proxy(proxy)
            conf += "}\n"

        return conf

    def _generate_stream_proxy(self, proxy):
        """生成TCP代理的配置块。"""
        tls_conf = (
            f"ssl_certificate {proxy['cert_path']};\nssl_certificate_key {proxy['key_path']};\n"
            if proxy["tls_version"] != "none" else ""
        )
        tls_protocols = f"ssl_protocols {proxy['tls_version']};" if proxy["tls_version"] != "none" else ""
        upstream_conf = "\n".join([f"server {ip}:{proxy['pass_port']};" for ip in proxy["pass_ips"]])

        stream_proxy = f"""
        server {{
            listen {proxy['listen_ip']}:{proxy['listen_port']} {'ssl' if proxy['tls_version'] != 'none' else ''};
            proxy_pass {"upstream_backend" if proxy['load_balancing'] else f"{proxy['pass_ips'][0]}:{proxy['pass_port']}"};
            {tls_conf}
            {tls_protocols}
            {proxy['listen_tls_options']}
        }}
        """

        if proxy["load_balancing"]:
            stream_proxy = f"upstream upstream_backend {{\n{upstream_conf}\n}}\n" + stream_proxy
        return stream_proxy

    def _generate_http_proxy(self, proxy):
        """生成HTTP/HTTP2代理的配置块。"""
        http2 = "http2" if proxy["protocol"] == "http2" else ""
        tls_conf = (
            f"ssl_certificate {proxy['cert_path']};\nssl_certificate_key {proxy['key_path']};\n"
            if proxy["tls_version"] != "none" else ""
        )
        tls_protocols = f"ssl_protocols {proxy['tls_version']};" if proxy["tls_version"] != "none" else ""
        upstream_conf = "\n".join([f"server {ip}:{proxy['pass_port']};" for ip in proxy["pass_ips"]])

        http_proxy = f"""
        server {{
            listen {proxy['listen_ip']}:{proxy['listen_port']} {'ssl' if proxy['tls_version'] != 'none' else ''} {http2};
            location / {{
                proxy_pass {"http://upstream_backend" if proxy['load_balancing'] else f"http://{proxy['pass_ips'][0]}:{proxy['pass_port']}"};
                {tls_conf}
                {tls_protocols}
                {proxy['listen_tls_options']}
                {proxy['pass_tls_options']}
            }}
        }}
        """

        if proxy["load_balancing"]:
            http_proxy = f"upstream upstream_backend {{\n{upstream_conf}\n}}\n" + http_proxy
        return http_proxy

    def write_nginx_conf(self):
        """将生成的配置内容写入nginx.conf文件。"""
        conf_content = self.generate_nginx_conf()
        with open(self.nginx_conf_path, "w") as file:
            file.write(conf_content)

    def reload_nginx(self):
        """重新加载nginx服务以应用新配置。"""
        subprocess.run([self.nginx_executable, "-s", "reload"], shell=True)

    def apply_config(self):
        """应用当前配置，通过写入nginx.conf并重新加载nginx。"""
        self.write_nginx_conf()
        self.reload_nginx()
        print("Nginx配置已应用并重新加载。")

# 使用示例
if __name__ == "__main__":
    # 创建配置生成器实例
    nginx_config = NginxConfigGenerator()

    # 添加TCP代理配置
    nginx_config.add_proxy(
        protocol="tcp",
        tls_version="tls1.2",
        listen_ip="127.0.0.1",
        listen_port=8000,
        pass_ips=["192.168.0.1", "192.168.0.2"],
        pass_port=9000,
        cert_path="path/to/cert.pem",
        key_path="path/to/key.pem",
        listen_tls_options="",
        pass_tls_options="",
        load_balancing=True
    )

    # 添加HTTP代理配置
    nginx_config.add_proxy(
        protocol="http",
        tls_version="tls1.3",
        listen_ip="127.0.0.1",
        listen_port=8080,
        pass_ips=["192.168.0.3"],
        pass_port=80,
        cert_path="path/to/cert.pem",
        key_path="path/to/key.pem",
        listen_tls_options="",
        pass_tls_options="",
        load_balancing=False
    )

    # 应用配置并重载Nginx
    nginx_config.apply_config()